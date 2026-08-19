/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/slop_calculator.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

namespace {

// Minimum absolute gap between two sorted, non-empty position lists.
// Two-pointer scan, O(|left| + |right|).
SlopPosition MinGap(const std::vector<SlopPosition>& left,
                    const std::vector<SlopPosition>& right) {
  size_t i = 0;
  size_t j = 0;
  SlopPosition best = std::numeric_limits<SlopPosition>::max();
  while (i < left.size() && j < right.size()) {
    SlopPosition l = left[i];
    SlopPosition r = right[j];
    best = std::min(best, l > r ? l - r : r - l);
    if (l < r) {
      ++i;
    } else {
      ++j;
    }
  }
  return best;
}

// floor(sqrt(n)) computed in integer space. Avoids depending on the exact
// rounding of std::sqrt under -ffast-math: seed from a double then correct.
// Comparisons use division rather than squaring so they cannot overflow even
// when n is near UINT64_MAX (where (x+1)*(x+1) would wrap).
uint32_t IntSqrt(uint64_t n) {
  if (n == 0) return 0;
  uint64_t x = static_cast<uint64_t>(std::sqrt(static_cast<double>(n)));
  if (x == 0) x = 1;  // guard against a rounded-down seed before dividing by x
  while (x > n / x) --x;             // x*x > n
  while (n / (x + 1) >= x + 1) ++x;  // (x+1)*(x+1) <= n
  return static_cast<uint32_t>(x);
}

}  // namespace

SlopCalculator::Anchor& SlopCalculator::NextAnchorSlot() {
  if (anchor_count_ == anchors_.size()) {
    anchors_.emplace_back();
  }
  return anchors_[anchor_count_++];
}

void SlopCalculator::EmitPositions(absl::Span<const SlopPosition> positions) {
  if (group_depth_ == 0) {
    NextAnchorSlot().assign(positions.begin(), positions.end());
    return;
  }
  // Fold into the enclosing group's union.
  Anchor& parent = group_stack_[group_depth_ - 1];
  parent.insert(parent.end(), positions.begin(), positions.end());
}

void SlopCalculator::OnTerm(absl::Span<const SlopPosition> positions) {
  // A term absent in this doc contributes no anchor. For an outermost term
  // this only happens when admission did not require it (e.g. an OR branch).
  if (positions.empty()) {
    return;
  }
  EmitPositions(positions);
}

void SlopCalculator::EnterGroup() {
  if (group_depth_ == group_stack_.size()) {
    group_stack_.emplace_back();
  } else {
    group_stack_[group_depth_].clear();
  }
  ++group_depth_;
}

void SlopCalculator::ExitGroup() {
  CHECK(group_depth_ > 0);
  --group_depth_;
  // The union stays in its slot; EmitPositions copies it out, so the slot keeps
  // its capacity for the next document.
  const Anchor& group = group_stack_[group_depth_];
  // An all-absent group has an empty union: drop it rather than emit an
  // empty anchor.
  if (group.empty()) {
    return;
  }
  EmitPositions(group);
}

uint32_t SlopCalculator::Finalize() {
  CHECK(group_depth_ == 0);
  if (anchor_count_ <= 1) {
    return 1;
  }
  // Sort positions *within* each anchor so MinGap's two-pointer scan holds.
  // The order of the anchors themselves is the query order and is left
  // untouched.
  for (size_t i = 0; i < anchor_count_; ++i) {
    std::sort(anchors_[i].begin(), anchors_[i].end());
  }
  uint64_t sum_squares = 0;
  for (size_t i = 0; i + 1 < anchor_count_; ++i) {
    uint64_t gap = MinGap(anchors_[i], anchors_[i + 1]);
    // Saturate rather than wrap: positions are uint32_t and the anchor count
    // is bounded only by the query-terms limit, so the sum of squared gaps
    // can in theory exceed uint64_t. A saturated sum still yields a large,
    // monotonic slop, which is the correct ranking signal.
    uint64_t term = gap * gap;
    sum_squares = (sum_squares > std::numeric_limits<uint64_t>::max() - term)
                      ? std::numeric_limits<uint64_t>::max()
                      : sum_squares + term;
  }
  if (sum_squares == 0) {
    // Every consecutive pair shares a position, so the gaps say nothing about
    // the query's spread. Fall back to the anchor count, which is also what an
    // index created NOOFFSETS produces (every token stored at position 0).
    // anchor_count_ >= 2 here, so this keeps the min-1 guarantee.
    return static_cast<uint32_t>(anchor_count_ - 1);
  }
  // The min-1 guard is applied only here, to the final slop, to avoid a
  // divide-by-zero when slop later divides the TFIDF numerator.
  return std::max(1u, IntSqrt(sum_squares));
}

void SlopCalculator::Reset() {
  // Rewind rather than clear: the slots (and their allocated position buffers)
  // are reused by the next document.
  anchor_count_ = 0;
  group_depth_ = 0;
}

}  // namespace valkey_search::indexes::scoring
