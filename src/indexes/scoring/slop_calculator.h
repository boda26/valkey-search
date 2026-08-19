/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SLOP_CALCULATOR_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SLOP_CALCULATOR_H_

#include <cstdint>
#include <vector>

#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

// Term position within a document, matching indexes::text::Position.
using SlopPosition = uint32_t;

// Computes the TFIDF slop for a single (query, doc) pair: the floored
// Euclidean distance across the position gaps between consecutive
// outermost query nodes.
//
//   sum_squares = sum of MinGap(node[i], node[i+1])^2
//   slop        = sum_squares > 0 ? max(1, floor(sqrt(sum_squares)))
//                                 : anchor_count - 1
//
// Inner gaps are taken as-is (so two terms sharing a position contribute a
// gap of 0). The zero-sum fallback covers two cases where the gaps carry no
// information: every anchor sharing a position (`red red red`), and an index
// created NOOFFSETS, which stores every token at position 0.
//
// The caller drives the calculator as a passenger on the per-doc query
// tree walk. The root group is unwrapped by the caller: its direct
// children are the outermost level, so the caller iterates them without
// an enclosing EnterGroup/ExitGroup. Only nested groups fire the group
// hooks; a nested group collapses to the union of its leaf positions and
// contributes a single anchor to its parent level.
//
// One document at a time: drive, Finalize, then Reset before the next.
class SlopCalculator {
 public:
  SlopCalculator() = default;

  // Records a query term occurring at the given sorted positions in the
  // document. An empty span (term absent in this doc) contributes nothing.
  void OnTerm(absl::Span<const SlopPosition> positions);

  // Opens a nested group. Its leaf positions are unioned together and
  // surface to the enclosing level as one anchor on ExitGroup.
  void EnterGroup();

  // Closes the nested group opened by the matching EnterGroup.
  void ExitGroup();

  // Returns the slop for the document. Must be called once, after the walk.
  uint32_t Finalize();

  // Clears the walk state, retaining capacity, so one calculator can be reused
  // across the candidates of a query instead of reallocating per document.
  void Reset();

 private:
  // An anchor is one outermost-level node reduced to the positions it
  // occupies. A bare term yields its own positions; a nested group yields
  // the union of its leaves. Gaps are measured between consecutive anchors.
  using Anchor = std::vector<SlopPosition>;

  // Folds positions into the enclosing level: into the open group's union, or
  // into a new outermost anchor when no group is open.
  void EmitPositions(absl::Span<const SlopPosition> positions);

  // The next outermost anchor slot, growing the pool only past the widest
  // document seen so far.
  Anchor& NextAnchorSlot();

  // Slot pools, reused across documents. Both vectors keep their element
  // buffers allocated; the live prefix of each is given by the counter below
  // it, and Reset only rewinds those counters. A steady-state scoring loop
  // therefore performs no allocation per document.
  std::vector<Anchor> anchors_;
  size_t anchor_count_ = 0;
  std::vector<Anchor> group_stack_;
  size_t group_depth_ = 0;
};

}  // namespace valkey_search::indexes::scoring

#endif  // VALKEYSEARCH_SRC_INDEXES_SCORING_SLOP_CALCULATOR_H_
