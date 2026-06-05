/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_TFIDF_SCORER_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_TFIDF_SCORER_H_

#include <string_view>

#include "src/indexes/scoring/scorer.h"

namespace valkey_search::indexes::scoring {

// TF-IDF scoring ("TFIDF"), RediSearch-compatible.
//
//   IDF        = floor(log2(1 + (N + 1) / dt))
//   tfidf_leaf = leaf_weight * TF * IDF
//   final      = sum_of_leaves * document_score / norm / slop
//
// `norm` (max term frequency in the doc) and `slop` (proximity penalty) are
// RediSearch-specific divisors. Neither is applied yet: the generic Scorer
// interface has no channel for a per-document `norm`, and SLOP lands on a
// separate branch (slop == 1 here). ComposeDocumentScore therefore omits the
// `/ norm / slop` terms for now; wiring `norm` through the scoring paths is a
// follow-up. TFIDF-specific behavior that does NOT need norm is preserved:
// negative document scores (including -inf) clamp to 0.
class TfidfScorer : public Scorer {
 public:
  static constexpr std::string_view kName = "TFIDF";

  std::string_view Name() const override { return kName; }
  ScorerType Type() const override { return ScorerType::kTfidf; }

  // TFIDF normalizes by `norm` (max term frequency), not document length, so it
  // needs neither per-document nor corpus lengths.
  bool NeedsDocumentLength() const override { return false; }

  // IDF = floor(log2(1 + (N + 1) / dt)).
  float PrecomputeIDF(const IdfInput& input) const override;

  // Scores one leaf given a precomputed IDF: leaf_weight * TF * IDF.
  float ScoreLeaf(const LeafScoreInput& input) const override;

  float ComposeDocumentScore(float sum_of_terms,
                             float document_score) const override;
};

}  // namespace valkey_search::indexes::scoring

#endif
