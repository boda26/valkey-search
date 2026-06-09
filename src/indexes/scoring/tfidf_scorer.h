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
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

// TF-IDF scoring ("TFIDF"), RediSearch-compatible.
//
//   IDF        = floor(log2(1 + (N + 1) / dt))
//   tfidf_leaf = leaf_weight * TF * IDF
//   final      = sum_of_leaves * document_score / norm / slop
//
// `norm` (max term frequency in the doc) and `slop` (proximity penalty) are
// RediSearch-specific divisors. norm == 0 means the doc has no TEXT fields and
// forces the score to 0; negative final scores are clamped to 0. SLOP lands on
// a separate branch; GetSlop() returns 1 here, applying no proximity penalty.
class TfidfScorer : public Scorer {
 public:
  static constexpr std::string_view kName = "TFIDF";

  std::string_view Name() const override { return kName; }
  ScorerType Type() const override { return ScorerType::kTfidf; }

  float ScoreLeaf(const ScoringStats& stats, float leaf_weight) const override;

  float ComposeDocumentScore(float sum_of_terms,
                             const ScoringStats& stats) const override;

 private:
  float GetSlop(const TfidfStats& stats) const;
};

}  // namespace valkey_search::indexes::scoring

#endif
