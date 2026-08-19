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
// `slop` (proximity penalty) lands on a separate branch and is 1 here.
class TfidfScorer : public Scorer {
 public:
  static constexpr std::string_view kName = "TFIDF";

  std::string_view Name() const override { return kName; }
  ScorerType Type() const override { return ScorerType::kTfidf; }

  // TFIDF normalizes by `norm` (max term frequency), not document length, so it
  // needs neither per-document nor corpus lengths.
  bool NeedsDocumentLength() const override { return false; }

  // Divides the document score by `norm`.
  bool NeedsNorm() const override { return true; }

  // IDF = floor(log2(1 + (N + 1) / dt)).
  float PrecomputeIDF(const IdfInput& input) const override;

  // Scores one leaf given a precomputed IDF: leaf_weight * TF * IDF.
  float ScoreLeaf(const LeafScoreInput& input) const override;

  // Standard TF-IDF scores a matched numeric leaf as weight 1 * frequency 1.
  float ScoreNumericLeaf() const override { return 1.0f; }

  float ComposeDocumentScore(const DocumentScoreInput& input) const override;
};

}  // namespace valkey_search::indexes::scoring

#endif
