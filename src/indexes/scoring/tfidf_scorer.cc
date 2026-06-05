/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/tfidf_scorer.h"

#include <cmath>
#include <cstdint>

#include "absl/log/check.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

namespace {

float Idf(uint32_t total_docs, uint32_t num_doc_contain_term) {
  CHECK_GT(num_doc_contain_term, 0u);
  CHECK_LE(num_doc_contain_term, total_docs);
  const float n = static_cast<float>(total_docs);
  const float dt = static_cast<float>(num_doc_contain_term);
  return std::floor(std::log2(1.0f + (n + 1.0f) / dt));
}

}  // namespace

// SLOP lands on a separate branch; until then there is no proximity penalty.
float TfidfScorer::GetSlop(const TfidfStats& stats) const { return 1.0f; }

float TfidfScorer::ScoreLeaf(const ScoringStats& stats,
                             float leaf_weight) const {
  const auto* tfidf_stats = dynamic_cast<const TfidfStats*>(&stats);
  CHECK(tfidf_stats != nullptr);

  const float idf =
      Idf(tfidf_stats->total_docs, tfidf_stats->num_doc_contain_term);
  const float tf = static_cast<float>(tfidf_stats->term_frequency);
  return leaf_weight * tf * idf;
}

float TfidfScorer::ComposeDocumentScore(float sum_of_terms,
                                        const ScoringStats& stats) const {
  const auto* tfidf_stats = dynamic_cast<const TfidfStats*>(&stats);
  CHECK(tfidf_stats != nullptr);

  // norm == 0 => doc has no TEXT fields => score is 0.
  if (tfidf_stats->norm == 0) return 0.0f;

  const float document_score = tfidf_stats->document_score;
  // Negative document scores (including -inf) clamp to 0 under TFIDF.
  if (document_score < 0.0f) return 0.0f;

  const float norm = static_cast<float>(tfidf_stats->norm);
  const float words = sum_of_terms / norm / GetSlop(*tfidf_stats);

  // Avoid 0 * inf -> NaN; propagate +inf as the final score.
  if (IsInf(document_score)) return document_score;
  return words * document_score;
}

}  // namespace valkey_search::indexes::scoring
