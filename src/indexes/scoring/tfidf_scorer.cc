/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/tfidf_scorer.h"

#include <cmath>

#include "absl/log/check.h"
#include "src/indexes/scoring/scorer.h"

namespace valkey_search::indexes::scoring {

float TfidfScorer::PrecomputeIDF(const IdfInput& input) const {
  // dt <= total_docs is enforced by callers via clamping (see Bm25StdScorer);
  // a debug-only check catches genuine caller bugs without aborting production.
  DCHECK_LE(input.num_doc_contain_term, input.total_docs);
  // A term in no documents carries no information; return 0 rather than
  // dividing by dt == 0 (which would yield +inf). Such a leaf matches no
  // document, so its contribution is never summed anyway.
  if (input.num_doc_contain_term == 0) return 0.0f;
  const float n = static_cast<float>(input.total_docs);
  const float dt = static_cast<float>(input.num_doc_contain_term);
  return std::floor(std::log2(1.0f + (n + 1.0f) / dt));
}

float TfidfScorer::ScoreLeaf(const LeafScoreInput& input) const {
  const float tf = static_cast<float>(input.term_frequency);
  return input.leaf_weight * tf * input.idf;
}

float TfidfScorer::ComposeDocumentScore(float sum_of_terms,
                                        float document_score) const {
  // Negative document scores (including -inf) clamp to 0 under TFIDF.
  if (document_score < 0.0f) return 0.0f;
  // Avoid 0 * inf -> NaN; propagate +inf as the final score.
  if (IsInf(document_score)) return document_score;
  // TODO: divide by the per-document `norm` (max term frequency) and `slop`
  // once the scoring interface carries a norm channel. Until then the score
  // omits the `/ norm / slop` normalization (see tfidf_scorer.h).
  return sum_of_terms * document_score;
}

}  // namespace valkey_search::indexes::scoring
