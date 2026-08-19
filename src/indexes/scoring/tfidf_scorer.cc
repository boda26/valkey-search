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

float TfidfScorer::ComposeDocumentScore(const DocumentScoreInput& input) const {
  // Negative document scores (including -inf) clamp to 0 under TFIDF.
  if (input.document_score < 0.0f) return 0.0f;
  // A text-less document has norm 0; score it 0 rather than dividing.
  if (input.norm == 0) return 0.0f;
  // Avoid 0 * inf -> NaN; propagate +inf as the final score.
  if (IsInf(input.document_score)) return input.document_score;
  return input.sum_of_terms * input.document_score /
         static_cast<float>(input.norm);
}

}  // namespace valkey_search::indexes::scoring
