/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/tfidf_scorer.h"

#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/scoring/scorer.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

constexpr float kFloatTolerance = 1e-4f;

using test_data::LeafData;

LeafData MakeLeaf(uint32_t total_docs, uint32_t num_doc_contain_term,
                  uint32_t term_frequency) {
  LeafData in;
  in.total_docs = total_docs;
  in.num_doc_contain_term = num_doc_contain_term;
  in.term_frequency = term_frequency;
  return in;
}

// Precomputes the IDF from the leaf, then scores it via ScoreLeaf. The
// production path precomputes the IDF once per term; tests recompute per call.
// TFIDF ignores doc_len/avg_doc_len (NeedsDocumentLength() == false).
float ScoreLeaf(const TfidfScorer& scorer, const LeafData& leaf,
                float leaf_weight) {
  const float idf =
      scorer.PrecomputeIDF({leaf.total_docs, leaf.num_doc_contain_term});
  return scorer.ScoreLeaf({idf, leaf.term_frequency, leaf.doc_len,
                           /*avg_doc_len=*/0.0f, leaf_weight});
}

// --- Direct scorer math ---

TEST(TfidfScorerTest, IdentityNameAndType) {
  TfidfScorer scorer;
  EXPECT_EQ(scorer.Name(), "TFIDF");
  EXPECT_EQ(scorer.Type(), ScorerType::kTfidf);
  EXPECT_FALSE(scorer.NeedsDocumentLength());
}

// IDF = floor(log2(1 + (N+1)/dt)); with N=8: hello (dt=6) -> 1, rare (dt=2) ->
// 2, unique (dt=1) -> 3. leaf_weight and TF are both 1 here, so the leaf score
// equals the IDF.
TEST(TfidfScorerTest, IdfDifferentiatesByDt) {
  TfidfScorer scorer;
  EXPECT_NEAR(
      ScoreLeaf(scorer, test_data::LeafForHello(test_data::kDocs[0]), 1.0f),
      1.0f, kFloatTolerance);
  EXPECT_NEAR(
      ScoreLeaf(scorer, test_data::LeafForRare(test_data::kDocs[5]), 1.0f),
      2.0f, kFloatTolerance);
  EXPECT_NEAR(
      ScoreLeaf(scorer, test_data::LeafForUnique(test_data::kDocs[5]), 1.0f),
      3.0f, kFloatTolerance);
}

TEST(TfidfScorerTest, ScoreLeafScalesWithFrequency) {
  TfidfScorer scorer;
  // hello IDF=1; doc:2 has TF=2 -> score 2.
  EXPECT_NEAR(
      ScoreLeaf(scorer, test_data::LeafForHello(test_data::kDocs[1]), 1.0f),
      2.0f, kFloatTolerance);
}

TEST(TfidfScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  TfidfScorer scorer;
  LeafData leaf = test_data::LeafForHello(test_data::kDocs[1]);
  const float base = ScoreLeaf(scorer, leaf, 1.0f);
  EXPECT_NEAR(ScoreLeaf(scorer, leaf, 5.0f), 5.0f * base, kFloatTolerance);
  EXPECT_EQ(ScoreLeaf(scorer, leaf, 0.0f), 0.0f);
}

TEST(TfidfScorerTest, ScoreLeafZeroFrequencyReturnsZero) {
  TfidfScorer scorer;
  LeafData leaf = test_data::LeafForWorld(test_data::kDocs[4]);
  ASSERT_EQ(leaf.term_frequency, 0u);
  EXPECT_EQ(ScoreLeaf(scorer, leaf, 1.0f), 0.0f);
}

// A term in no documents has dt=0; PrecomputeIDF returns 0 (rather than
// dividing by dt) so the leaf contributes nothing.
TEST(TfidfScorerTest, ScoreLeafEmptyIndexReturnsZero) {
  TfidfScorer scorer;
  LeafData leaf = MakeLeaf(/*N=*/0, /*dt=*/0, /*F=*/0);
  EXPECT_EQ(ScoreLeaf(scorer, leaf, 1.0f), 0.0f);
}

// dt > total_docs violates the IDF precondition, but callers clamp to uphold
// it. The guard is debug-only (DCHECK): it dies in debug builds, tolerated in
// release.
TEST(TfidfScorerDeathTest, DtGreaterThanNIsDebugOnly) {
  TfidfScorer scorer;
  LeafData leaf = MakeLeaf(/*N=*/2, /*dt=*/3, /*F=*/1);
  EXPECT_DEBUG_DEATH(ScoreLeaf(scorer, leaf, 1.0f), "");
}

TEST(TfidfScorerTest, ComposeMultipliesByDocumentScore) {
  TfidfScorer scorer;
  EXPECT_NEAR(
      scorer.ComposeDocumentScore({2.0f, /*document_score=*/0.8f, /*norm=*/1}),
      1.6f, kFloatTolerance);
}

// TFIDF clamps negative document scores (including -inf) to 0.
TEST(TfidfScorerTest, ComposeNegativeDocumentScoreClampsToZero) {
  TfidfScorer scorer;
  const float kInf = std::numeric_limits<float>::infinity();
  EXPECT_EQ(scorer.ComposeDocumentScore({2.0f, -10.0f, /*norm=*/1}), 0.0f);
  EXPECT_EQ(scorer.ComposeDocumentScore({2.0f, -kInf, /*norm=*/1}), 0.0f);
}

TEST(TfidfScorerTest, ComposePositiveInfinityShortCircuits) {
  TfidfScorer scorer;
  const float kInf = std::numeric_limits<float>::infinity();
  EXPECT_EQ(scorer.ComposeDocumentScore({2.0f, kInf, /*norm=*/1}), kInf);
}

// Redis oracle: a single-term query ties at 1.0 because TF cancels norm.
TEST(TfidfScorerTest, ComposeDividesByNorm) {
  TfidfScorer scorer;
  EXPECT_TRUE(scorer.NeedsNorm());
  EXPECT_NEAR(scorer.ComposeDocumentScore({2.0f, 1.0f, /*norm=*/2}), 1.0f,
              kFloatTolerance);
  EXPECT_NEAR(scorer.ComposeDocumentScore({5.0f, 1.0f, /*norm=*/5}), 1.0f,
              kFloatTolerance);
  EXPECT_NEAR(scorer.ComposeDocumentScore({3.0f, 1.0f, /*norm=*/2}), 1.5f,
              kFloatTolerance);
}

// A text-less document (tag/numeric/wildcard match) has norm 0 and scores 0.
TEST(TfidfScorerTest, ComposeZeroNormScoresZero) {
  TfidfScorer scorer;
  const float kInf = std::numeric_limits<float>::infinity();
  EXPECT_EQ(scorer.ComposeDocumentScore({2.0f, 1.0f, /*norm=*/0}), 0.0f);
  EXPECT_EQ(scorer.ComposeDocumentScore({2.0f, kInf, /*norm=*/0}), 0.0f);
}

// Per-document ranking (score-desc / key-asc ordering, document_score
// multiplier, AND/OR accumulation) is exercised end-to-end through the scoring
// integration suite (integration/test_scoring.py), since it depends on the
// predicate-tree walk and index metadata rather than the scorer in isolation.

}  // namespace
}  // namespace valkey_search::indexes::scoring
