/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/tfidf_scorer.h"

#include <limits>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_session.h"
#include "src/indexes/scoring/scoring_stats.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

using ::testing::ElementsAre;

constexpr float kFloatTolerance = 1e-4f;

std::vector<DocId> RankOrder(const std::vector<RankedDoc>& ranked) {
  std::vector<DocId> ids;
  ids.reserve(ranked.size());
  for (const auto& r : ranked) ids.push_back(r.doc_id);
  return ids;
}

float ScoreFor(const std::vector<RankedDoc>& ranked, DocId id) {
  for (const auto& r : ranked) {
    if (r.doc_id == id) return r.score;
  }
  ADD_FAILURE() << "doc_id " << id << " not in ranked results";
  return 0.0f;
}

// --- Direct scorer math ---

TEST(TfidfScorerTest, IdentityNameAndType) {
  TfidfScorer scorer;
  EXPECT_EQ(scorer.Name(), "TFIDF");
  EXPECT_EQ(scorer.Type(), ScorerType::kTfidf);
}

// IDF = floor(log2(1 + (N+1)/dt)); hello (dt=6) -> 1, rare (dt=2) -> 2,
// unique (dt=1) -> 3.
TEST(TfidfScorerTest, IdfDifferentiatesByDt) {
  TfidfScorer scorer;
  EXPECT_NEAR(scorer.ScoreLeaf(
                  test_data::TfidfStatsForHello(test_data::kDocs[0]), 1.0f),
              1.0f, kFloatTolerance);
  EXPECT_NEAR(
      scorer.ScoreLeaf(test_data::TfidfStatsForRare(test_data::kDocs[7]), 1.0f),
      2.0f, kFloatTolerance);
  EXPECT_NEAR(scorer.ScoreLeaf(
                  test_data::TfidfStatsForUnique(test_data::kDocs[5]), 1.0f),
              3.0f, kFloatTolerance);
}

TEST(TfidfScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  TfidfScorer scorer;
  TfidfStats stats = test_data::TfidfStatsForHello(test_data::kDocs[1]);
  const float base = scorer.ScoreLeaf(stats, 1.0f);
  EXPECT_NEAR(scorer.ScoreLeaf(stats, 5.0f), 5.0f * base, kFloatTolerance);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 0.0f), 0.0f);
}

// norm == 0 marks a doc with no TEXT fields; final score is forced to 0.
TEST(TfidfScorerTest, ComposeZeroNormReturnsZero) {
  TfidfScorer scorer;
  TfidfStats stats = test_data::TfidfStatsForHello(test_data::kDocs[0]);
  stats.norm = 0;
  EXPECT_EQ(scorer.ComposeDocumentScore(5.0f, stats), 0.0f);
}

TEST(TfidfScorerTest, ComposeDividesByNorm) {
  TfidfScorer scorer;
  TfidfStats stats = test_data::TfidfStatsForHello(test_data::kDocs[3]);
  ASSERT_EQ(stats.norm, 5u);
  EXPECT_NEAR(scorer.ComposeDocumentScore(6.0f, stats), 1.2f, kFloatTolerance);
}

TEST(TfidfScorerTest, ComposeMultipliesByDocumentScore) {
  TfidfScorer scorer;
  TfidfStats stats = test_data::TfidfStatsForHello(test_data::kDocs[0]);
  stats.document_score = 0.8f;
  EXPECT_NEAR(scorer.ComposeDocumentScore(2.0f, stats), 1.6f, kFloatTolerance);
}

// TFIDF clamps negative document scores (including -inf) to 0.
TEST(TfidfScorerTest, ComposeNegativeDocumentScoreClampsToZero) {
  TfidfScorer scorer;
  TfidfStats stats = test_data::TfidfStatsForHello(test_data::kDocs[0]);
  stats.document_score = -10.0f;
  EXPECT_EQ(scorer.ComposeDocumentScore(2.0f, stats), 0.0f);
  stats.document_score = -std::numeric_limits<float>::infinity();
  EXPECT_EQ(scorer.ComposeDocumentScore(2.0f, stats), 0.0f);
}

TEST(TfidfScorerTest, ComposePositiveInfinityShortCircuits) {
  TfidfScorer scorer;
  const float kInf = std::numeric_limits<float>::infinity();
  TfidfStats stats = test_data::TfidfStatsForHello(test_data::kDocs[0]);
  stats.document_score = kInf;
  EXPECT_EQ(scorer.ComposeDocumentScore(2.0f, stats), kInf);
}

TEST(TfidfScorerDeathTest, WrongStatsSubtypeCrashes) {
  TfidfScorer scorer;
  Bm25StdStats wrong_stats = test_data::StatsForHello(test_data::kDocs[0]);
  EXPECT_DEATH(scorer.ScoreLeaf(wrong_stats, 1.0f), "");
}

// --- Query-driven tests ---
//
// Each test reproduces one query from design/tfidf_redis_test_results.md
// (Redis 8.6, SCORER TFIDF, NOSTEM corpus). slop is not implemented on this
// branch and is 1 for every query in that file, so our scores match Redis
// exactly. Asserted scores are the Redis-reported final values.

// `hello`: single leaf, score normalized to 1 for every matching doc.
TEST(TfidfScorerQueryTest, Hello) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 3, 4, 5, 7));
  for (DocId id : {1, 2, 3, 4, 5, 7}) {
    EXPECT_NEAR(ScoreFor(ranked, id), 1.0f, kFloatTolerance);
  }
}

// `hello world` (implicit AND): only docs with both terms are admitted.
TEST(TfidfScorerQueryTest, HelloWorldAnd) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0 && doc.f_world > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 1.0f);
      session.RecordLeaf(test_data::TfidfStatsForWorld(doc), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 7, 3, 4));
  EXPECT_NEAR(ScoreFor(ranked, 1), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 1.333333f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 1.2f, kFloatTolerance);
}

// `hello | world` (OR): a doc is admitted if either term matches.
TEST(TfidfScorerQueryTest, HelloWorldOr) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 1.0f);
    }
    if (doc.f_world > 0) {
      session.RecordLeaf(test_data::TfidfStatsForWorld(doc), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 7, 3, 4, 5, 6));
  EXPECT_NEAR(ScoreFor(ranked, 1), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 1.333333f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 1.2f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 5), 1.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 6), 1.0f, kFloatTolerance);
}

// `rare`: dt=2 -> IDF=2; both matching docs score 2.
TEST(TfidfScorerQueryTest, Rare) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_rare > 0) {
      session.RecordLeaf(test_data::TfidfStatsForRare(doc), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(6, 8));
  EXPECT_NEAR(ScoreFor(ranked, 6), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 8), 2.0f, kFloatTolerance);
}

// `unique`: dt=1 -> IDF=3; doc:6 alone.
TEST(TfidfScorerQueryTest, Unique) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_unique > 0) {
      session.RecordLeaf(test_data::TfidfStatsForUnique(doc), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(6));
  EXPECT_NEAR(ScoreFor(ranked, 6), 3.0f, kFloatTolerance);
}

// `(hello)=>{$weight:5}`: leaf weight scales every score to 5.
TEST(TfidfScorerQueryTest, HelloLeafWeight) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 5.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 3, 4, 5, 7));
  for (DocId id : {1, 2, 3, 4, 5, 7}) {
    EXPECT_NEAR(ScoreFor(ranked, id), 5.0f, kFloatTolerance);
  }
}

// `((hello)=>{$weight:4} (world)=>{$weight:3})=>{$weight:2}`:
//   final = 2 * (4*hello + 3*world) / norm
TEST(TfidfScorerQueryTest, NestedGroupsLayeredWeights) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  session.EnterGroup();
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0 && doc.f_world > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 4.0f);
      session.RecordLeaf(test_data::TfidfStatsForWorld(doc), 3.0f);
    }
  }
  session.ExitGroup(2.0f);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 7, 3, 4));
  EXPECT_NEAR(ScoreFor(ranked, 1), 14.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 11.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 11.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 10.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 9.2f, kFloatTolerance);
}

// `(hello world) | rare`: two admission paths. doc:6 enters via `rare` so its
// `world` token contributes nothing.
TEST(TfidfScorerQueryTest, OrAtTopMixedAdmissionPaths) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_rare > 0) {
      session.RecordLeaf(test_data::TfidfStatsForRare(doc), 1.0f);
    } else if (doc.f_hello > 0 && doc.f_world > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 1.0f);
      session.RecordLeaf(test_data::TfidfStatsForWorld(doc), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 6, 8, 2, 7, 3, 4));
  EXPECT_NEAR(ScoreFor(ranked, 1), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 6), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 8), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 1.333333f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 1.2f, kFloatTolerance);
}

// `hello (world | rare)`: admitted docs (1-4, 7) lack `rare`, so the OR group
// contributes only `world` and scores match plain `hello world`.
TEST(TfidfScorerQueryTest, AndOfLeafAndOrGroup) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    const bool or_match = doc.f_world > 0 || doc.f_rare > 0;
    if (doc.f_hello > 0 && or_match) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 1.0f);
      session.EnterGroup();
      if (doc.f_world > 0) {
        session.RecordLeaf(test_data::TfidfStatsForWorld(doc), 1.0f);
      }
      if (doc.f_rare > 0) {
        session.RecordLeaf(test_data::TfidfStatsForRare(doc), 1.0f);
      }
      session.ExitGroup(1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 7, 3, 4));
  EXPECT_NEAR(ScoreFor(ranked, 1), 2.0f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 1.5f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 1.333333f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 1.2f, kFloatTolerance);
}

// `((hello)=>{$weight:4} | (rare)=>{$weight:2})=>{$weight:3}`:
//   final = 3 * (4*hello if matched + 2*rare if matched) / norm
// Every admitted doc lands on 12.
TEST(TfidfScorerQueryTest, PerLeafWeightInsideOrWithGroupWeight) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello == 0 && doc.f_rare == 0) continue;
    session.EnterGroup();
    if (doc.f_hello > 0) {
      session.RecordLeaf(test_data::TfidfStatsForHello(doc), 4.0f);
    }
    if (doc.f_rare > 0) {
      session.RecordLeaf(test_data::TfidfStatsForRare(doc), 2.0f);
    }
    session.ExitGroup(3.0f);
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(1, 2, 3, 4, 5, 6, 7, 8));
  for (DocId id : {1, 2, 3, 4, 5, 6, 7, 8}) {
    EXPECT_NEAR(ScoreFor(ranked, id), 12.0f, kFloatTolerance);
  }
}

// `nonexistent`: no leaves recorded; ranking is empty.
TEST(TfidfScorerQueryTest, NonExistentTermEmpty) {
  TfidfScorer scorer;
  ScoringSession session(&scorer);
  auto ranked = session.Rank();
  EXPECT_TRUE(ranked.empty());
}

}  // namespace
}  // namespace valkey_search::indexes::scoring
