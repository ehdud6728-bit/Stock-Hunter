"""R24 deterministic offline tests for train-only model and D0 temporal integrity."""
import copy
import unittest

import numpy as np

import r24_entry_risk as r24


def event(date:str,i:int,code:str="000001")->dict:
    # Synthetic variation prevents single-class fit and constant-only features.
    feat={
       "close_location":.5+.4*(i%3)/3,
       "amount_vs_prev20":.5+(i%5)*.4,
       "volume_vs_prev20":.7+(i%7)*.23,
       "return_prev20_pct":-4+(i%9)*1.5,
       "breakout_vs_prior55_high_pct":.1+(i%4)*.7,
    }
    net=(-6.0 if i%3==0 else (1.5 if i%3==1 else 5.0))
    return {
      "code":code,"date":date,"year":int(date[:4]),"features_d0":list(feat.values()),
      "outcomes":{
         r24.BASE:{"net_pct":net,"exit_day":3},
         r24.SECONDARY:{"net_pct":net-2.0,"exit_day":7}
      },
      "labels_EXPOST_NOT_FEATURES":{
         "ma5_is_loss":int(net<=0),
         "max_favorable_60_pct":40.0 if i%5==0 else 3.0,
         "big30_opportunity":i%5==0,
      }
    }


def cohorts():
    train=[event(f"2022-02-{(i%25)+1:02d}",i,code=f"C{i%6}") for i in range(42)]
    gap=[event(f"2023-11-{i+1:02d}",i+42) for i in range(6)]
    review=[event(f"2024-03-{(i%25)+1:02d}",i+48,code=f"C{i%6}") for i in range(24)]
    return train,gap,review


class R24OfflineTests(unittest.TestCase):
    def test_d0_features_are_fixed_five_columns(self):
        self.assertEqual(len(r24.FEATURES),5)
        self.assertNotIn("future_mfe",r24.FEATURES)
        self.assertNotIn("close_vs_ma224_pct",r24.FEATURES)

    def test_vector_ignores_post_entry_outcomes_and_future_labels(self):
        e={"features":dict(zip(r24.FEATURES,[.8,2.,1.7,5.,3.])),
           "outcomes":{"MA5":{"net_pct":-80}},
           "expost_not_tradable":{"max_favorable_60_pct":400}}
        first=r24.vector(e)
        e["outcomes"]["MA5"]["net_pct"]=1000
        e["expost_not_tradable"]["max_favorable_60_pct"]=9000
        self.assertEqual(first,r24.vector(e))

    def test_nans_are_retained_as_missing_not_zero(self):
        e={"features":dict(zip(r24.FEATURES,[None,2.,float("inf"),3.,"invalid"]))}
        v=r24.vector(e)
        self.assertTrue(np.isnan(v[0]))
        self.assertTrue(np.isnan(v[2]))
        self.assertTrue(np.isnan(v[4]))
        self.assertEqual(v[1],2.)

    def test_temporal_split_purges_q4_2023(self):
        train,gap,review=cohorts()
        tr,ga,re=r24.fixed_split(train+gap+review)
        self.assertEqual((len(tr),len(ga),len(re)),(42,6,24))
        self.assertLess(max(x["date"] for x in tr),r24.REVIEW_START)
        self.assertGreaterEqual(min(x["date"] for x in re),r24.REVIEW_START)

    def test_review_labels_do_not_affect_training_cutoff_or_scores(self):
        train,_,review=cohorts()
        clf,cut=r24.fit_train_only(train)
        first=r24.score_review(clf,cut,review)
        changed=copy.deepcopy(review)
        for e in changed:
            e["labels_EXPOST_NOT_FEATURES"]["ma5_is_loss"]=1-e["labels_EXPOST_NOT_FEATURES"]["ma5_is_loss"]
            e["outcomes"][r24.BASE]["net_pct"]=999.
            e["labels_EXPOST_NOT_FEATURES"]["big30_opportunity"]=True
        second=r24.score_review(clf,cut,changed)
        self.assertEqual([x["loss_risk_score"] for x in first],
                         [x["loss_risk_score"] for x in second])
        self.assertEqual([x["research_review_group"] for x in first],
                         [x["research_review_group"] for x in second])

    def test_training_is_reproducible_fixed_c_and_quantile(self):
        train,_,_=cohorts()
        c1,q1=r24.fit_train_only(train)
        c2,q2=r24.fit_train_only(train)
        self.assertEqual(q1,q2)
        self.assertTrue(np.array_equal(
            c1.predict_proba(np.array([x["features_d0"] for x in train])),
            c2.predict_proba(np.array([x["features_d0"] for x in train]))))

    def test_single_class_fails_closed(self):
        train,_,_=cohorts()
        for r in train:
            r["labels_EXPOST_NOT_FEATURES"]["ma5_is_loss"]=1
        with self.assertRaisesRegex(ValueError,"ONE_OUTCOME_CLASS"):
            r24.fit_train_only(train)

    def test_training_too_small_fails(self):
        train,gap,review=cohorts()
        with self.assertRaisesRegex(ValueError,"INSUFFICIENT"):
            r24.fixed_split(train[:10]+gap+review)

    def test_group_stats_exposes_outliers(self):
        train,_,_=cohorts()
        d=r24.group_stats(train,r24.BASE)
        self.assertEqual(d["n"],42)
        self.assertIsNotNone(d["mean_excluding_best_pct"])
        self.assertIn("big30_opportunity_n_EXPOST",d)

    def test_empty_group_reports_none_not_zero_return(self):
        d=r24.group_stats([],r24.BASE)
        self.assertEqual(d["n"],0)
        self.assertIsNone(d["mean_net_pct"])

    def test_label_mismatch_fails_source_gate(self):
        x={"code":"000001","date":"2022-01-03","year":2022,
           "features":dict(zip(r24.FEATURES,[.8,1.,1.,2.,3.])),
           "outcomes":{"D0_LOW_NEXT_OPEN":{"net_pct":1},
                       "PREV5_LOW_NEXT_OPEN":{"net_pct":1},
                       "PREV10_LOW_NEXT_OPEN":{"net_pct":1}},
           "expost_not_tradable":{"max_favorable_60_pct":30}}
        with self.assertRaisesRegex(ValueError,"IDENTITY_MISMATCH"):
            r24.by_ids([x],[])

    def test_reviews_use_same_retain_fraction_for_both_exit_policies(self):
        train,_,review=cohorts()
        clf,cut=r24.fit_train_only(train)
        scored=r24.score_review(clf,cut,review)
        metrics=r24.review_metrics(scored,cut)
        self.assertEqual(metrics["review_n"],len(review))
        self.assertEqual(metrics["retained"][r24.BASE]["n"],
                         metrics["retained"][r24.SECONDARY]["n"])
        self.assertEqual(metrics["high_risk_rejected"][r24.BASE]["n"],
                         metrics["high_risk_rejected"][r24.SECONDARY]["n"])
        self.assertEqual(
          metrics["retained"][r24.BASE]["n"]+metrics["high_risk_rejected"][r24.BASE]["n"],24)

    def test_review_loss_label_is_not_model_feature(self):
        train,_,review=cohorts()
        clf,cut=r24.fit_train_only(train)
        scores=r24.score_review(clf,cut,review)
        for r in scores:
            self.assertEqual(len(r["features_d0"]),5)
        self.assertTrue(all(0<=r["loss_risk_score"]<=1 for r in scores))


if __name__=="__main__":
    unittest.main()
