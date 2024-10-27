import pandas as pd
import numpy as np
import xgboost as xgb
from odds import Odds
from datetime import datetime, timedelta

oddsApi = Odds()


class StatsLib:
    def fighter_stats(self, fighter, before=np.datetime64("now")):
        returner = {}
        latest = (
            self.df[
                filter(
                    lambda x: ("avg" in x)
                    or x in ["age", "height", "weight", "reach", "date"],
                    self.df.columns,
                )
            ][(self.df["fighter"] == fighter) & (self.df["date"] < before)]
            .head(1)
            .squeeze()
            .to_dict()
        )
        for key, value in latest.items():
            returner[key] = value
        return pd.Series(returner)

    def fighter_history(self, fighter, date):
        full_history = self.df[
            (self.df["fighter"] == fighter) & (self.df["date"] < date)
        ]
        recent_history = full_history[
            full_history["date"] >= date - pd.Timedelta(days=730)
        ]
        returner = {}
        for key in ["weight", "height", "reach", "age"]:
            returner["avg_" + key + "_diff"] = (
                full_history[key + "_diff"].mean() if full_history.shape[0] > 0 else 0.0
            )
            returner["recent_avg_" + key + "_diff"] = (
                recent_history[key + "_diff"].mean()
                if recent_history.shape[0] > 0
                else 0.0
            )
        to_do = []
        for key in [
            "knockdowns",
            "reversals",
            "control_time",
            "age",
            "weight",
            "reach",
            "height",
        ]:
            to_do += [key, key + "_diff"]
        for key in [
            "total_str",
            "sig_str",
            "td",
            "ground",
            "head",
            "body",
            "leg",
            "distance",
            "clinch",
        ]:
            for suffix in [
                "landed_diff",
                "attempted_diff",
                "absorbed_diff",
                "defended_diff",
                "landed",
                "attempted",
                "absorbed",
                "defended",
            ]:
                to_do.append("_".join([key, suffix]))
        for key in to_do:
            avgK = "_".join(["avg", key])
            peakK = "_".join([key, "peak"])
            valleyK = "_".join([key, "valley"])
            recentAvgK = "_".join(["recent_avg", key])
            returner[avgK] = (
                full_history[key].mean() if full_history.shape[0] > 0 else 0.0
            )
            returner[recentAvgK] = (
                recent_history[key].mean() if recent_history.shape[0] > 0 else 0.0
            )
            returner[peakK] = (
                full_history[key].max() if full_history.shape[0] > 0 else 0.0
            )
            returner[valleyK] = (
                full_history[key].min() if full_history.shape[0] > 0 else 0.0
            )
            returner[recentAvgK + "_vs_peak"] = (
                returner[recentAvgK] / returner[peakK] if returner[peakK] > 0 else 0.0
            )
            returner[recentAvgK + "_vs_valley"] = (
                returner[recentAvgK] / returner[valleyK]
                if returner[valleyK] > 0
                else 0.0
            )
            returner[avgK + "_vs_peak"] = (
                returner[avgK] / returner[peakK] if returner[peakK] > 0 else 0.0
            )
            returner[avgK + "_vs_valley"] = (
                returner[avgK] / returner[valleyK] if returner[valleyK] > 0 else 0.0
            )
        returner["recent_wins"] = recent_history[recent_history["outcome"] == 1].shape[
            0
        ]
        returner["recent_losses"] = recent_history[
            recent_history["outcome"] == 0
        ].shape[0]
        returner["wins"] = float(full_history[full_history["outcome"] == 1].shape[0])
        returner["losses"] = float(full_history[full_history["outcome"] == 0].shape[0])
        returner["win_ratio"] = (
            returner["wins"] / (returner["wins"] + returner["losses"])
            if (returner["wins"] + returner["losses"]) > 0
            else 0.0
        )
        for method, transformed in [
            ["t/ko", "ko"],
            ["submission", "sub"],
            ["decision", "dec"],
        ]:
            returner[transformed + "_wins"] = full_history[
                (full_history["method"] == method) & (full_history["outcome"] == 1)
            ]["outcome"].sum()
            returner[transformed + "_losses"] = full_history[
                (full_history["method"] == method) & (full_history["outcome"] == 0)
            ]["outcome"].sum()
            returner[transformed + "_win_ratio"] = (
                returner[transformed + "_wins"]
                / (returner[transformed + "_wins"] + returner[transformed + "_losses"])
                if (returner[transformed + "_wins"] + returner[transformed + "_losses"])
                > 0
                else 0.0
            )
            returner[transformed + "_loss_ratio"] = (
                returner[transformed + "_losses"]
                / (returner[transformed + "_losses"] + returner[transformed + "_wins"])
                if (returner[transformed + "_losses"] + returner[transformed + "_wins"])
                > 0
                else 0.0
            )
            returner["recent_" + transformed + "_wins"] = recent_history[
                (recent_history["method"] == method) & (recent_history["outcome"] == 1)
            ]["outcome"].shape[0]
            returner["recent_" + transformed + "_losses"] = recent_history[
                (recent_history["method"] == method) & (recent_history["outcome"] == 0)
            ]["outcome"].shape[0]
        return returner

    def fighter_history_diffs(self, fighter, opponent, date):
        returner = {}
        fighter_history = self.fighter_history(fighter, date)
        for key in fighter_history:
            returner["precomp_" + key] = fighter_history[key]
        opponent_history = self.fighter_history(opponent, date)
        for key in opponent_history:
            returner["opponent_precomp_" + key] = opponent_history[key]
        to_do = []
        for key in [
            "knockdowns",
            "reversals",
            "control_time",
            "age",
            "weight",
            "reach",
            "height",
        ]:
            to_do += [key, key + "_diff"]
        for key in [
            "total_str",
            "sig_str",
            "td",
            "ground",
            "head",
            "body",
            "leg",
            "distance",
            "clinch",
        ]:
            for suffix in [
                "landed_diff",
                "attempted_diff",
                "landed",
                "attempted",
                "absorbed",
                "defended",
            ]:
                to_do.append("_".join([key, suffix]))
        for key in to_do:
            avgK = "_".join(["avg", key])
            peakK = "_".join([key, "peak"])
            valleyK = "_".join([key, "valley"])
            recentAvgK = "_".join(["recent_avg", key])
            for k in [avgK, peakK, valleyK, recentAvgK]:
                returner["precomp_" + k + "_vs_opp"] = (
                    returner["precomp_" + k] - returner["opponent_precomp_" + k]
                )
        return returner

    def predict_outcome(self, fighter, opponent, date=np.datetime64("now")):
        df = pd.DataFrame([self.fighter_history_diffs(fighter, opponent, date)]).filter(
            like="precomp"
        )
        x = self.model.predict(xgb.DMatrix(df, enable_categorical=True))[0]
        return x

    def __init__(
        self,
        dataframe=pd.read_csv("fighter_stats.csv"),
        model_path="model.json",
    ):
        self.df = dataframe.copy()
        self.df["date"] = pd.to_datetime(self.df["date"])
        print("Copied dataframe size: %d" % self.df.shape[0])
        self.model = xgb.Booster()
        self.model.load_model(model_path)

        print("Library initialized")


# EXAMPLE USAGE:
# stats = StatsLib()
# events = oddsApi.list_fights(
#     (datetime.now()).isoformat().split(".")[0] + "Z",
#     (datetime.now() + timedelta(days=2)).isoformat().split(".")[0] + "Z",
# )
# fightDate = np.datetime64("2024-10-25")
# print("UFC 308: Topuria vs. Holloway\n")
# for fighter, opponent in [
#     [
#         "Ilia Topuria",
#         "Max Holloway",
#     ],
#     [
#         "Robert Whittaker",
#         "Khamzat Chimaev",
#     ],
#     [
#         "Magomed Ankalaev",
#         "Aleksandar Rakić",
#     ],
#     [
#         "Lerone Murphy",
#         "Dan Ige",
#     ],
#     [
#         "Shara Magomedov",
#         "Armen Petrosyan",
#     ],
#     [
#         "Ibo Aslan",
#         "Raffael Cerqueira",
#     ],
#     [
#         "Geoff Neal",
#         "Rafael dos Anjos",
#     ],
#     [
#         "Myktybek Orolbai",
#         "Mateusz Rębecki",
#     ],
#     [
#         "Abus Magomedov",
#         "Brunno Ferreira",
#     ],
#     [
#         "Kennedy Nzechukwu",
#         "Chris Barnett",
#     ],
#     [
#         "Farid Basharat",
#         "Victor Hugo",
#     ],
#     [
#         "Rinat Fakhretdinov",
#         "Carlos Leal",
#     ],
# ]:
#     a_pred = stats.predict_outcome(opponent, fighter, fightDate)
#     prob = 1.0 / (1.0 + np.exp(-a_pred))
#     print("Fight: %s vs. %s" % (fighter, opponent))
#     print("Predicted probability: %.2f%%" % (prob * 100.0))
#     a_pct = prob * 100.0
#     b_pct = 100.0 - a_pct
#     for e in events:
#         if (e["home_team"] != fighter and e["away_team"] != fighter) and (
#             e["home_team"] != opponent or e["away_team"] != opponent
#         ):
#             continue
#         odds = oddsApi.get_odds(e["id"])[0]
#         a = []
#         b = []
#         for bm in odds["bookmakers"]:
#             for outcome in bm["markets"][0]["outcomes"]:
#                 if outcome["name"] == fighter:
#                     a.append(outcome["price"])
#                 else:
#                     b.append(outcome["price"])
#         a_odds = np.mean(a)
#         b_odds = np.mean(b)

#     print(
#         "%s (%.2f%%) %.2f vs. %s (%.2f%%) %.2f"
#         % (fighter, a_pct, a_odds, opponent, b_pct, b_odds)
#     )
