import pandas as pd
import numpy as np

from postgres import Postgres
from fighters import Fighters


class Fights:
    properties = [
        "total_strikes",
        "significant_strikes",
        "takedowns",
        "head_strikes",
        "body_strikes",
        "leg_strikes",
        "distance_strikes",
        "clinch_strikes",
        "ground_strikes",
        "reversals",
        "submissions",
        "control_time",
        "age",
    ]

    modifiers = ["landed", "attempted", "absorbed"]

    _query = {
        "all": """
SELECT
  id as row_id,
  date,
  method,
  duration,
  fighter_id,
  opponent_id,
  winner_id
FROM
  fights
""",
    }

    @staticmethod
    def time_to_seconds(t: str) -> int:
        if pd.isnull(t):
            return np.nan
        if t == "0:00":
            return 0
        if "--" in t:
            return np.nan
        t = t.split(":")
        return int(t[0]) * 60 + int(t[1])

    @staticmethod
    def fight_results_time(row):
        returner = Fights.time_to_seconds(row["time"])
        if pd.isnull(returner):
            returner = row["round"] * 300
        row["time"] = returner
        return row

    @staticmethod
    def total_duration(row):
        returner = 0
        if row["round"] == 1:
            returner = row["time"]
        returner = row["time"] + (row["round"] - 1) * 300
        row["total_time"] = returner
        return row

    @staticmethod
    def round_to_int(round):
        if pd.isnull(round):
            return None
        return int(round.split(" ")[1])

    @staticmethod
    def get_stat_part(key, stat):
        if not isinstance(stat, str):
            return None
        if pd.isnull(stat):
            return None
        splitted = stat.split(" of ")
        if len(splitted) == 2:
            return splitted[key]
        return None

    @staticmethod
    def get_left_stat(stat):
        return Fights.get_stat_part(0, stat)

    @staticmethod
    def get_right_stat(stat):
        return Fights.get_stat_part(1, stat)

    def all(self) -> pd.DataFrame:
        returner = pd.DataFrame(
            self._pg.query(Fights._query.get("all")),
            columns=[
                "row_id",
                "date",
                "method",
                "duration",
                "fighter_id",
                "opponent_id",
                "winner_id",
            ],
            axis=1,
        )
        return returner

    def __init__(self, skip_creation=False):
        self._pg = Postgres()
        if not skip_creation:
            fighters_df = Fighters(True).all()
            events_df = pd.read_csv("./ufc_event_details.csv")
            events_column_mapping = {}
            for col in events_df.columns:
                events_column_mapping[col] = col.lower()
            events_df = events_df.rename(
                mapper=events_column_mapping, errors="raise", axis=1
            )
            events_df["event"] = (
                events_df["event"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .str.lower()
                .dropna()
            )
            events_df["date"] = pd.to_datetime(events_df["date"]).dropna()

            fight_results_df = pd.read_csv("./ufc_fight_results.csv")
            fight_results_column_mapping = {}
            for col in fight_results_df.columns:
                fight_results_column_mapping[col] = col.lower()
            fight_results_df = fight_results_df.rename(
                mapper=fight_results_column_mapping, errors="raise", axis=1
            )

            fight_results_df = fight_results_df.dropna(
                subset=["outcome", "weightclass", "bout", "event"]
            )
            fight_results_df = fight_results_df[
                (~fight_results_df["outcome"].isin(["D/D", "NC/NC"]))
            ]
            relevant_weight_classes = [
                "Flyweight",
                "Bantamweight",
                "Featherweight",
                "Lightweight",
                "Welterweight",
                "Middleweight",
                "Light Heavyweight",
                "Heavyweight",
            ]
            modded_relevant_weight_classes = []
            for weight_class in relevant_weight_classes:
                modded_relevant_weight_classes.append(weight_class + " Bout")
                modded_relevant_weight_classes.append(
                    "UFC " + weight_class + " Title Bout"
                )
                modded_relevant_weight_classes.append(
                    "UFC Interim " + weight_class + " Title Bout"
                )
            fight_results_df = fight_results_df[
                (fight_results_df["weightclass"].isin(modded_relevant_weight_classes))
            ]
            weightclass_mapping = {
                "Featherweight Bout": 145,
                "UFC Featherweight Title Bout": 145,
                "UFC Interim Featherweight Title Bout": 145,
                "Bantamweight Bout": 135,
                "UFC Bantamweight Title Bout": 135,
                "UFC Interim Bantamweight Title Bout": 135,
                "Lightweight Bout": 155,
                "UFC Lightweight Title Bout": 155,
                "UFC Interim Lightweight Title Bout": 155,
                "Welterweight Bout": 170,
                "UFC Welterweight Title Bout": 170,
                "UFC Interim Welterweight Title Bout": 170,
                "Middleweight Bout": 185,
                "UFC Middleweight Title Bout": 185,
                "UFC Interim Middleweight Title Bout": 185,
                "Light Heavyweight Bout": 205,
                "UFC Light Heavyweight Title Bout": 205,
                "UFC Interim Light Heavyweight Title Bout": 205,
                "Flyweight Bout": 125,
                "UFC Flyweight Title Bout": 125,
                "UFC Interim Flyweight Title Bout": 125,
                "Heavyweight Bout": 255,
                "UFC Heavyweight Title Bout": 255,
                "UFC Interim Heavyweight Title Bout": 255,
            }
            fight_results_df["weightclass"] = fight_results_df["weightclass"].map(
                weightclass_mapping
            )
            fight_results_df[["fighter_a", "fighter_b"]] = fight_results_df[
                "bout"
            ].str.split(" vs. ", expand=True)
            fight_results_df["fighter_a"] = (
                fight_results_df["fighter_a"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )
            fight_results_df["fighter_b"] = (
                fight_results_df["fighter_b"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )
            fight_results_df["outcome"] = fight_results_df["outcome"].apply(
                lambda x: 1 if x == "W/L" else 0
            )
            fight_results_df = fight_results_df[(fight_results_df["method"] != "dq ")]
            fight_results_df = fight_results_df.drop("referee", axis=1)
            fight_results_df = fight_results_df.drop("details", axis=1)
            fight_results_df["event"] = (
                fight_results_df["event"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .str.lower()
            )
            fight_results_df["bout"] = (
                fight_results_df["bout"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .str.lower()
            )
            fight_results_df["method"] = (
                fight_results_df["method"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .str.lower()
                .apply(lambda x: "decision" if "decision" in x else x)
            )
            fight_results_df = fight_results_df[
                (~fight_results_df["method"].isin(["tko - doctor's stoppage", "dq"]))
            ]

            fight_results_df = pd.merge(
                fight_results_df, events_df[["event", "date"]], on="event", how="left"
            ).dropna()
            fight_results_df = (
                fight_results_df.apply(Fights.fight_results_time, axis=1)
                .apply(pd.Series)
                .dropna()
            )
            fight_results_df = fight_results_df.apply(Fights.total_duration, axis=1)
            fight_results_df = fight_results_df.drop(
                columns=["url", "time format"], axis=1
            )

            fight_stats_df = pd.read_csv("./ufc_fight_stats.csv")
            fight_stats_df["ROUND"] = fight_stats_df["ROUND"].apply(Fights.round_to_int)
            fight_stats_df["ROUND"] = fight_stats_df["ROUND"].replace(
                [np.inf, -np.inf], np.nan
            )
            fight_stats_df["ROUND"] = fight_stats_df["ROUND"].dropna()

            for after, before in [
                ["SIG_STR_", "SIG.STR."],
                ["TOTAL_STR_", "TOTAL STR."],
                ["TD_", "TD"],
                ["HEAD_", "HEAD"],
                ["BODY_", "BODY"],
                ["LEG_", "LEG"],
                ["DISTANCE_", "DISTANCE"],
                ["CLINCH_", "CLINCH"],
                ["GROUND_", "GROUND"],
            ]:
                fight_stats_df[after + "LANDED"] = (
                    fight_stats_df[before].apply(Fights.get_left_stat).astype(float)
                )
                fight_stats_df[after + "LANDED"] = fight_stats_df[
                    after + "LANDED"
                ].astype(float)
                fight_stats_df[after + "ATTEMPTED"] = fight_stats_df[before].apply(
                    Fights.get_right_stat
                )
                fight_stats_df[after + "ATTEMPTED"] = fight_stats_df[
                    after + "ATTEMPTED"
                ].astype(float)
                fight_stats_df.drop(columns=[before], inplace=True)

            fight_stats_df["CTRL"] = (
                fight_stats_df["CTRL"]
                .apply(Fights.time_to_seconds)
                .dropna()
                .astype(float)
            )
            fight_stats_df.drop(columns=["SIG.STR. %", "TD %"], inplace=True)
            fight_stats_df["EVENT"] = (
                fight_stats_df["EVENT"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .str.lower()
                .reset_index(drop=True)
            )

            fight_stats_df["BOUT"] = (
                fight_stats_df["BOUT"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .str.lower()
                .reset_index(drop=True)
            )

            fight_stats_column_mapping = {}
            for col in fight_stats_df.columns:
                fight_stats_column_mapping[col] = col.lower()

            fight_stats_column_mapping["REV."] = "reversals_landed"
            fight_stats_column_mapping["CTRL"] = "control_time"
            fight_stats_column_mapping["SUB.ATT"] = "submissions_attempted"
            fight_stats_column_mapping["KD"] = "knockdowns_landed"

            fight_stats_df = fight_stats_df.rename(
                mapper=fight_stats_column_mapping, errors="raise", axis=1
            ).reset_index(drop=True)

            def get_fight_data(fighter, event, bout):
                fight_results_row = fight_results_df[
                    [
                        "fighter_a",
                        "fighter_b",
                        "date",
                        "outcome",
                        "method",
                        "total_time",
                    ]
                ][
                    (
                        (fight_results_df["fighter_a"] == fighter)
                        | (fight_results_df["fighter_b"] == fighter)
                    )
                    & (fight_results_df["event"] == event)
                    & (fight_results_df["bout"] == bout)
                ]
                if fight_results_row.empty:
                    return [None, None, None, None, None]
                outcome = fight_results_row["outcome"].values[0]
                method = fight_results_row["method"].values[0]
                if fight_results_row["fighter_a"].values[0] == fighter:
                    return [
                        fight_results_row["fighter_b"].values[0],
                        fight_results_row["date"].values[0],
                        outcome,
                        method,
                        fight_results_row["total_time"].values[0],
                    ]
                return [
                    fight_results_row["fighter_a"].values[0],
                    fight_results_row["date"].values[0],
                    1 if outcome == 0 else 0,
                    method,
                    fight_results_row["total_time"].values[0],
                ]

            def apply_fight_data(row):
                (
                    row["opponent"],
                    row["date"],
                    row["outcome"],
                    row["method"],
                    row["total_time"],
                ) = get_fight_data(row["fighter"], row["event"], row["bout"])
                return row

            fight_stats_df = fight_stats_df.apply(apply_fight_data, axis=1).dropna()

            def get_fighter_stats(fighter, event, bout):
                fighter_results = fight_stats_df[
                    (fight_stats_df["fighter"] == fighter)
                    & (fight_stats_df["event"] == event)
                    & (fight_stats_df["bout"] == bout)
                ]
                if fighter_results.shape[0] == 0:
                    return None
                fight_date = fighter_results["date"].head(1).item()
                fighter_results = fighter_results.drop(
                    columns=["fighter", "event", "bout", "round"]
                )
                fighter_stats = fighters_df[
                    ["weight", "height", "reach", "date_of_birth"]
                ][fighters_df["name"] == fighter].head(1)
                if fighter_stats.shape[0] == 0:
                    return None
                opponent_name = fighter_results["opponent"].head(1).item()
                age = fight_date - fighter_stats["date_of_birth"].item()
                age = age.days / 365.25
                returner = {
                    "date": fight_date,
                    "event": event,
                    "bout": bout,
                    "fighter": fighter,
                    "opponent": opponent_name,
                    "weight": float(fighter_stats["weight"].astype(float).values[0]),
                    "height": float(fighter_stats["height"].astype(float).values[0]),
                    "reach": float(fighter_stats["reach"].astype(float).values[0]),
                    "age": float(age),
                    "outcome": float(
                        fighter_results["outcome"].astype(float).values[0]
                    ),
                    "method": fighter_results["method"].values[0],
                    "total_time": float(
                        fighter_results["total_time"].astype(float).values[0]
                    ),
                }
                for col in filter(
                    lambda x: x.endswith("landed")
                    or x.endswith("attempted")
                    or x == "control_time",
                    fighter_results.columns,
                ):
                    returner[col] = float(fighter_results[col].sum())
                return returner

            def apply_get_fighter_stats(row):
                return get_fighter_stats(row["fighter"], row["event"], row["bout"])

            unique_fights = fight_stats_df[
                ["fighter", "event", "bout"]
            ].drop_duplicates()
            unique_fights["fighter"] = unique_fights["fighter"].str.strip().dropna()
            unique_fights["event"] = unique_fights["event"].str.strip().dropna()
            unique_fights["bout"] = unique_fights["bout"].str.strip().dropna()
            fights_df = (
                unique_fights.apply(apply_get_fighter_stats, axis=1)
                .apply(pd.Series)
                .dropna()
            )

            for index, row in fights_df.iterrows():
                print("\n\nfight %s - %s" % (row["event"], row["bout"]))
                fighter = fighters_df[(fighters_df["name"] == row["fighter"])]
                if fighter.empty:
                    continue
                opponent = fighters_df[(fighters_df["name"] == row["opponent"])]
                if opponent.empty:
                    continue
                fighter_id = int(fighter["row_id"].values[0])
                opponent_id = int(opponent["row_id"].values[0])
                # Can't just upsert because the fighter could be the fighter or the opponent and we don't want to create two rows per fight
                existing_id = self._pg.one(
                    "SELECT id FROM fights WHERE date = %s AND (fighter_id = %s OR fighter_id = %s) AND (opponent_id = %s OR opponent_id = %s)",
                    (
                        row["date"].to_datetime64().astype(str),
                        fighter_id,
                        opponent_id,
                        fighter_id,
                        opponent_id,
                    ),
                )
                row_id = None
                if existing_id:
                    row_id = existing_id
                else:
                    row_id = self._pg.insert(
                        "INSERT INTO fights (date, method, duration, fighter_id, opponent_id, winner_id) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (fighter_id, opponent_id, date) DO NOTHING RETURNING id",
                        (
                            row["date"].to_datetime64().astype(str),
                            row["method"],
                            row["total_time"],
                            fighter_id,
                            opponent_id,
                            (fighter_id if row["outcome"] < 1.0 else opponent_id),
                        ),
                    )
                fights_df.at[index, "row_id"] = row_id
                for from_stat, to_stat in [
                    ("total_str", "total_strikes"),
                    ("sig_str", "significant_strikes"),
                    ("td", "takedowns"),
                    ("head", "head_strikes"),
                    ("body", "body_strikes"),
                    ("leg", "leg_strikes"),
                    ("distance", "distance_strikes"),
                    ("clinch", "clinch_strikes"),
                    ("ground", "ground_strikes"),
                    ("submissions", "submissions"),
                    ("reversals", "reversals"),
                ]:
                    for k in ["attempted", "landed"]:
                        if from_stat + "_" + k not in row:
                            continue
                        v = row[from_stat + "_" + k]
                        print("%s_%s: %s" % (from_stat, k, v))
                        if v is None:
                            continue
                        self._pg.insert(
                            "insert into fight_stats (property, modifier, type, value, fighter_id, fight_id) values (%s, %s, %s, %s, %s, %s) on conflict (fight_id, fighter_id, property, modifier, type) do update set value = excluded.value returning id",
                            (
                                to_stat,
                                k,
                                "total",
                                v,
                                fighter_id,
                                row_id,
                            ),
                        )

                self._pg.insert(
                    "insert into fight_stats (property, modifier, type, value, fighter_id, fight_id) values (%s, %s, %s, %s, %s, %s) on conflict (fight_id, fighter_id, property, modifier, type) do update set value = excluded.value returning id",
                    (
                        "control_time",
                        None,
                        "total",
                        row["control_time"],
                        fighter_id,
                        row_id,
                    ),
                )
                self._pg.insert(
                    "insert into fight_stats (property, modifier, type, value, fighter_id, fight_id) values (%s, %s, %s, %s, %s, %s) on conflict (fight_id, fighter_id, property, modifier, type) do update set value = excluded.value returning id",
                    (
                        "age",
                        None,
                        "total",
                        row["age"],
                        fighter_id,
                        row_id,
                    ),
                )
