import pandas as pd
import numpy as np

events_df = pd.read_csv("ufc_event_details.csv")

column_mapping = {}
for col in events_df.columns:
    column_mapping[col] = col.lower()
events_df = events_df.rename(mapper=column_mapping, errors="raise", axis=1)

events_df["event"] = (
    events_df["event"]
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
    .str.lower()
    .dropna()
)
events_df["date"] = pd.to_datetime(events_df["date"]).dropna()

fight_results_df = pd.read_csv("./ufc_fight_results.csv")

column_mapping = {}
for col in fight_results_df.columns:
    column_mapping[col] = col.lower()
fight_results_df = fight_results_df.rename(
    mapper=column_mapping, errors="raise", axis=1
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
]
modded_relevant_weight_classes = []
for weight_class in relevant_weight_classes:
    modded_relevant_weight_classes.append(weight_class + " Bout")
    modded_relevant_weight_classes.append("UFC " + weight_class + " Title Bout")
    modded_relevant_weight_classes.append("UFC Interim " + weight_class + " Title Bout")
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
fight_results_df[["fighter_a", "fighter_b"]] = fight_results_df["bout"].str.split(
    " vs. ", expand=True
)
fight_results_df["fighter_a"] = (
    fight_results_df["fighter_a"].str.replace(r"\s+", " ", regex=True).str.strip()
)
fight_results_df["fighter_b"] = (
    fight_results_df["fighter_b"].str.replace(r"\s+", " ", regex=True).str.strip()
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


def time_to_seconds(t):
    if pd.isnull(t):
        return np.nan
    if t == "0:00":
        return 0
    if "--" in t:
        return np.nan
    t = t.split(":")
    return int(t[0]) * 60 + int(t[1])


def fight_results_time(row):
    returner = time_to_seconds(row["time"])
    if pd.isnull(returner):
        returner = row["round"] * 300
    row["time"] = returner
    return row


fight_results_df = (
    fight_results_df.apply(fight_results_time, axis=1).apply(pd.Series).dropna()
)


def total_duration(row):
    returner = 0
    if row["round"] == 1:
        returner = row["time"]
    returner = row["time"] + (row["round"] - 1) * 300
    row["total_time"] = returner
    return row


fight_results_df = fight_results_df.apply(total_duration, axis=1)

fight_results_df = fight_results_df.drop(columns=["url", "time format"], axis=1)


def height_to_inches(height):
    if height == "--":
        return None
    feet, inches = height.split("' ")
    inches = inches.replace('"', "")
    return int(feet) * 12 + int(inches)


def reach_to_inches(reach):
    if reach == "--":
        return None
    return int(reach.replace('"', ""))


def weight_to_num(weight):
    if not " lbs." in weight:
        return None
    return int(weight.replace(" lbs.", ""))


fighters_df = pd.read_csv("./ufc_fighter_tott.csv")
column_mapping = {}
for col in fighters_df.columns:
    column_mapping[col] = col.lower()
fighters_df = fighters_df.rename(mapper=column_mapping, errors="raise", axis=1)
# Remove Bruno Silvas
fighters_df = fighters_df[fighters_df["fighter"] != "Bruno Silva"]
fighters_df = fighters_df[
    (~fighters_df["dob"].isna())
    & (fighters_df["dob"].str.len() > 3)
    & (~fighters_df["height"].isna())
    & (~fighters_df["reach"].isna())
]
fighters_df["dob"] = pd.to_datetime(fighters_df["dob"], errors="coerce")
fighters_df["weight"] = fighters_df["weight"].apply(weight_to_num).astype(float)
fighters_df["height"] = fighters_df["height"].apply(height_to_inches).astype(float)
fighters_df["reach"] = fighters_df["reach"].apply(reach_to_inches).astype(float)
fighters_df = fighters_df.dropna(subset=["height", "weight", "reach", "dob"])
fighters_df = fighters_df.drop(columns=["stance", "url"])
fighters_df = fighters_df.drop_duplicates(subset=["fighter", "dob"])
fighters_df["fighter"] = (
    fighters_df["fighter"].str.replace(r"\s+", " ", regex=True).str.strip().dropna()
)

fight_stats_df = pd.read_csv("./ufc_fight_stats.csv")


def round_to_int(round):
    if pd.isnull(round):
        return None
    return int(round.split(" ")[1])


def get_stat_part(key, stat):
    if not isinstance(stat, str):
        return None
    if pd.isnull(stat):
        return None
    splitted = stat.split(" of ")
    if len(splitted) == 2:
        return splitted[key]
    return None


def get_left_stat(stat):
    return get_stat_part(0, stat)


def get_right_stat(stat):
    return get_stat_part(1, stat)


fight_stats_df["ROUND"] = fight_stats_df["ROUND"].apply(round_to_int)
fight_stats_df["ROUND"] = fight_stats_df["ROUND"].replace([np.inf, -np.inf], np.nan)
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
        fight_stats_df[before].apply(get_left_stat).astype(float)
    )
    fight_stats_df[after + "LANDED"] = fight_stats_df[after + "LANDED"].astype(float)
    fight_stats_df[after + "ATTEMPTED"] = fight_stats_df[before].apply(get_right_stat)
    fight_stats_df[after + "ATTEMPTED"] = fight_stats_df[after + "ATTEMPTED"].astype(
        float
    )
    fight_stats_df.drop(columns=[before], inplace=True)

fight_stats_df["CTRL"] = (
    fight_stats_df["CTRL"].apply(time_to_seconds).dropna().astype(float)
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

column_mapping = {}
for col in fight_stats_df.columns:
    column_mapping[col] = col.lower()

column_mapping["REV."] = "reversals"
column_mapping["CTRL"] = "control_time"
column_mapping["SUB.ATT"] = "sub_attempted"
column_mapping["KD"] = "knockdowns"

fight_stats_df = fight_stats_df.rename(
    mapper=column_mapping, errors="raise", axis=1
).reset_index(drop=True)


def get_fight_data(fighter, event, bout):
    fight_results_row = fight_results_df[
        ["fighter_a", "fighter_b", "date", "outcome", "method"]
    ][
        (
            (fight_results_df["fighter_a"] == fighter)
            | (fight_results_df["fighter_b"] == fighter)
        )
        & (fight_results_df["event"] == event)
        & (fight_results_df["bout"] == bout)
    ]
    if fight_results_row.empty:
        return [None, None, None, None]
    outcome = fight_results_row["outcome"].values[0]
    method = fight_results_row["method"].values[0]
    if fight_results_row["fighter_a"].values[0] == fighter:
        return [
            fight_results_row["fighter_b"].values[0],
            fight_results_row["date"].values[0],
            outcome,
            method,
        ]
    return [
        fight_results_row["fighter_a"].values[0],
        fight_results_row["date"].values[0],
        1 if outcome == 0 else 0,
        method,
    ]


def apply_fight_data(row):
    row["opponent"], row["date"], row["outcome"], row["method"] = get_fight_data(
        row["fighter"], row["event"], row["bout"]
    )
    return row


fight_stats_df = fight_stats_df.apply(apply_fight_data, axis=1).dropna()

missing_fight_details = 0
missing_fighter_results = 0
missing_opp_results = 0
missing_fighter_stats = 0
missing_opp_stats = 0

# fighter_stats_df = pd.read_csv('fighter_stats.csv')


def get_fighter_stats(fighter, event, bout):
    global missing_fight_details
    global missing_fighter_results
    global missing_opp_results
    global missing_fighter_stats
    global missing_opp_stats
    fighter_results = fight_stats_df[
        (fight_stats_df["fighter"] == fighter)
        & (fight_stats_df["event"] == event)
        & (fight_stats_df["bout"] == bout)
    ]
    if fighter_results.shape[0] == 0:
        missing_fighter_results += 1
        return None
    fight_date = fighter_results["date"].head(1).item()
    fighter_results = fighter_results.drop(
        columns=["fighter", "event", "bout", "round"]
    )
    fighter_stats = fighters_df[["weight", "height", "reach", "dob"]][
        fighters_df["fighter"] == fighter
    ].head(1)
    if fighter_stats.shape[0] == 0:
        missing_fighter_stats += 1
        return None
    opponent_name = fighter_results["opponent"].head(1).item()
    opp_results = fight_stats_df[
        (fight_stats_df["fighter"] == opponent_name)
        & (fight_stats_df["event"] == event)
        & (fight_stats_df["bout"] == bout)
    ]
    opp_results = opp_results.drop(columns=["fighter", "event", "bout", "round"])
    if opp_results.shape[0] == 0:
        missing_opp_results += 1
        return None
    opp_stats = fighters_df[["weight", "height", "reach", "dob"]][
        fighters_df["fighter"] == opponent_name
    ].head(1)
    if opp_stats.shape[0] == 0:
        missing_opp_stats += 1
        return None
    age = fight_date - fighter_stats["dob"].item()
    age = age.days / 365.25
    opponent_age = fight_date - opp_stats["dob"].item()
    opponent_age = opponent_age.days / 365.25
    returner = {
        "date": fight_date,
        "event": event,
        "bout": bout,
        "fighter": fighter,
        "weight": float(fighter_stats["weight"].astype(float).values[0]),
        "height": float(fighter_stats["height"].astype(float).values[0]),
        "reach": float(fighter_stats["reach"].astype(float).values[0]),
        "age": float(age),
        "outcome": float(fighter_results["outcome"].astype(float).values[0]),
        "method": fighter_results["method"].values[0],
        "opponent": opponent_name,
    }
    for col in filter(
        lambda x: x.endswith("landed")
        or x.endswith("attempted")
        or x in ["knockdowns", "reversals", "control_time"],
        fighter_results.columns,
    ):
        returner[col] = float(fighter_results[col].sum())
    for col in filter(
        lambda x: x.endswith("landed")
        or x.endswith("attempted")
        or x in ["knockdowns", "reversals", "control_time"],
        opp_results.columns,
    ):
        returner["opponent_" + col] = float(opp_results[col].sum())
    returner["opponent_weight"] = float(opp_stats["weight"].astype(float).values[0])
    returner["opponent_height"] = float(opp_stats["height"].astype(float).values[0])
    returner["opponent_reach"] = float(opp_stats["reach"].astype(float).values[0])
    returner["opponent_age"] = float(
        (fight_date - opp_stats["dob"].item()).days / 365.25
    )
    for key in [
        "height",
        "weight",
        "reach",
        "age",
        "control_time",
        "reversals",
        "knockdowns",
    ]:
        numerator = float(returner[key] - returner["opponent_" + key])
        denominator = float(returner[key] + returner["opponent_" + key])
        returner[key + "_diff"] = (numerator if denominator != 0 else 0) / (
            denominator if denominator != 0 else 1
        )
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
        landed_numerator = float(
            returner[key + "_landed"] - returner["opponent_" + key + "_landed"]
        )
        landed_denominator = float(
            returner[key + "_landed"] + returner["opponent_" + key + "_landed"]
        )
        returner[key + "_landed_diff"] = (
            landed_numerator if landed_denominator != 0 else 0
        ) / (landed_denominator if landed_denominator != 0 else 1)
        attempted_numerator = float(
            returner[key + "_attempted"] - returner["opponent_" + key + "_attempted"]
        )
        attempted_denominator = float(
            returner[key + "_attempted"] + returner["opponent_" + key + "_attempted"]
        )
        returner[key + "_attempted_diff"] = (
            attempted_numerator if attempted_denominator != 0 else 0
        ) / (attempted_denominator if attempted_denominator != 0 else 1)
        absorbed = float(returner["opponent_" + key + "_landed"])
        defended = float(returner["opponent_" + key + "_attempted"]) - absorbed
        if defended < 0:
            defended = 0
        returner[key + "_absorbed"] = absorbed
        returner[key + "_defended"] = defended
        absorbed_numerator = float(absorbed - defended)
        absorbed_denominator = float(absorbed + defended)
        returner[key + "_absorbed_diff"] = (
            absorbed_numerator if absorbed_denominator != 0 else 0
        ) / (absorbed_denominator if absorbed_denominator != 0 else 1)
        returner[key + "_defended_diff"] = (defended if defended != 0 else 0) / (
            absorbed + defended if absorbed + defended != 0 else 1
        )
    knockdowns_absorbed = float(returner["opponent_knockdowns"])
    knockdowns_numerator = float(returner["knockdowns"] - knockdowns_absorbed)
    knockdowns_denominator = float(returner["knockdowns"] + knockdowns_absorbed)
    returner["knockdowns_diff"] = (
        knockdowns_numerator if knockdowns_denominator != 0 else 0
    ) / (knockdowns_denominator if knockdowns_denominator != 0 else 1)
    return returner


def apply_get_fighter_stats(row):
    return get_fighter_stats(row["fighter"], row["event"], row["bout"])


unique_fights = fight_stats_df[["fighter", "event", "bout"]].drop_duplicates()
unique_fights["fighter"] = unique_fights["fighter"].str.strip().dropna()
unique_fights["event"] = unique_fights["event"].str.strip().dropna()
unique_fights["bout"] = unique_fights["bout"].str.strip().dropna()

fighter_stats_df = (
    unique_fights.apply(apply_get_fighter_stats, axis=1).apply(pd.Series).dropna()
)


def fighter_history_by_date(fighter, date):
    full_history = fighter_stats_df[
        (fighter_stats_df["fighter"] == fighter) & (fighter_stats_df["date"] < date)
    ]
    recent_history = full_history[full_history["date"] >= date - pd.Timedelta(days=730)]
    returner = {}
    for key in ["weight", "height", "reach", "age"]:
        returner["avg_" + key + "_diff"] = (
            full_history[key + "_diff"].mean() if full_history.shape[0] > 0 else 0.0
        )
        returner["recent_avg_" + key + "_diff"] = (
            recent_history[key + "_diff"].mean() if recent_history.shape[0] > 0 else 0.0
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
        returner[avgK] = full_history[key].mean() if full_history.shape[0] > 0 else 0.0
        returner[recentAvgK] = (
            recent_history[key].mean() if recent_history.shape[0] > 0 else 0.0
        )
        if "absorbed" in key:
            returner[peakK] = (
                full_history[key].min() if full_history.shape[0] > 0 else 0.0
            )
            returner[valleyK] = (
                full_history[key].max() if full_history.shape[0] > 0 else 0.0
            )
        else:
            returner[peakK] = (
                full_history[key].max() if full_history.shape[0] > 0 else 0.0
            )
            returner[valleyK] = (
                full_history[key].min() if full_history.shape[0] > 0 else 0.0
            )
        returner[recentAvgK + "_vs_peak"] = (
            returner[recentAvgK] / returner[peakK] if returner[peakK] != 0 else 0.0
        )
        returner[recentAvgK + "_vs_valley"] = (
            returner[recentAvgK] / returner[valleyK] if returner[valleyK] != 0 else 0.0
        )
        returner[avgK + "_vs_peak"] = (
            returner[avgK] / returner[peakK] if returner[peakK] != 0 else 0.0
        )
        returner[avgK + "_vs_valley"] = (
            returner[avgK] / returner[valleyK] if returner[valleyK] != 0 else 0.0
        )
    returner["recent_wins"] = recent_history[recent_history["outcome"] == 1].shape[0]
    returner["recent_losses"] = recent_history[recent_history["outcome"] == 0].shape[0]
    returner["wins"] = float(full_history[full_history["outcome"] == 1].shape[0])
    returner["losses"] = float(full_history[full_history["outcome"] == 0].shape[0])
    returner["win_ratio"] = (
        returner["wins"] / (returner["wins"] + returner["losses"])
        if (returner["wins"] + returner["losses"]) != 0
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
            != 0
            else 0.0
        )
        returner[transformed + "_loss_ratio"] = (
            returner[transformed + "_losses"]
            / (returner[transformed + "_losses"] + returner[transformed + "_wins"])
            if (returner[transformed + "_losses"] + returner[transformed + "_wins"])
            != 0
            else 0.0
        )
        returner["recent_" + transformed + "_wins"] = recent_history[
            (recent_history["method"] == method) & (recent_history["outcome"] == 1)
        ]["outcome"].shape[0]
        returner["recent_" + transformed + "_losses"] = recent_history[
            (recent_history["method"] == method) & (recent_history["outcome"] == 0)
        ]["outcome"].shape[0]
    return returner


def apply_fighter_history(row):
    row = row.to_dict()
    history_data = fighter_history_by_date(row["fighter"], row["date"])
    for key in history_data:
        row["precomp_" + key] = history_data[key]
    return row


fight_stats_with_history_df = (
    fighter_stats_df.apply(apply_fighter_history, axis=1).apply(pd.Series).dropna()
)


def get_history_diffs(fighter, opponent, date):
    returner = {}
    fighter_history = fighter_history_by_date(fighter, date)
    for key in fighter_history:
        returner["precomp_" + key] = fighter_history[key]
    opponent_history = fighter_history_by_date(opponent, date)
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


def apply_history_diffs(row):
    row = row.to_dict()
    history_diff = get_history_diffs(row["fighter"], row["opponent"], row["date"])
    for k in history_diff:
        row[k] = history_diff[k]
    return row


fight_stats_with_history_diffs_df = (
    fight_stats_with_history_df.apply(apply_history_diffs, axis=1)
    .apply(pd.Series)
    .dropna()
)

fight_stats_with_history_diffs_df.to_csv("fighter_stats.csv", index=False)
