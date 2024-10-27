from datetime import date
import pandas as pd
from postgres import Postgres
from fights import Fights


class Fighters:
    weights = [
        (125, "flyweight"),
        (135, "bantamweight"),
        (145, "featherweight"),
        (155, "lightweight"),
        (170, "welterweight"),
        (185, "middleweight"),
        (205, "light_heavyweight"),
        # Heavyweight is just > 206
    ]

    properties = ["height", "weight", "reach"]
    types = ["total", "zscore"]

    _query = {
        "all": """
SELECT
  f.id as row_id,
  f.name,
  f.date_of_birth,
  f.weight_class
FROM
  fighters f
ORDER BY
  f.name;
""",
        "one": {
            "by": {
                "name": """
SELECT
  f.id as row_id,
  f.name,
  f.date_of_birth,
  f.weight_class
FROM
  fighters f
WHERE
  f.name = %s
ORDER BY
  f.id
LIMIT
  1;
""",
                "id": """
SELECT
  f.id as row_id,
  f.name,
  f.date_of_birth,
  f.weight_class
FROM
  fighters f
WHERE
  f.id = %s
LIMIT
  1;
""",
            }
        },
        "fight": {
            "stats": {
                "before": {
                    "date": """
SELECT
  f.date,
  fs.fight_id,
  case when fs.fighter_id = %s then 'opponent' else NULL end as "prefix",
  fs.property,
  fs.modifier,
  fs.type,
  fs.value
FROM
  fights f
JOIN
  fight_stats fs ON f.id = fs.fight_id
WHERE
  f.date < %s
  AND f.id IN (SELECT id FROM fights WHERE fighter_id = %s OR opponent_id = %s)
ORDER BY
  f.date DESC,
  fs.id DESC
"""
                },
            },
        },
        "fighter": {
            "stats": {
                "by": {
                    "id": """
SELECT
  property,
  type,
  value
FROM
  fighter_stats
WHERE
  fighter_id = %s
ORDER BY
  id DESC;
"""
                }
            }
        },
    }

    @staticmethod
    def height_to_inches(height: str) -> int | None:
        if height == "--":
            return None
        feet, inches = height.split("' ")
        inches = inches.replace('"', "")
        return int(feet) * 12 + int(inches)

    @staticmethod
    def reach_to_inches(reach: str) -> int | None:
        if reach == "--":
            return None
        return int(reach.replace('"', ""))

    @staticmethod
    def weight_to_num(weight: str) -> int | None:
        if not " lbs." in weight:
            return None
        return int(weight.replace(" lbs.", ""))

    @staticmethod
    def weight_to_class(weight: int) -> str | None:
        if weight > 205:
            return "heavyweight"
        for w, class_name in Fighters.weights:
            if weight <= w:
                return class_name
        return None

    def get_fighter_history(
        self, id: int, before: str = date.today().isoformat()
    ) -> pd.DataFrame:
        columns = [
            "prefix",
            "property",
            "modifier",
            "type",
            "value",
        ]
        fighter_fight_stats_rows = self._pg.query(
            Fighters._query.get("fight").get("stats").get("before").get("date"),
            (
                id,
                before,
                id,
                id,
            ),
        )
        if not fighter_fight_stats_rows or len(fighter_fight_stats_rows) == 0:
            return pd.DataFrame([], columns=columns)
        stats_df = pd.DataFrame(fighter_fight_stats_rows, columns=columns)
        stats_df["date"] = pd.to_datetime(stats_df["date"])
        self_stats_df = stats_df[stats_df["prefix"].isnull()]
        start_date = date.fromisoformat(before)
        recent_date = date(start_date.year - 1, start_date.month, start_date.day)
        recent_stats_df = stats_df[stats_df["date"] >= recent_date]
        recent_self_stats_df = recent_stats_df[recent_stats_df["prefix"].isnull()]
        grouped_df = stats_df.groupby("fight_id")
        data = []
        for fight_id, group in grouped_df:
            fighter_df = group[group["prefix"].isnull()]
            print("\n\nfighter\n", fighter_df)
            opponent = group[group["prefix"] == "opponent"]
            print("\n\nopponent\n", opponent)
            if opponent.empty:
                continue
            # differential
            for property in fighter_df["property"].unique():
                print("property", property)
                for modifier in fighter_df["modifier"].unique():
                    fighter_value = fighter_df[
                        (fighter_df["property"] == property)
                        & (fighter_df["modifier"] == modifier)
                    ]
                    if fighter_value.empty:
                        continue
                    fighter_value = float(fighter_value["value"].values[0])
                    opponent_value = opponent[
                        (opponent["property"] == property)
                        & (opponent["modifier"] == modifier)
                    ]
                    if opponent_value.empty:
                        continue
                    opponent_value = float(opponent_value["value"].values[0])
                    numerator = fighter_value - opponent_value
                    denominator = fighter_value + opponent_value
                    # differential is between -1 and 1
                    differential = numerator / denominator if denominator != 0 else 0
                    data += [
                        (
                            group["date"].values[0],
                            fight_id,
                            property,
                            modifier,
                            "differential",
                            differential,
                        )
                    ]
                    data += [
                        (
                            group["date"].values[0],
                            fight_id,
                            property,
                            modifier,
                            "total",
                            fighter_value,
                        )
                    ]
        returner = []
        for property in self_stats_df["property"].unique():
            for modifier in self_stats_df["modifier"].unique():
                peak = self_stats_df[
                    self_stats_df["property"]
                    == property & self_stats_df["modifier"]
                    == modifier & self_stats_df["type"]
                    == "total"
                ]["value"].max()
                recent_peak = recent_self_stats_df[
                    recent_self_stats_df["property"]
                    == property & recent_self_stats_df["modifier"]
                    == modifier & recent_self_stats_df["type"]
                    == "total"
                ]["value"].max()
                valley = self_stats_df[
                    self_stats_df["property"]
                    == property & self_stats_df["modifier"]
                    == modifier & self_stats_df["type"]
                    == "total"
                ]["value"].min()
                recent_valley = recent_self_stats_df[
                    recent_self_stats_df["property"]
                    == property & recent_self_stats_df["modifier"]
                    == modifier & recent_self_stats_df["type"]
                    == "total"
                ]["value"].min()
                avg = self_stats_df[
                    self_stats_df["property"]
                    == property & self_stats_df["modifier"]
                    == modifier & self_stats_df["type"]
                    == "total"
                ]["value"].avg()
                recent_avg = recent_self_stats_df[
                    recent_self_stats_df["property"]
                    == property & recent_self_stats_df["modifier"]
                    == modifier & recent_self_stats_df["type"]
                    == "total"
                ]["value"].avg()
                data += [
                    (
                        None,
                        property,
                        modifier,
                        "peak",
                        peak if peak is not None else 0,
                    ),
                    (
                        None,
                        property,
                        modifier,
                        "recent_peak",
                        recent_peak if recent_peak is not None else 0,
                    ),
                    (
                        None,
                        property,
                        modifier,
                        "valley",
                        valley if valley is not None else 0,
                    ),
                    (
                        None,
                        property,
                        modifier,
                        "recent_valley",
                        recent_valley if recent_valley is not None else 0,
                    ),
                    (
                        None,
                        property,
                        modifier,
                        "average",
                        avg if avg is not None else 0,
                    ),
                    (
                        None,
                        property,
                        modifier,
                        "recent_average",
                        recent_avg if recent_avg is not None else 0,
                    ),
                ]
        synthesized_df = pd.DataFrame(data, columns=columns)
        return synthesized_df

    def get_by_name(self, name: str) -> pd.DataFrame:
        columns = [
            "row_id",
            "name",
            "date_of_birth",
            "weight_class",
        ]
        returner = pd.DataFrame([], columns=columns)
        row = self._pg.row(Fighters._query.get("one").get("by").get("name"), (name,))
        if not row or len(row) == 0:
            return returner
        (row_id, name, date_of_birth, weight_class) = row
        date_of_birth = pd.to_datetime(date_of_birth)
        fighter_stats_rows = self._pg.query(
            Fighters._query.get("fighter").get("stats").get("by").get("id"), (row_id,)
        )
        if not fighter_stats_rows or len(fighter_stats_rows) == 0:
            returner = pd.DataFrame(
                [row_id, name, date_of_birth, weight_class], columns=columns
            )
            return returner
        data = (
            row_id,
            name,
            date_of_birth,
            weight_class,
        )
        for property, type, value in fighter_stats_rows:
            columns.append(f"{property}_{type}" if type != "total" else property)
            data = data + (value,)
        return pd.DataFrame([data], columns=columns)

    def get_by_id(self, id: int) -> pd.DataFrame:
        columns = [
            "row_id",
            "name",
            "date_of_birth",
            "weight_class",
        ]
        returner = pd.DataFrame([], columns=columns)
        row = self._pg.row(Fighters._query.get("one").get("by").get("id"), (id,))
        if not row or len(row) == 0:
            return returner
        (row_id, name, date_of_birth, weight_class) = row
        date_of_birth = pd.to_datetime(date_of_birth)
        fighter_stats_rows = self._pg.query(
            Fighters._query.get("fighter").get("stats").get("by").get("id"), (row_id,)
        )
        if not fighter_stats_rows or len(fighter_stats_rows) == 0:
            returner = pd.DataFrame(
                [row_id, name, date_of_birth, weight_class], columns=columns
            )
            return returner
        data = (
            row_id,
            name,
            date_of_birth,
            weight_class,
        )
        for property, type, value in fighter_stats_rows:
            columns.append(f"{property}_{type}" if type != "total" else property)
            data = data + (value,)
        return pd.DataFrame([data], columns=columns)

    def all(self) -> pd.DataFrame:
        returner = pd.DataFrame()
        rows = self._pg.query(Fighters._query.get("all"))
        if not rows:
            return returner
        returner = pd.DataFrame(
            rows,
            columns=[
                "row_id",
                "name",
                "date_of_birth",
                "weight_class",
            ],
        )
        returner["date_of_birth"] = pd.to_datetime(returner["date_of_birth"])
        return returner

    def _create_fighter(self, row):
        returner = row
        returner["row_id"] = self._pg.insert(
            "INSERT INTO fighters (name, date_of_birth, weight_class) VALUES (%s, %s, %s) ON CONFLICT (name) DO UPDATE SET date_of_birth = EXCLUDED.date_of_birth, weight_class = EXCLUDED.weight_class RETURNING id",
            (returner["name"], returner["date_of_birth"], returner["weight_class"]),
        )
        for stat in ["height", "weight", "reach"]:
            self._pg.insert(
                "INSERT INTO fighter_stats (property, type, value, fighter_id) VALUES (%s, %s, %s, %s) ON CONFLICT (fighter_id, property, type) DO UPDATE SET value = EXCLUDED.value RETURNING id",
                (stat, "total", returner[stat], returner["row_id"]),
            )
        return returner

    def _create_fighter_zscores(self, row):
        returner = row
        if not returner["row_id"]:
            return returner
        subset = self._fighters_df[
            (self._fighters_df["weight_class"] == returner["weight_class"])
        ]
        for stat in ["height", "weight", "reach"]:
            stddev = subset[stat].std()
            if stddev == 0:
                continue
            mean = subset[stat].mean()
            zscore = (returner[stat] - mean) / stddev
            self._pg.insert(
                "INSERT INTO fighter_stats (property, type, value, fighter_id) VALUES (%s, %s, %s, %s) ON CONFLICT (fighter_id, property, type) DO UPDATE SET value = EXCLUDED.value RETURNING id",
                (stat, "zscore", zscore, returner["row_id"]),
            )

    def __init__(self, skip_create: bool = False):
        self._pg = Postgres()
        if not skip_create:
            self._fighters_df = pd.read_csv("./ufc_fighter_tott.csv")
            column_mapping = {}
            for col in self._fighters_df.columns:
                column_mapping[col] = col.lower()
            self._fighters_df = self._fighters_df.rename(
                mapper=column_mapping, errors="raise", axis=1
            )
            # Remove Bruno Silvas
            self._fighters_df = self._fighters_df[
                self._fighters_df["fighter"] != "Bruno Silva"
            ]
            self._fighters_df = self._fighters_df[
                (~self._fighters_df["dob"].isna())
                & (self._fighters_df["dob"].str.len() > 3)
                & (~self._fighters_df["height"].isna())
                & (~self._fighters_df["reach"].isna())
            ]
            self._fighters_df["date_of_birth"] = pd.to_datetime(
                self._fighters_df["dob"], errors="coerce"
            )
            self._fighters_df["weight"] = (
                self._fighters_df["weight"].apply(Fighters.weight_to_num).astype(float)
            )
            self._fighters_df["height"] = (
                self._fighters_df["height"]
                .apply(Fighters.height_to_inches)
                .astype(float)
            )
            self._fighters_df["reach"] = (
                self._fighters_df["reach"].apply(Fighters.reach_to_inches).astype(float)
            )
            self._fighters_df = self._fighters_df.dropna(
                subset=["height", "weight", "reach", "date_of_birth"]
            )
            self._fighters_df = self._fighters_df.drop_duplicates(subset=["fighter"])
            self._fighters_df["name"] = (
                self._fighters_df["fighter"]
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
                .dropna()
            )
            self._fighters_df = self._fighters_df.drop(
                columns=["fighter", "dob", "stance", "url"]
            )
            self._fighters_df["weight_class"] = self._fighters_df["weight"].apply(
                Fighters.weight_to_class
            )
            self._fighters_df = self._fighters_df.apply(
                self._create_fighter, axis=1
            ).apply(self._create_fighter_zscores, axis=1)
