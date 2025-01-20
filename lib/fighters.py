from datetime import datetime
import pandas as pd
from postgres import Postgres
import lib.sql as sql


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

    def get_fighter_id(self, name: str) -> int:
        return self._pg.one(sql.get("fighter.id.by.name"), (name,))

    def get_precomp_stat_recent_average(
        self,
        fighter_id,
        property,
        modifier,
        type,
        before: str = datetime.today().isoformat(),
    ) -> float:
        value = self._pg.one(
            sql.get("prefight.recent.average.stat"),
            (
                fighter_id,
                property,
                modifier,
                type,
                before,
                before,
            ),
        )
        return value if value is not None else 0.0

    def get_precomp_stat_average(
        self,
        fighter_id,
        property,
        modifier,
        type,
        before: str = datetime.today().isoformat(),
    ) -> float:
        value = self._pg.one(
            sql.get("prefight.average.stat"),
            (
                fighter_id,
                property,
                modifier,
                type,
                before,
            ),
        )
        return value if value is not None else 0.0

    def get_fights_by_fighter(
        self, fighter_id, before: str = datetime.today().isoformat()
    ) -> pd.DataFrame:
        returner = pd.DataFrame(
            self._pg.query(
                sql.get("fights.by.fighter"),
                (
                    before,
                    fighter_id,
                    fighter_id,
                ),
            ),
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
        returner["date"] = pd.to_datetime(returner["date"])
        return returner

    def get_fighter_history(
        self, id: int, before: str = datetime.today().isoformat()
    ) -> pd.DataFrame:
        def calculate_differential(fighter_value, opponent_value):
            numerator = fighter_value - opponent_value
            denominator = fighter_value + opponent_value
            return numerator / denominator if denominator != 0 else 0

        out_columns = [
            "prefix",
            "property",
            "modifier",
            "type",
            "value",
        ]
        columns = ["date", "fight_id"] + out_columns
        fighter_fight_stats_rows = self._pg.query(
            sql.get("fight.stats.before.date"),
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
        fighter_fights = self.get_fights_by_fighter(id, before)
        grouped_df = stats_df.groupby("fight_id")
        data = []
        for fight_id, group in grouped_df:
            precomp_total_duration = fighter_fights[("date" < group["date"].values[0])][
                "duration"
            ].sum()
            opponent_fights_df = self.get_fights_by_fighter(
                opponent["fighter_id"].values[0], before
            )
            opponent_precomp_total_duration = opponent_fights_df[
                ("date" < group["date"].values[0])
            ]["duration"].sum()
            group = pd.concat(
                [
                    group,
                    pd.DataFrame(
                        [
                            (
                                None,
                                "precomp_total_duration",
                                None,
                                "total",
                                precomp_total_duration,
                            ),
                            (
                                None,
                                "opponent_precomp_total_duration",
                                None,
                                "total",
                                opponent_precomp_total_duration,
                            ),
                        ],
                        columns=columns,
                    ),
                ]
            )
            fighter_df = group[group["prefix"].isnull()]
            opponent = group[group["prefix"] == "opponent"]
            if opponent.empty:
                continue
            # differential
            for property in fighter_df["property"].unique():
                if fighter_df[(fighter_df["property"] == property)].empty:
                    continue
                if not fighter_df[
                    (fighter_df["property"] == property)
                    & (fighter_df["modifier"].isnull())
                ].empty:
                    fighter_value = float(
                        fighter_df[
                            (fighter_df["property"] == property)
                            & (fighter_df["modifier"].isnull())
                        ]["value"].values[0]
                    )
                    opponent_value = opponent[
                        (opponent["property"] == property)
                        & (opponent["modifier"].isnull())
                    ]
                    if opponent_value.empty:
                        continue
                    differential = calculate_differential(
                        fighter_value, float(opponent_value["value"].values[0])
                    )
                    data += [
                        (
                            group["date"].values[0],
                            fight_id,
                            None,
                            property,
                            None,
                            "differential",
                            differential,
                        )
                    ]
                    data += [
                        (
                            group["date"].values[0],
                            fight_id,
                            None,
                            property,
                            None,
                            "total",
                            fighter_value,
                        )
                    ]
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
                    differential = calculate_differential(
                        fighter_value, float(opponent_value["value"].values[0])
                    )
                    data += [
                        (
                            group["date"].values[0],
                            fight_id,
                            None,
                            property,
                            modifier,
                            "differential",
                            differential,
                        ),
                        (
                            group["date"].values[0],
                            fight_id,
                            None,
                            property,
                            modifier,
                            "total",
                            fighter_value,
                        ),
                    ]
                    if modifier == "landed":
                        data += [
                            (
                                group["date"].values[0],
                                fight_id,
                                None,
                                property,
                                "absorbed",
                                "total",
                                opponent_value,
                            ),
                            (
                                group["date"].values[0],
                                fight_id,
                                "opponent",
                                property,
                                "absorbed",
                                "total",
                                fighter_value,
                            ),
                            (
                                group["date"].values[0],
                                fight_id,
                                "opponent",
                                property,
                                "absorbed",
                                "differential",
                                differential,
                            ),
                            (
                                group["date"].values[0],
                                fight_id,
                                None,
                                property,
                                "absorbed",
                                "differential",
                                differential * -1,
                            ),
                        ]

        data = pd.DataFrame(data, columns=columns)
        self_stats_df = data[data["prefix"].isnull()]
        start_date = datetime.fromisoformat(before)
        recent_date = datetime(start_date.year - 1, start_date.month, start_date.day)
        recent_stats_df = data[data["date"] >= recent_date]
        recent_self_stats_df = recent_stats_df[recent_stats_df["prefix"].isnull()]
        returner = []
        for property in self_stats_df["property"].unique():
            for t in self_stats_df["type"].unique():
                d = self_stats_df[
                    self_stats_df["modifier"].isnull()
                    & (self_stats_df["property"] == property)
                    & (self_stats_df["type"] == t)
                ]
                if not d.empty:
                    v = d["value"]
                    peak = v.max()
                    valley = v.min()
                    avg = v.mean()
                    recent_peak = None
                    recent_valley = None
                    recent_avg = None
                    rd = recent_self_stats_df[
                        (recent_self_stats_df["modifier"].isnull())
                        & (recent_self_stats_df["property"] == property)
                        & (recent_self_stats_df["type"] == t)
                    ]
                    if not rd.empty:
                        rv = rd["value"]
                        recent_peak = rv.max()
                        recent_valley = rv.min()
                        recent_avg = rv.mean()
                    returner += [
                        (
                            None,
                            property,
                            None,
                            "peak" if t == "total" else "differential_peak",
                            float(peak) if peak is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            None,
                            (
                                "recent_peak"
                                if t == "total"
                                else "differential_recent_peak"
                            ),
                            float(recent_peak) if recent_peak is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            None,
                            "valley" if t == "total" else "differential_valley",
                            float(valley) if valley is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            None,
                            (
                                "recent_valley"
                                if t == "total"
                                else "differential_recent_valley"
                            ),
                            float(recent_valley) if recent_valley is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            None,
                            "average" if t == "total" else "differential_average",
                            float(avg) if avg is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            None,
                            (
                                "recent_average"
                                if t == "total"
                                else "differential_recent_average"
                            ),
                            float(recent_avg) if recent_avg is not None else 0.0,
                        ),
                    ]
                for modifier in self_stats_df["modifier"].unique():
                    d = self_stats_df[
                        (self_stats_df["property"] == property)
                        & (self_stats_df["modifier"] == modifier)
                        & (self_stats_df["type"] == t)
                    ]
                    if d.empty:
                        print("with modifier is empty for", property, modifier, t)
                        continue
                    v = d["value"]
                    peak = v.max()
                    valley = v.min()
                    avg = v.mean()
                    recent_peak = None
                    recent_valley = None
                    recent_avg = None
                    rd = recent_self_stats_df[
                        (recent_self_stats_df["property"] == property)
                        & (recent_self_stats_df["modifier"] == modifier)
                        & (recent_self_stats_df["type"] == t)
                    ]
                    if not rd.empty:
                        rv = rd["value"]
                        recent_peak = rv.max()
                        recent_valley = rv.min()
                        recent_avg = rv.mean()
                    returner += [
                        (
                            None,
                            property,
                            modifier,
                            "peak" if t == "total" else "differential_peak",
                            float(peak) if peak is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            modifier,
                            (
                                "recent_peak"
                                if t == "total"
                                else "differential_recent_peak"
                            ),
                            float(recent_peak) if recent_peak is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            modifier,
                            "valley" if t == "total" else "differential_valley",
                            float(valley) if valley is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            modifier,
                            (
                                "recent_valley"
                                if t == "total"
                                else "differential_recent_valley"
                            ),
                            float(recent_valley) if recent_valley is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            modifier,
                            "average" if t == "total" else "differential_average",
                            float(avg) if avg is not None else 0.0,
                        ),
                        (
                            None,
                            property,
                            modifier,
                            (
                                "recent_average"
                                if t == "total"
                                else "differential_recent_average"
                            ),
                            float(recent_avg) if recent_avg is not None else 0.0,
                        ),
                    ]
        synthesized_df = pd.DataFrame(returner, columns=out_columns)
        return synthesized_df

    def get_by_name(self, name: str) -> pd.DataFrame:
        columns = [
            "row_id",
            "name",
            "date_of_birth",
            "weight_class",
        ]
        returner = pd.DataFrame(
            [],
            columns=columns,
        )
        row = self._pg.row(sql.get("fighter.by.name"), (name,))
        if not row or len(row) == 0:
            return returner
        (row_id, name, date_of_birth, weight_class) = row
        date_of_birth = pd.to_datetime(date_of_birth)
        fighter_stats_rows = self._pg.query(sql.get("fighter.stats.by.id"), (row_id,))
        if not fighter_stats_rows or len(fighter_stats_rows) == 0:
            returner = pd.DataFrame(
                [row_id, name, date_of_birth, weight_class],
                columns=columns,
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
        return pd.DataFrame(
            [data],
            columns=columns,
        )

    def get_by_id(self, id: int) -> pd.DataFrame:
        columns = [
            "row_id",
            "name",
            "date_of_birth",
            "weight_class",
        ]
        returner = pd.DataFrame(
            [],
            columns=columns,
        )
        row = self._pg.row(sql.get("fighter.by.id"), (id,))
        if not row or len(row) == 0:
            return returner
        (row_id, name, date_of_birth, weight_class) = row
        date_of_birth = pd.to_datetime(date_of_birth)
        fighter_stats_rows = self._pg.query(sql.get("fighter.stats.by.id"), (row_id,))
        if not fighter_stats_rows or len(fighter_stats_rows) == 0:
            returner = pd.DataFrame(
                [row_id, name, date_of_birth, weight_class],
                columns=columns,
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
        return pd.DataFrame(
            [data],
            columns=columns,
        )

    def all(self) -> pd.DataFrame:
        columns = [
            "row_id",
            "name",
            "date_of_birth",
            "weight_class",
        ]
        returner = pd.DataFrame(
            [],
            columns=columns,
        )
        rows = self._pg.query(sql.get("fighters.all"))
        if not rows:
            return returner
        rows = pd.DataFrame(
            rows,
            columns=columns,
        )
        rows["date_of_birth"] = pd.to_datetime(rows["date_of_birth"])
        returner = []
        for row in rows.iterrows():
            returner.append(self.get_by_id(row[0]))
        returner = pd.concat(returner)
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
                (~self._fighters_df["dob"].isnull())
                & (self._fighters_df["dob"].str.len() > 3)
                & (~self._fighters_df["height"].isnull())
                & (~self._fighters_df["reach"].isnull())
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
