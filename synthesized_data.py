from postgres import Postgres
import pandas as pd

pg = Postgres()

(fighter_id, dt, method, duration, fighter_id, opponent_id, winner_id) = pg.row(
    "SELECT * FROM fights WHERE id = 1 LIMIT 1;"
)
fight = {
    "id": fighter_id,
    "date": dt,
    "method": method,
    "duration": duration,
    "fighter_id": fighter_id,
    "opponent_id": opponent_id,
    "winner_id": winner_id,
}

stats = pg.query(
    "SELECT property, modifier, type, value, fighter_id FROM fight_stats WHERE fight_id = 1;"
)
fighter_stats = pg.query(
    "SELECT property, type, value FROM fighter_stats WHERE fighter_id = %s;",
    (fighter_id,),
)
opponent_stats = pg.query(
    "SELECT property, type, value FROM fighter_stats WHERE fighter_id = %s;",
    (opponent_id,),
)
fighter = {
    "id": fighter_id,
    "name": pg.one("SELECT name FROM fighters WHERE id = %s;", (fighter_id,)),
    "stats": pd.DataFrame(fighter_stats, columns=["property", "type", "value"]),
}
opponent = {
    "id": opponent_id,
    "name": pg.one("SELECT name FROM fighters WHERE id = %s;", (opponent_id,)),
    "stats": pd.DataFrame(fighter_stats, columns=["property", "type", "value"]),
}

for property, modifier, type, value, fighter_id in stats:
    print(
        f"{fighter["name"] if fighter_id == fighter_id else opponent["name"]} {property} {modifier if modifier is not None else ""} {type} {value}"
    )
