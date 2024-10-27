import os
import pandas as pd

from postgres import Postgres
from fighters import Fighters

from fights import Fights

pg = Postgres()
print("Connected to Postgres")
fighters = Fighters(True)
print("Got fighters")

fighter = fighters.get_by_name("Israel Adesanya")
id = int(fighter["row_id"].values[0])
print(fighter)

history = fighters.get_fighter_history(id)
print(history)

# fights_df = Fights(True).all()
# print("Got fights")

# for col in fights_df.columns:
#     print(col)
# print(fights_df.describe())
# print(fights_df.head())


# print("Fights seeded")
# print(fights_df.head())

# for col in fights_df.columns:
#     print(col)

# TODO: Scale every fighter's stats to a 0-1 scale
# Scale all the stats in a fight to a 0-1 scale
# How to scale? (x - min) / (max - min)
# Should each fighter be scaled individually or should all fighters be scaled together?
# If scaled individually, how to scale the test data?
# If scaled together, how to scale the test data?
