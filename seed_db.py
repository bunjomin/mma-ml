import os
import pandas as pd

from lib.fighters import Fighters
from lib.fights import Fights

fighters = Fighters()
f = fighters.all()
print(f.head())
print("Got fighters")

fights = Fights()
print("Got fights")
