from postgres import Postgres
import pandas as pd
import matplotlib.pyplot as plt

pg = Postgres()

_q = """
select
  property,
  modifier,
  type,
  value,
  (select weight_class from fighters where id = fighter_id) as weight_class
from
  fight_stats
"""

stats = pg.query(_q)

df = pd.DataFrame(
    stats, columns=["property", "modifier", "type", "value", "weight_class"]
)

for weight_class in df["weight_class"].unique():
    for property in df["property"].unique():
        for modifier in df["modifier"].unique():
            subset = df[
                (df["weight_class"] == weight_class)
                & (df["property"] == property)
                & (df["modifier"] == modifier)
                & (df["type"] == "total")
            ]
            if subset.empty:
                continue
            fig = subset.plot.hist(bins=12, alpha=0.5).get_figure()
            fig.savefig("./charts/%s/%s/%s.png" % (property, modifier, weight_class))
            plt.close(fig)
        unmodified = df[
            (df["weight_class"] == weight_class)
            & (df["property"] == property)
            & (df["modifier"].isnull())
            & (df["type"] == "total")
        ]
        if unmodified.empty:
            continue
        fig = unmodified.plot.hist(bins=12, alpha=0.5).get_figure()
        fig.savefig("./charts/%s/%s.png" % (property, weight_class))
        plt.close(fig)

_q = """
select
  property,
  type,
  value,
  (select weight_class from fighters where id = fighter_id) as weight_class
from
  fighter_stats
where property in ('height', 'reach')
"""

stats = pg.query(_q)

df = pd.DataFrame(stats, columns=["property", "type", "value", "weight_class"])

for weight_class in df["weight_class"].unique():
    for property in df["property"].unique():
        subset = df[
            (df["weight_class"] == weight_class)
            & (df["property"] == property)
            & (df["type"] == "total")
        ]
        if subset.empty:
            continue
        fig = subset.plot.hist(bins=12, alpha=0.5).get_figure()
        fig.savefig("./charts/%s/%s.png" % (property, weight_class))
        plt.close(fig)
