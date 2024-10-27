# MMA-ML
Hacking around with machine learning for predicting the outcomes of UFC fights.

I'm not a python dev, so none of this is guaranteed to work, and everything is very sloppy and disorganized for now.

## Setup

### Prerequisites

- [Anaconda](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html). All other python stuff is within the conda env.
- Postgres 16 however you want it. I recommend running it in a container. There is a [compose](./compose.yml) file for running PG + adminer in containers.

### Steps

Create the conda environment.
```sh
conda env create -n <YOUR ENVIRONMENT NAME> -f conda-env.yml
```

Activate it. Now you have an isolated python environment with all the pip dependencies installed.
```sh
conda activate <YOUR ENVIRONMENT NAME>
```

Copy the env example, then modify its values to be accurate for your environment.
```sh
cp .env.example .env
```

Execute [schema.sql](./schema.sql) on your postgres database. That will set up the database tables for you. If you have `psql` on the commandline, then just run `psql <your db name> < schema.sql`.

Now the project is set up.

## Usage

Scrape the [UFC stats website](https://ufcstats.com) into a bunch of nasty CSV files:
```sh
python ./scrape_ufc_stats_unparsed_data.py
```

Parse those CSV files into one mega CSV:
```sh
python ./generate_fighter_stats.py
```

Train the model:
```sh
python ./train_model.py
```

At this point, there isn't a very slick way to run a prediction, so you'll have to use [library.py](./library.py) to run predictions.
There is an example of using it to predict a card _in_ the file.
