CREATE EXTENSION IF NOT EXISTS "vector";

DROP TYPE IF EXISTS "e_weight_class" CASCADE;
CREATE TYPE "e_weight_class" AS ENUM (
  'unknown',
  'flyweight',
  'bantamweight',
  'featherweight',
  'lightweight',
  'welterweight',
  'middleweight',
  'light_heavyweight',
  'heavyweight'
);

DROP TABLE IF EXISTS "fighters" CASCADE;
CREATE TABLE "fighters" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT UNIQUE NOT NULL,
  "date_of_birth" DATE NOT NULL,
  "weight_class" "e_weight_class" NOT NULL DEFAULT 'unknown'
) WITH (oids = FALSE);

CREATE INDEX "fighters_name_idx" ON "fighters" ("name");
CREATE INDEX "fighters_weight_class_idx" ON "fighters" ("weight_class");

DROP TYPE IF EXISTS "e_fighter_property" CASCADE;
CREATE TYPE "e_fighter_property" AS ENUM (
  'height',
  'weight',
  'reach'
);
DROP TYPE IF EXISTS "e_stat_type" CASCADE;
CREATE TYPE "e_stat_type" AS ENUM (
  'total',
  'zscore',
  'percentile',
  'differential'
);

DROP TABLE IF EXISTS "fighter_stats" CASCADE;
CREATE TABLE "fighter_stats" (
  "id" SERIAL PRIMARY KEY,
  "property" "e_fighter_property" NOT NULL,
  "type" "e_stat_type" NOT NULL,
  "value" FLOAT(53) NOT NULL,
  "fighter_id" INTEGER NOT NULL REFERENCES "fighters" ("id"),
  UNIQUE ("fighter_id", "property", "type")
) WITH (oids = FALSE);

DROP TYPE IF EXISTS "e_fight_method" CASCADE;
CREATE TYPE "e_fight_method" AS ENUM (
  'decision',
  'submission',
  'ko/tko',
  'draw',
  'no_contest'
);

DROP TABLE IF EXISTS "fights" CASCADE;
CREATE TABLE "fights" (
  "id" SERIAL PRIMARY KEY,
  "date" DATE NOT NULL,
  "method" "e_fight_method" NOT NULL,
  "duration" FLOAT(53) NOT NULL,
  "fighter_id" INTEGER NOT NULL REFERENCES "fighters" ("id"),
  "opponent_id" INTEGER NOT NULL REFERENCES "fighters" ("id"),
  "winner_id" INTEGER NOT NULL REFERENCES "fighters" ("id"),
  UNIQUE ("fighter_id", "opponent_id", "date")
) WITH (oids = FALSE);

DROP TYPE IF EXISTS "e_fight_property_modifier" CASCADE;
CREATE TYPE "e_fight_property_modifier" AS ENUM (
  'attempted',
  'landed',
  'defended',
  'absorbed'
);

DROP TYPE IF EXISTS "e_fight_property" CASCADE;
CREATE TYPE "e_fight_property" AS ENUM (
  'total_strikes',
  'significant_strikes',
  'clinch_strikes',
  'ground_strikes',
  'distance_strikes',
  'head_strikes',
  'leg_strikes',
  'body_strikes',
  'takedowns',
  'reversals',
  'knockdowns',
  'submissions',
  'control_time',
  'time_since_last_fight',
  'age',
  'height',
  'weight',
  'reach'
);

DROP TABLE IF EXISTS "fight_stats" CASCADE;
CREATE TABLE "fight_stats" (
  "id" SERIAL PRIMARY KEY,
  "property" "e_fight_property" NOT NULL,
  "modifier" "e_fight_property_modifier",
  "type" "e_stat_type" NOT NULL,
  "value" FLOAT(53) NOT NULL,
  "fighter_id" INTEGER NOT NULL REFERENCES "fighters" ("id"),
  "fight_id" INTEGER NOT NULL REFERENCES "fights" ("id"),
  UNIQUE ("fight_id", "fighter_id", "property", "modifier", "type")
) WITH (oids = FALSE);
