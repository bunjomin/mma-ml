SELECT
  AVG(fs.value) as "value"
FROM
  fight_stats fs
WHERE
  %s = fs.fighter_id
  AND %s = fs.property
  AND %s = fs.modifier
  AND %s = fs.type
  AND fs.fight_id IN (
    SELECT
      id
    FROM
      fights
    WHERE
      "date" < %s
      AND "date" > ((%s)::DATE - INTERVAL '1 year')
  )
