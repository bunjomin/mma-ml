SELECT
  "date",
  "fight_id",
  case when fs.fighter_id != %s then 'opponent' else NULL end as "prefix",
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
