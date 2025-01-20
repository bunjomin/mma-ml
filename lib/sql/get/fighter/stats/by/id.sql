SELECT
  property,
  type,
  value
FROM
  fighter_stats
WHERE
  fighter_id = %s
ORDER BY
  id DESC
