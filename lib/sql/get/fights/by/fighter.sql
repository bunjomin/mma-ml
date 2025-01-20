SELECT
  id as row_id,
  "date",
  method,
  duration,
  fighter_id,
  opponent_id,
  winner_id
FROM
  fights
WHERE
  "date" < %s
  AND (fighter_id = %s
  OR opponent_id = %s)
ORDER BY
  id DESC
