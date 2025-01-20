SELECT
  id as row_id,
  date,
  method,
  duration,
  fighter_id,
  opponent_id,
  winner_id
FROM
  fights
ORDER BY
  id DESC
