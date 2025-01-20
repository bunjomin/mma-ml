SELECT
  f.id as row_id,
  f.name,
  f.date_of_birth,
  f.weight_class
FROM
  fighters f
WHERE
  f.name = %s
ORDER BY
  f.id
LIMIT
  1
