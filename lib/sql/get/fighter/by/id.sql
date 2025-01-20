SELECT
  f.id as row_id,
  f.name,
  f.date_of_birth,
  f.weight_class
FROM
  fighters f
WHERE
  f.id = %s
LIMIT
  1
