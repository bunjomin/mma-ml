SELECT
  f.id as row_id,
  f.name,
  f.date_of_birth,
  f.weight_class
FROM
  fighters f
ORDER BY
  f.name
