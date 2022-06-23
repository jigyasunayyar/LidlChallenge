WITH
  impressions AS (
  SELECT
    *
  FROM
    UNNEST([ STRUCT(1002313 AS product_id,
        'true' AS click,
        DATE("2018-07-10") AS date_id), (1002312,
        'false',
        DATE("2018-07-10")) ])),
  purchases AS (
  SELECT
    *
  FROM
    UNNEST([ STRUCT(1002313 AS product_id,
        103431 AS user_id,
        DATE("2018-07-10") AS date_id), (1002312,
        103432,
        DATE("2018-07-11")) ] )),
  products AS (
  SELECT
    *
  FROM
    UNNEST([ STRUCT(1002313 AS product_id,
        1 AS category_id,
        10 AS price), (1002312,
        2,
        15) ] ) )
-------------------------------------------
  --question 1
-------------------------------------------
  -- SELECT
  --   product_id, EXTRACT(month FROM date_id) month,
  --   COUNT(CASE
    --         WHEN click = "true" THEN click
    --       ELSE
    --       NULL end) /  COUNT(*) ctr
  -- FROM
  --   impressions
  -- GROUP BY
  --   1, 2
-------------------------------------------
-------------------------------------------

-------------------------------------------
  -- question 2
-------------------------------------------
-- SELECT
--   pd.category_id,
--   COUNT(CASE
--       WHEN click = "true" THEN click
--     ELSE
--     NULL
--   END
--     ) / COUNT(*) ctr
-- FROM
--   impressions AS imp
-- JOIN
--   products AS pd
-- ON
--   imp.product_id = pd.product_id
-- GROUP BY
--   1
-- ORDER BY
--   2 DESC
-- LIMIT
--   3
-------------------------------------------
-------------------------------------------

-------------------------------------------
  -- question 3
-------------------------------------------
  -- SELECT
  --   CASE
  --     WHEN pd.price BETWEEN 0 AND 5 THEN "0-5"
  --     WHEN pd.price BETWEEN 6 AND 10 THEN "6-10"
  --     WHEN pd.price BETWEEN 11 AND 15 THEN "11-15"
  --   ELSE ">15"
  -- END
  --   price_bucket,
  --   COUNT(CASE
    --       WHEN click = "true" THEN click
    --     ELSE
    --     NULL
    --   END
    --     ) / COUNT(*) ctr
  -- FROM
  --   impressions imp
  -- JOIN
  --   products pd
  -- ON
  --   imp.product_id = pd.product_id
  --   group by 1
-------------------------------------------
-------------------------------------------
