CREATE OR REPLACE TABLE `OLAP.dim_date` AS
SELECT
  DISTINCT
  CAST(FORMAT_DATE('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', orderDate))) AS INT64) AS orderDateKey,
  EXTRACT(MONTH FROM DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', orderDate))) AS month,
  EXTRACT(QUARTER FROM DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', orderDate))) AS quarter,
  EXTRACT(YEAR FROM DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', orderDate))) AS year,
  DATE(TIMESTAMP(orderDate)) AS OrderDate
FROM
  `OLTP.orders`;



  