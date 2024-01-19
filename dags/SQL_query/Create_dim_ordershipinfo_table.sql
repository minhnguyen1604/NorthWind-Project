CREATE OR REPLACE TABLE `OLAP.dim_ordershipinfo` AS
SELECT
  orderID,
  requiredDate,
  shippedDate,
  freight,
  shipName,
  shipAddress,
  shipCity,
  shipRegion,
  shipPostalCode,
  shipCountry
FROM
  `OLTP.orders`;