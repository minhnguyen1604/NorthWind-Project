CREATE OR REPLACE TABLE `OLAP.dim_customer` AS
SELECT
  customerID,
  companyName AS customerName,
  contactName,
  contactTitle,
  address,
  city,
  region,
  postalCode,
  country,
  phone
FROM
  `OLTP.customers`;