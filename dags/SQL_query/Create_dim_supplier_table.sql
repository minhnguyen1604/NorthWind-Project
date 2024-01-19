CREATE OR REPLACE TABLE `OLAP.dim_suppliers` AS
SELECT
  CAST(supplierID AS STRING) AS supplierID,
  companyName AS supplierName,
  contactName,
  contactTitle,
  address,
  city,
  region,
  postalCode,
  country,
  phone
FROM
  `OLTP.suppliers`
ORDER BY supplierID;


