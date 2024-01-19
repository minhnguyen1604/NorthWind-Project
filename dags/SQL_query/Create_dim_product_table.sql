CREATE OR REPLACE TABLE `OLAP.dim_product` AS
SELECT
  CAST(p.productID AS STRING) AS productID,
  p.productName,
  p.quantityPerUnit,
  CAST(p.unitPrice AS FLOAT64) AS unitPrice,
  CAST(p.unitsInStock AS INT64) AS unitsInStock,
  p.unitsOnOrder,
  CAST(p.reorderLevel AS INT64) AS reorderLevel,
  CAST(p.categoryID AS INT64) AS categoryID,
  c.categoryName,
  c.description
FROM
  `OLTP.products` AS p
JOIN
  `OLTP.categories` AS c
ON
  p.categoryID = c.categoryID
ORDER BY productID;


