CREATE OR REPLACE TABLE `OLAP.fact_order` AS
SELECT
  o.orderID,
  od.productID,
  o.customerID,
  o.employeeID,
  p.supplierID,
  CAST(FORMAT_DATE('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', o.orderDate))) AS INT64) AS orderdatekey,
  od.unitPrice,
  od.quantity,
  od.discount
FROM
  `OLTP.orders` AS o
JOIN
  `OLTP.orderdetails` AS od
ON
  o.orderID = od.orderID
JOIN
  `OLTP.products` AS p
ON
  od.productID = p.productID;


  