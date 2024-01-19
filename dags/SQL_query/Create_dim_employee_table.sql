CREATE OR REPLACE TABLE `OLAP.dim_employee` AS
SELECT
  employeeID,
  CONCAT(firstName, ' ', lastName) AS employeeName,
  title,
  titleOfCourtesy,
  birthDate,
  hireDate,
  address,
  city,
  region,
  postalCode,
  country,
  homePhone AS phone
FROM
  `OLTP.employees`
 ORDER BY employeeID ASC;