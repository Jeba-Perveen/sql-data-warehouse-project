use master;
go
  
-- create  ]'DataWarehouse' database
create database DataWarehouse;

use DataWarehouse;
go
  
--create schmeas
create schema bronze;
go
  
create schema silver;
go
  
create schema gold;
go
