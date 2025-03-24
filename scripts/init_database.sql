/*
Create Database and Schema
*/

use master
go

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set SINGLE_USER WITH ROLLBACK IMMEDIATE;
	drop database DataWarehouse;
end
go

-- Create the 'DataWarehouse' database
create database DataWarehouse

use DataWarehouse
-- Create Scherma
create schema bronze
go
create schema silver
go
create schema gold
go

