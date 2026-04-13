-- Sql server
/*
======================================================
Create Database and Schemas
======================================================

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, 
the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/
use master
go

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN 
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
  
--Create the new 'DataWarehouse' database
create database DataWarehouse
go

--use the 'DataWarehouse'
use DataWarehouse
go

--Create Schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go


/*
======================================================
Create DWH Schemas (Users) in Oracle
======================================================
Script Purpose:
This script sets up the three layers of the Data Warehouse by creating 
three separate schemas: 'bronze', 'silver', and 'gold'.

Note: In Oracle, a 'Schema' is owned by a 'User'. 
We create users and grant them session privileges.
*/

-- 1. Drop Users if they already exist to start fresh
-- 'CASCADE' ensures all tables/objects inside are deleted
select * from dba_users;

BEGIN
    FOR user_rec IN (SELECT username FROM dba_users WHERE username IN ('BRONZE', 'SILVER', 'GOLD')) LOOP
        EXECUTE IMMEDIATE 'DROP USER ' || user_rec.username || ' CASCADE';
    END LOOP;
END;
/

-- 2. Create the Bronze Schema
CREATE USER bronze IDENTIFIED BY password;

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE TO bronze;

ALTER USER bronze QUOTA UNLIMITED ON USERS;

-- 3. Create the Silver Schema
CREATE USER silver IDENTIFIED BY password;

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE TO silver;

ALTER USER silver QUOTA UNLIMITED ON USERS;

-- 4. Create the Gold Schema
CREATE USER gold IDENTIFIED BY password;

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE TO gold;

ALTER USER gold QUOTA UNLIMITED ON USERS;


SELECT * FROM dba_users WHERE username IN ('BRONZE', 'SILVER', 'GOLD')
