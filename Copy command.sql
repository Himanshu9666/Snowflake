create or replace database himanshudb;

create schema external_stages;

create or replace table himanshudb.public.product(ProductID varchar(30),ProductName varchar(30),category varchar(30),Subcategory varchar (30));

describe table product;
/* 
create table => create stage => load data from stage to the table using copy command 
access key - 

*/
create or replace stage s3_stage_location
 URL = 's3://regex-himanshu'
 CREDENTIALS = (AWS_KEY_ID = 'AKIATX6T4SC'
 AWS_SECRET_KEY ='wY7ph7axFRNPOXMf');

describe stage himanshudb.external_stages.s3_stage_location;

list@himanshudb.external_stages.s3_stage_location; -- for check all the file of s3
-- copying file from s3_stage(extrenal stage ) to the snowflake 

-- copy command
copy into  himanshudb.public.product 
from @himanshudb.external_stages.s3_stage_location
file_format = (skip_header = 1);

select * from himanshudb.public.product;


---
use database himanshudb;
create or replace schema New_schema;

create or replace stage new_stage
 URL = 's3://regex-himanshu'
 CREDENTIALS = (AWS_KEY_ID = 'AKIAV55ER'
 AWS_SECRET_KEY ='wY7phSxFRNPOXMf');

create or replace table std (id int,name varchar(20),email varchar(30));
select * from std;

describe stage himanshudb.New_schema.new_stage;

-- list command is used to access the files of external location
-- into the temp area external stage =>  

list @himanshudb.New_schema.new_stage;

-- copy command
copy into himanshudb.New_schema.std 
from @himanshudb.New_schema.new_stage
file_format = (FIELD_DELIMITER='-',skip_header = 1)
FILES = ('stddata.txt');

select * from std;


----
-- 13/01/2026

use database himanshudb;
create or replace schema himanshudb.testschema;
create table himanshudb.testschema.employee(id int,Name varchar(30),age int);


SELECT * FROM employee;


create or replace stage himanshudb.testschema.s3_stage
URL = 's3://regex-himanshu'
 CREDENTIALS = (AWS_KEY_ID = 'AKIAV55ER'
 AWS_SECRET_KEY ='wY7phFRNPOXMf');


DESCRIBE STAGE himanshudb.testschema.s3_stage;

list @himanshudb.testschema.s3_stage;

copy into himanshudb.testschema.employee
from( select substr(s.$1,2), s.$2, s.$4 from @s3_stage s)
file_format=( field_delimiter='-', skip_header=1);

select * from employee;

 


 

