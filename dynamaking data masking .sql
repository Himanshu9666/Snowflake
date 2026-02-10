create database if not exists sales_db;
USE sales_db; -- database
USE ROLE ACCOUNTADMIN; -- masking / polciy accointadmin ke through


-- Prepare table --
create or replace table customers(
  id number,
  full_name varchar,
  email varchar,
  phone varchar,
  spent number,
  create_date DATE DEFAULT CURRENT_DATE);

-- insert values in table --
insert into customers (id, full_name, email,phone,spent)
values
  (1,'abc','asakjsdfjkasf@un.org','583-665-9168',333),
  (2,'Tylor','iuyhshtgall1@mayoclinic.com','412-987-7120',643),
  (3,'Marieee','mspadllkjwelkrjtazzi2@txnews.com','412-946-3659',1356),
  (4,'Neena Sharma','sadfasdf@patch.com','123-853-8192',9795),
  (5,'Odilia','fsadf@globo.com','095-451-8637',2958),
  (6,'LrMeggie123','yhasdf@rediff.com','866-896-6138',800);

select * from customers;

-- set up roles
CREATE OR REPLACE ROLE ANALYST_MASKED;
CREATE OR REPLACE ROLE ANALYST_FULL;


-- grant all PRIVILEGES on db.* to role role_name;
-- grant select on table to roles
GRANT SELECT ON TABLE sales_db.PUBLIC.CUSTOMERS TO ROLE ANALYST_MASKED;
GRANT SELECT ON TABLE sales_db.PUBLIC.CUSTOMERS TO ROLE ANALYST_FULL;

show grants to role ANALYST_FULL;

GRANT USAGE ON SCHEMA sales_db.PUBLIC TO ROLE ANALYST_MASKED;
GRANT USAGE ON SCHEMA sales_db.PUBLIC TO ROLE ANALYST_FULL;

show USERS;
-- show grant to users HIMANSHU;

-- grant warehouse access to roles
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_MASKED;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_FULL;


select current_user();
-- assign roles to a user
GRANT ROLE ANALYST_MASKED TO USER HIMANSHU;
GRANT ROLE ANALYST_FULL TO USER HIMANSHU;

show grants to user HIMANSHU;

-- Set up making policy;
-- var is column of varchar datatype
-- case => condition in case if my role is analyst_full then i will acess the orginal column
-- itself 
-- otherwise we will see the ##
create or replace masking policy phone_p
as (val varchar)
returns varchar ->
    case
        when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
        else '##-###-##'
    end;

-- apply policy on specific column 
Alter table if exists customers modify column phone
set masking policy phone_p;

USE ROLE accountadmin; -- jaan bhuj kr likha hai 
select current_role();
select * from customers;

USE ROLE ANALYST_MASKED;
select * from customers;

ALTER table if exists customers modify column email
set masking policy phone_p;



--replace or drop policy 


use role accountadmin;
Drop masking policy phone_p; -- poicy drop karni hi make sure vo kisi ko attach nhi ho

desc masking policy phone_p; -- policy ke baare me
show masking policies;

select * from table(information_schema.policy_references(policy_name=> 'phone_p'));

Alter table customers modify column phone unset masking policy;
Alter table customers modify column email unset masking policy;

Drop masking policy phone_p; -- ow we can drop policy 

create or replace masking policy phone
as (val varchar)
returns varchar ->
    case
        when current_role() in ('ANALYST_FULL', 'ACCOUNTADMIN') then val
        else concat(left(val, 2), '********')
    end;

Alter table if exists customers modify column phone
set masking policy phone;

USE ROLE accountadmin; -- jaan bhuj kr likha hai 
select current_role();
select * from customers;

USE ROLE ANALYST_MASKED;
select * from customers;