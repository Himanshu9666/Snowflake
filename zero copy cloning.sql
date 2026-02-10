
use streams_db;
create or replace table sales_raw_staging(
  id varchar,
  product varchar,
  price varchar,
  amount varchar,
  store_id varchar);
 
-- insert values
insert into sales_raw_staging
    values
        (1,'Banana',1.99,1,1),
        (2,'Lemon',0.99,1,1),
        (3,'Apple',1.79,1,2),
        (4,'Orange Juice',1.89,1,2),
        (5,'Cereals',5.98,2,1);  


select * from streams_db.public.sales_raw_staging;
drop table streams_db.public.sales_raw_staging;

create table sales_raw_clone
clone sales_raw_staging;

select * from sales_raw_clone;

update sales_raw_clone set product='regex' where id in (1,2);
select * from sales_raw_clone; -- change 2 new are insert into new partition

select * from sales_raw_staging;
update sales_raw_staging set product='tushar' where id in (4);

select * from sales_raw_clone; -- change 2 new are insert into new partition

-- database schema clone
-- same way we copy for schema
create schema streams_db.order_clone
clone streams_db.order_Ext_stage;

select * from streams_db.order_clone.ordertable;



-- tushardb has this table
select * from sales_raw_staging;
update sales_raw_staging set price=10;

-- time travel
select * from sales_raw_staging at (offset => -120);

-- clone table with time travel
create or replace table sales_clone2_time
clone sales_raw_staging at (offset => -300);

select * from sales_clone2_time; -- done clone with timetravel
select * from sales_raw_staging;

-- data sample
-- data sampling

-- you need to create a task
-- -- which will copy the file
-- from a bronze table to silver table different in schema
-- while extracting the year, month,
-- quater and the name of the week day
-- and all these column should in the
-- silver table