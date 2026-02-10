use database STREAMS_DB;

create or replace
table sales_raw(
id varchar,
product varchar,
price varchar,
amount varchar,
store_id varchar,
is_current boolean DEFAULT TRUE);

INSERT INTO sales_raw (id, product, price, amount, store_id )
VALUES
    (1, 'Banana', 1.99, 1, 1 ),
    (2, 'Lemon',  0.99, 1, 1 );

select * from sales_raw;

create or replace table store_table(
store_id number,
location varchar,
employees number,
is_current boolean DEFAULT TRUE);

INSERT INTO STORE_TABLE 
(store_id, location,employees)
VALUES
(1,'Chicago',33),
(2, 'London', 12);

select * from store_table;

create or replace table sales_final_table(
id int,
product varchar,
price number,
amount int,
store_id int,
is_current boolean,
location varchar,
employees int);


-- Insert into final table

INSERT INTO sales_final_table (id,product,price,amount,store_id,is_current,location,employees)
SELECT
    SA.id,
    SA.product,
    SA.price,
    SA.amount,
    ST.store_id,
    ST.is_current,
    ST.location,
    ST.employees
FROM sales_raw SA
JOIN store_table ST
  ON ST.store_id = SA.store_id;


select * from sales_final_table;

create or replace stream sales_stream on table sales_raw;

select * from sales_stream;

SHOW STREAMS;

desc stream sales_stream;

-- Get changes on data using stream (INSERTS)
select * from sales_stream;

select * from sales_raw;

-- insert values
insert into sales_raw (id, product, price, amount, store_id)
values
(3, 'Mango' , 1.99, 1, 2),
(4,'Garlic', 0.99,1,1);

-- Get changes on data using stream (INSERTS)

select * from sales_stream;

select * from sales;
select * from sales_final_table;

