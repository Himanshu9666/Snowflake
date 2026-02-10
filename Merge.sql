use database orderdb;
 
CREATE  or replace TABLE emp (
    id INT,
    price INT
);

INSERT INTO emp (id, price) VALUES
(1, 200),
(2, 500);


select * from emp;

CREATE  or replace TABLE emp1 (
    id INT,
    price INT
);

INSERT INTO emp1 (id, price) VALUES
(1, 1000);

select * from emp1;

MERGE INTO emp AS target
USING emp1 AS source
ON target.id = source.id

WHEN MATCHED THEN
    UPDATE SET target.price = source.price

WHEN NOT MATCHED THEN
    INSERT (id, price)
    VALUES (source.id, source.price);

select * from emp;