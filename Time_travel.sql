use orderdb;
create table test ( id int,name varchar(30));
insert into test values(10,'abc'),(11,'def');
select * from test;
update test set name ='himanshu';

select * from test AT(OFFSET => -60);

select * from test;
delete from test;

select * from test;
select * from test AT(OFFSET => -60*2);   -- data from few secound ago

select * from test before (statement => '01c1e5a2-3202-3d47-0013-16e20001ff7e');

select current_timestamp();


create warehouse DS_WH
with warehouse_size ='small'
warehouse_type ='standard'
auto_suspend = 300
auto_resume =true
min_cluster_count = 1
max_cluster_count = 1
scaling_policy = 'standard';


// create role for data scientist & DBS

create user DS1 password ='DS1' Login_name = 'DS1' default_warehouse = 'DS_WH';