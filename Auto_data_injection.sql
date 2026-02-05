-- 1. Database and Schemas
create OR replace DATABASE projectdb;

create or replace schema projectdb.snowpipe_schema; -- pipe schema
create or replace schema projectdb.table_schema_dim; -- table schema 
create or replace schema projectdb.file_formate_schema_dim; --- file formate schema
create or replace schema projectdb.pro_stage_schema; -- stage schema

-- 2. File Format
CREATE OR REPLACE FILE FORMAT projectdb.file_formate_schema_dim.jsonformat
TYPE = JSON;

-- 3. Table
CREATE OR REPLACE TABLE projectdb.table_schema_dim.dim_customer (
    row_column VARIANT,
    file_name STRING,
    load_time TIMESTAMP
);

-- 4. Stage
CREATE OR REPLACE STAGE projectdb.pro_stage_schema.s3_snowpipe_stage
URL = 's3://regex-himanshu'
CREDENTIALS = (
  AWS_KEY_ID = '',
  AWS_SECRET_KEY = ''
);

--  check file working
LIST @projectdb.pro_stage_schema.s3_snowpipe_stage;

-- 5. Snowpipe
CREATE OR REPLACE PIPE projectdb.snowpipe_schema.dim_pipe
AUTO_INGEST = TRUE
AS
COPY INTO projectdb.table_schema_dim.dim_customer
(row_column, file_name, load_time)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED
    FROM @projectdb.pro_stage_schema.s3_snowpipe_stage
)
FILE_FORMAT = projectdb.file_formate_schema_dim.jsonformat;

-- 6. Inspect pipe
DESC PIPE projectdb.snowpipe_schema.dim_pipe;

-- 7. Count rows
SELECT COUNT(*) FROM projectdb.table_schema_dim.dim_customer;

SELECT * FROM projectdb.table_schema_dim.dim_customer;

SELECT
    row_column:CITY::STRING AS city,
    row_column:COUNTRY::STRING AS country,
    row_column:CUSTOMER_ID::STRING AS customer_id,
    row_column:CUSTOMER_NAME::STRING AS customer_name,
    row_column:CUSTOMER_SEGMENT::STRING AS customer_segment,
    row_column:CUSTOMER_STATUS::STRING AS customer_status,
    row_column:EFFECTIVE_START_DT::TIMESTAMP AS effective_start_dt,
    row_column:EFFECTIVE_END_DT::TIMESTAMP AS effective_end_dt,
    row_column:EMAIL_ID::STRING AS email,
    row_column:IS_CURRENT::BOOLEAN AS is_current,
    row_column:LOAD_DATE::DATE AS load_date,
    YEAR((row_column:LOAD_TIMESTAMP::TIMESTAMP)) AS year,
    row_column:PHONE_NUMBER::STRING AS phone_number,
    row_column:RECORD_HASH::STRING AS record_hash,
    row_column:SOURCE_SYSTEM::STRING AS source_system,
    row_column:STATE::STRING AS state
FROM projectdb.table_schema_dim.dim_customer;

--- creating new silver table

CREATE OR REPLACE TABLE projectdb.table_schema_dim.dim_customer_silver (
    city STRING,
    country STRING,
    customer_id STRING,
    customer_name STRING,
    customer_segment STRING,
    customer_status STRING,
    effective_start_dt TIMESTAMP,
    effective_end_dt TIMESTAMP,
    email STRING,
    is_current BOOLEAN,
    load_date DATE,
    year INT,
    month INT,
    quarter INT,
    dayname STRING,
    phone_number STRING,
    record_hash STRING,
    source_system STRING,
    state STRING
);

-- stream


CREATE OR REPLACE STREAM projectdb.table_schema_dim.dim_customer_stream
ON TABLE projectdb.table_schema_dim.dim_customer
APPEND_ONLY = TRUE;  

---- task 

CREATE OR REPLACE TASK projectdb.table_schema_dim.silver_task
WAREHOUSE = compute_wh
SCHEDULE = '1 minute'
AS
INSERT INTO projectdb.table_schema_dim.dim_customer_silver
SELECT
    row_column:CITY::string,
    row_column:COUNTRY::string,
    row_column:CUSTOMER_ID::string,
    row_column:CUSTOMER_NAME::string,
    row_column:CUSTOMER_SEGMENT::string,
    row_column:CUSTOMER_STATUS::string,
    row_column:EFFECTIVE_START_DT::timestamp,
    row_column:EFFECTIVE_END_DT::timestamp,
    row_column:EMAIL_ID::string,
    row_column:IS_CURRENT::boolean,
    row_column:LOAD_DATE::date,
    YEAR(row_column:LOAD_TIMESTAMP::timestamp),
    MONTH(row_column:LOAD_TIMESTAMP::timestamp),
    QUARTER(row_column:LOAD_TIMESTAMP::timestamp),
    DAYNAME(row_column:LOAD_TIMESTAMP::timestamp),
    row_column:PHONE_NUMBER::string,
    row_column:RECORD_HASH::string,
    row_column:SOURCE_SYSTEM::string,
    row_column:STATE::string
FROM projectdb.table_schema_dim.dim_customer_stream;

-- check data is insterted 
select * from projectdb.table_schema_dim.dim_customer_stream;


-- check silver table data
alter task projectdb.table_schema_dim.silver_task RESUME;
SELECT *
FROM projectdb.table_schema_dim.dim_customer_silver;




-----------------------
