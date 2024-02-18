USE DATABASE PROJECT_DB;
USE SCHEMA RAW;

copy into '@my_s3_stage/inventory_20240204.csv' from (select * from PROJECT_DB.raw.inventory where cal_dt <= current_date())
file_format=(FORMAT_NAME=csv_comma_skip1_format, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE;

copy into '@my_s3_stage/sales_20240204.csv' from (select * from PROJECT_DB.raw.sales where trans_dt <= current_date())
file_format=(FORMAT_NAME=csv_comma_skip1_format, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@my_s3_stage/store_20240204.csv' from (select * from PROJECT_DB.raw.store)
file_format=(FORMAT_NAME=csv_comma_skip1_format, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@my_s3_stage/product_20240204.csv' from (select * from PROJECT_DB.raw.product)
file_format=(FORMAT_NAME=csv_comma_skip1_format, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;

copy into '@my_s3_stage/calendar_20240204.csv' from (select * from PROJECT_DB.raw.calendar)
file_format=(FORMAT_NAME=csv_comma_skip1_format, TYPE=CSV, COMPRESSION='None')
single = true
MAX_FILE_SIZE=107772160
OVERWRITE=TRUE
HEADER = TRUE
;
