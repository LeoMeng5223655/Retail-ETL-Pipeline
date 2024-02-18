USE DATABASE PROJECT_DB;
USE SCHEMA RAW;

-- Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::339712925703:role/mysnowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://etl-bucket-leo/')

-- Retrieve the AWS IAM User
DESC INTEGRATION s3_integration;
--  Grant Stage
GRANT CREATE STAGE ON SCHEMA RAW TO ROLE accountadmin;

GRANT USAGE ON INTEGRATION s3_integration TO ROLE accountadmin;
-- Create Stage
CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://etl-bucket-leo/'
  FILE_FORMAT = csv_comma_skip1_format;

DESC STAGE my_s3_stage;
LIST @my_s3_stage;
