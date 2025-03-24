### Overview:

This project demonstrates an end-to-end ETL and ELT pipeline for processing user session data using Apache Airflow, Snowpark, and Snowflake. 
The pipeline extracts data from an AWS S3 bucket, transforms and loads it into Snowflake, and further processes it for analytical insights. Finally, it integrates Apache Superset with Snowflake for visualization.

### Architecture and Workflow

1. ETL Pipeline (Extract, Transform, Load)

The ETL pipeline was built using Apache Airflow, orchestrating the extraction, transformation, and loading of user session data from an S3 bucket (s3://s3-geospatial/readonly/) into Snowflake.

### Steps:
- Extract:

Airflow extracts two datasets from S3:

 -User session channel data

 -Session timestamp data

- Transform:

 -The extracted data is staged in Snowflake.

- Load:

 -Created Snowflake staging tables:

 -user_session_channel_table

 -session_timestamp_table

  -Used the COPY INTO command to load both tables with the extracted data from S3.

### 2. ELT Pipeline (Transform Data Within Snowflake)

Once the raw data is staged in Snowflake, an ELT process is executed to transform and structure the data for analytics.

### Steps:
 -Transform using SQL (CTAS - Create Table As Select):

 -A CTAS query is executed to join the staged tables:


### Data Quality Checks:

 -Primary key uniqueness check

 -Duplicate records check

### Final Analytics Table Creation:

 Once data quality checks pass, the final analytics table (session_summary) is created by swapping the CTAS result into the analytics schema.

### Data Analytics and Visualization
 -The processed session summary data is stored in Snowflake's Analytics Schema.

 -Apache Superset is integrated with Snowflake for visualization.

- WAU (Weekly Active Users) Time Series Chart is created in Superset to analyze user engagement over time.

### Technology Stack

- Data Orchestration: Apache Airflow

- Cloud Storage: AWS S3

- Data Processing & Querying: Snowflake (Snowpark)

- Visualization: Apache Superset

- Languages Used: SQL, Python


