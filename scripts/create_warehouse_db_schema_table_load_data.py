import snowflake.connector
import sys

def create_warehouse_db_schema_table(account, user, password, warehouse_name, role, stage_name, s3_bucket, file_name):
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role
        )
        cursor = conn.cursor()

        # Create a new warehouse
        create_warehouse_query = f"""
        CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}
        WITH WAREHOUSE_SIZE = 'XSMALL'
        WAREHOUSE_TYPE = 'STANDARD'
        AUTO_SUSPEND = 60
        AUTO_RESUME = TRUE;
        """
        cursor.execute(create_warehouse_query)
        print(f"Warehouse {warehouse_name} created successfully.")

        # Create a new database
        create_database_query = "CREATE DATABASE IF NOT EXISTS FOOD_REVIEWS_DB;"
        cursor.execute(create_database_query)
        print("Database FOOD_REVIEWS_DB created successfully.")

        # Create a new schema
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS FOOD_REVIEWS_DB.REVIEWS_SCHEMA;"
        cursor.execute(create_schema_query)
        print("Schema REVIEWS_SCHEMA created successfully.")

        # Create a new table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS FOOD_REVIEWS_DB.REVIEWS_SCHEMA.AMAZON_FOOD_REVIEWS (
            REVIEW_ID STRING,
            PRODUCT_ID STRING,
            REVIEW_TEXT STRING,
            RATING INT,
            REVIEW_DATE DATE
        );
        """
        cursor.execute(create_table_query)
        print("Table AMAZON_FOOD_REVIEWS created successfully.")
        
        # Create an external stage to access the S3 bucket
        create_stage_query = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL='s3://{s3_bucket}'
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """
        cursor.execute(create_stage_query)
        print(f"Stage {stage_name} created successfully, connected to S3 bucket {s3_bucket}.")

        # Copy data from the S3 bucket into the Snowflake table
        copy_query = f"""
        COPY INTO FOOD_REVIEWS_DB.REVIEWS_SCHEMA.AMAZON_FOOD_REVIEWS
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'SKIP_FILE';
        """
        cursor.execute(copy_query)
        print(f"Data from {file_name} loaded into AMAZON_FOOD_REVIEWS table successfully.")
        
    except Exception as e:
        print(f"Error creating warehouse, database, schema, table, or loading data: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    account = sys.argv[1]
    user = sys.argv[2]
    password = sys.argv[3]
    warehouse_name = sys.argv[4]
    role = sys.argv[5]
    stage_name = sys.argv[6]
    s3_bucket = sys.argv[7]
    file_name = sys.argv[8]

    create_warehouse_db_schema_table(account, user, password, warehouse_name, role, stage_name, s3_bucket, file_name)
