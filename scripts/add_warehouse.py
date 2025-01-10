import snowflake.connector
import sys

def create_warehouse(account, user, password, warehouse_name, role, database, schema):
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            warehouse=warehouse_name,
            database=database,
            schema=schema
        )

        # Create a new warehouse
        cursor = conn.cursor()
        create_query = f"""
        CREATE WAREHOUSE IF NOT EXISTS TEST_WH
        WITH WAREHOUSE_SIZE = 'XSMALL'
        WAREHOUSE_TYPE = 'STANDARD'
        AUTO_SUSPEND = 60
        AUTO_RESUME = TRUE;
        """
        cursor.execute(create_query)
        print(f"Warehouse TEST_WH created successfully.")
        
    except Exception as e:
        print(f"Error creating warehouse: {e}")
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
    database = sys.argv[6]
    schema = sys.argv[7]

    create_warehouse(account, user, password, warehouse_name, role, database, schema)
