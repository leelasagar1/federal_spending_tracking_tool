import psycopg2
import psycopg2.extras
from psycopg2 import sql
import pandas as pd

def postgres_connect():

    RDS_ENDPOINT = "database-1.cfwk0mcqm9mf.us-east-2.rds.amazonaws.com"
    RDS_PORT = 5432
    RDS_USERNAME = "postgres"
    RDS_PASSWORD = "admin123"
    RDS_DB_NAME = "projectdb"

    conn = psycopg2.connect(
    host=RDS_ENDPOINT,
    port=RDS_PORT,
    user=RDS_USERNAME,
    password=RDS_PASSWORD,
    dbname=RDS_DB_NAME
    )
    return conn

# def write_to_postgres(df, table_name):
#     try:
#         # Create a connection to the PostgreSQL database
#         conn = postgres_connect()
#         cursor = conn.cursor()
#         # Create insert query dynamically based on dataframe columns
#         columns = ', '.join(df.columns)
#         values = ', '.join([f'%({col})s' for col in df.columns])
#         insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING'
#         # print(insert_query)
#         # Convert dataframe to dictionary format for easy insertion
#         data = df.to_dict(orient='records')
#         # print(data)
#         # Execute batch insert
#         psycopg2.extras.execute_batch(cursor, insert_query, data)
#         # Commit the transaction
#         conn.commit()
#         cursor.close()
#         conn.close()
#         print(f"Data successfully written to table {table_name}.")
#         return True
#     except Exception as e:
#         print(f"Error: {e}")
#         return False

def write_to_postgres(df, table_name):
    try:
        # Create a connection to the PostgreSQL database
        conn = postgres_connect()
        cursor = conn.cursor()
        
        # Iterate through each row of the dataframe
        for _, row in df.iterrows():
            # Remove nan values from the row
            clean_row = {key: value for key, value in row.items() if pd.notna(value)}
            
            # Create insert query dynamically based on cleaned row columns
            columns = ', '.join(clean_row.keys())
            values = ', '.join([f'%({col})s' for col in clean_row.keys()])
            insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING'
            
            # Execute insert for each row
            cursor.execute(insert_query, clean_row)
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Data successfully written to table {table_name}.")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def get_max_value(table_name,column_name):
    try:
        connection = postgres_connect()
        cursor = connection.cursor()

        query = sql.SQL("""
            SELECT MAX({column}) FROM {table}
        """).format(
            column=sql.Identifier(column_name),
            table=sql.Identifier(table_name)
        )

        # Execute the query
        cursor.execute(query)
        values = cursor.fetchone()

        if values[0] != None:
            max_value = values[0]
        else:
            max_value = 0
        return max_value + 1

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # Close database connection

        cursor.close()
        connection.close()



def get_columns_from_table(table_name):
    try:
        # Connect to the PostgreSQL database
        connection = postgres_connect()
        cursor = connection.cursor()
        
        # SQL query to get column names from the table
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """
        
        cursor.execute(query)
        columns = cursor.fetchall()
        
        # Print the column names
        column_names = [col[0] for col in columns]
        return column_names

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching column names:", error)
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()


def fetch_existing_data_from_postgres(table_name):
    """
    This function fetches existing data from the specified table in the PostgreSQL database.

    :param table_name: Name of the table to fetch data from
    :return: DataFrame containing the existing data from the specified table
    """
    try:
        # Create a connection to the PostgreSQL database
        conn = postgres_connect()
        # query = f"SELECT * FROM {table_name}"
        # existing_data = pd.read_sql(query, conn)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            records = cursor.fetchall()

            # Convert the records into a pandas DataFrame
            column_names = [desc[0] for desc in cursor.description]
            existing_data = pd.DataFrame(records, columns=column_names)

        conn.close()
        return existing_data

    except Exception as e:
        print(f"Error fetching existing data from {table_name}: {e}")
        return None
