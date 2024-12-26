import sys
import psycopg2
import pytz
from datetime import datetime

print("Process Started at : ", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"))

collection_name = 'docs_qna'

# db_params = {
#     'host': 'itx-acm-llm-poc-postgres01.czijpxum5el7.us-east-1.rds.amazonaws.com',
#     'database': 'llmpoc',
#     'user': 'llm_poc_admin',
#     'password': 'F89mtr=KHla3',
# }

db_params = {
    'host': 'itx-acm-genai-llm-prod-postgres01.czijpxum5el7.us-east-1.rds.amazonaws.com',
    'database': 'chat_prod',
    'user': 'chatjanssen_prod_admin',
    'password': 'EvNcyrMw4=3t',
}

# Query to execute
query = '''
DELETE FROM public.langchain_pg_embedding
WHERE collection_id IN (
    SELECT a.uuid
    FROM public.langchain_pg_collection a
    WHERE a.name = %s
) AND (cmetadata->>'timestamp')::timestamp < NOW() - INTERVAL '5 days'
'''

def run_script():
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        # Create a cursor
        cursor = connection.cursor()
        # Print message indicating the query is being executed
        print("Executing the delete query...")
        # Execute the delete query
        cursor.execute(query, (collection_name,))
        # Get the number of rows deleted
        rows_deleted = cursor.rowcount
        # Commit the transaction
        connection.commit()
        # Print the number of rows deleted
        print(f"Number of rows deleted: {rows_deleted}")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    # Check if there is a command-line argument and it matches "RUN"
    if len(sys.argv) > 1 and sys.argv[1] == "RUN":
        run_script()
    else:
        print("Script will not run. Use 'RUN' as a command-line argument to execute.")


print("Process completed at : ", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"))
