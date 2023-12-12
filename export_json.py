import json
import decimal
import psycopg2
from psycopg2.extras import RealDictCursor

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

# Replace these values with your PostgreSQL connection details
db_params = {
    'host': 'localhost',
    'database': 'restaurant_db',
    'user': 'pazyuka_oleg',
    'password': '',
}

def export_data_to_json(connection, output_file):
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get a list of all tables in the database
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [table['table_name'] for table in cursor.fetchall()]

            # Create a dictionary to store data from all tables
            all_data = {}

            for table in tables:
                # Fetch all rows from each table
                cursor.execute(f"SELECT * FROM {table}")
                table_data = cursor.fetchall()

                # Store the table data in the dictionary
                all_data[table] = table_data

            # Write the dictionary to a JSON file using the custom encoder
            with open(output_file, 'w') as json_file:
                json.dump(all_data, json_file, indent=2, cls=DecimalEncoder)

            print(f'Data exported to {output_file}')

    except psycopg2.Error as e:
        print(f"Error: {e}")

# Establish a connection to the PostgreSQL database
try:
    connection = psycopg2.connect(**db_params)
    export_data_to_json(connection, 'export.json')

finally:
    # Close the database connection when done
    if connection:
        connection.close()
