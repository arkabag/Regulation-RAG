import psycopg2
import json
import io


class PostgresLocalConnector(object):
    def __init__(self, secrets_variable='postgres-md-database-credentials'):
        """
        Reads the environment variables and initializes few parameters.
        """
        self.POSTGRES_HOST = '0.0.0.0'
        self.POSTGRES_USERNAME = 'testuser'
        self.POSTGRES_PASSWORD = 'testpwd'
        self.POSTGRES_PORT = 5432
        self.POSTGRES_DB = 'vectordb'

        self.conn_string = "host=" + str(self.POSTGRES_HOST) + " port=" + str(self.POSTGRES_PORT) + \
                           " dbname=" + str(self.POSTGRES_DB) + " user=" + str(self.POSTGRES_USERNAME) \
                           + " password=" + str(self.POSTGRES_PASSWORD)
        print('PostGRES database to be used: ' + self.POSTGRES_HOST + ":" + str(self.POSTGRES_PORT))

        self.sql_dtype = {
            'object': 'TEXT',
            'float64': 'FLOAT',
            'int': 'INT',
            'int64': 'BIGINT',
            'datetime': 'TIMESTAMP',
            'str': 'TEXT'
        }

        # Check connection to the PostGRES database
        try:
            connection = psycopg2.connect(self.conn_string)
            connection.close()
            print('Successfully connected to PostGRES database.')
        except (RuntimeError, Exception) as err:
            print('Failed to connect to PostGRES database. Please check your environment variables and try again.')
            print('\nTHE APPLICATION WILL NOT WORK PROPERLY.\n')

    def query(self, query_string):
        """
        Executes the query and returns the results
        """
        conn = psycopg2.connect(self.conn_string)
        cursor = conn.cursor()
        cursor.execute(query_string)
        results = cursor.fetchall()
        cursor.close()
        return results

    def execute(self, query_string):
        """
        Executes the query
        """
        conn = psycopg2.connect(self.conn_string)
        conn.set_session(autocommit=True)
        cursor = conn.cursor()
        cursor.execute(query_string)
        cursor.close()
        return

    @staticmethod
    def close_cursor(self, cursor):
        """
        Closes the connection to the database.
        """
        cursor.close()

    def copy_from_csv(self, file_path, table_name, columns, sep=','):
        """
        columns must be a tuple
        """
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()
        f = open(file_path)
        #cur.copy_from(f, table_name, columns=columns, sep=sep)
        cur.copy_from(f, table_name, columns=columns, sep=sep)
        #sql = f'COPY {table_name}({str(columns)}) FROM STDIN WITH HEADER CSV'
        #print(sql)
        #cur.copy_expert(sql, f)
        conn.commit()
        conn.close()

    def save_dataframe(self, df, table_name):
        conn = psycopg2.connect(self.conn_string)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

    def create_table_from_dataframe(self, df, table_name):
        columns = df.columns
        data_types = df.dtypes
        print(data_types.shape)
        create_query = f"DROP TABLE IF EXISTS {table_name}; " \
                       f"CREATE TABLE {table_name} ( "
        table_fields = []
        for i in range(data_types.shape[0]):
            column_name = columns[i]
            dtype = data_types[i]
            table_fields.append(f"{column_name} {self.sql_dtype[str(dtype)]}")
        all_fields = ",".join(table_fields)
        create_query += f"{all_fields});"
        self.execute(create_query)

    def save_dataframe_to_table_rows(self, df, table_name, include_index=False):
        columns = df.columns
        df = df.reset_index()

        for index, row in df.iterrows():
            sql_column_list = []
            sql_value_list = []
            for index, value in row.items():
                if index == 'index' and include_index is False:
                    continue
                sql_column_list.append(f"{index}")

                if isinstance(value, int) or isinstance(value, float):
                    sql_value_list.append(f"{value}")
                else:
                    sql_value_list.append(f"'{value}'")
            insert_statement = f"INSERT INTO {table_name} ({','.join(sql_column_list)}) VALUES ({','.join(sql_value_list)});"
            #print(insert_statement)
            self.execute(insert_statement)
        return

    def save_data_frame_to_table_via_csv(self, df, table_name):
        # Path to your local CSV file
        csv_file_path = '../../data/vector_database_wikipedia_articles_embedded.csv'

        # Define a generator function to process the csv file
        def process_file(file_path):
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    yield line

        # Create a StringIO object to store the modified lines
        modified_lines = io.StringIO(''.join(list(process_file(csv_file_path))))

        # Create the COPY command for copy_expert
        copy_command = f'''
        COPY public.articles (id, url, title, content, title_vector, content_vector, vector_id)
        FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');
        '''

        # Execute the COPY command using copy_expert
        connection = psycopg2.connect(self.conn_string)
        cursor = connection.cursor()
        cursor.copy_expert(copy_command, modified_lines)

        # Commit the changes
        connection.commit()
