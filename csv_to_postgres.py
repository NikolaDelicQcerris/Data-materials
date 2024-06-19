import pandas as pd
import psycopg2
from psycopg2 import sql

host = "172.18.0.1"
database = "Data-project"
user = "postgres"
password = "Cristiano15+"

def connect_to_db(host, database, user, password):
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        print("Connected to the database")
        return connection
    except Exception as error:
        print(f"Error connecting to database: {error}")
        return None

def infer_schema(df):
    sch_map={
        "object" : "TEXT",
        "int64" : "INTEGER",
        "float64" : "REAL",
        "datetime64[ns]" : "TIMESTAMP",
        "bool" : "BOOLEAN"
        }

    columns_def=[]
    for column, dtype in df.dtypes.items():
        pg_type = sch_map.get(str(dtype),"TEXT")
        columns_def.append(f"{column} {pg_type}")
            
        return ", ".join(columns_def)


csv_file_path = '/home/qcerris/Desktop/Data-project/Data-materials/cars.csv'
table_name = 'cars'

df = pd.read_csv(csv_file_path, index_col=False)

if 'Unnamed: 0' in df.columns:
    df.drop(columns=['Unnamed: 0'], inplace=True)

print("CSV columns:", df.columns)

connection = connect_to_db(host=host, database=database, user=user, password=password)

if connection:
    with connection:
        with connection.cursor() as cursor:
            schema=infer_schema(df)
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
            cursor.execute(create_table_query)

            columns = list(df.columns)
            column_str = ', '.join(columns)

            insert_query = sql.SQL("""
                INSERT INTO {table} ({fields})
                VALUES ({values})
            """).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )

            for row in df.itertuples(index=False, name=None):
                cursor.execute(insert_query, row)

        connection.commit()
else:
    print("Failed to connect to the database")




