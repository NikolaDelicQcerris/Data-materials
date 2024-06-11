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
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            pg_columns = [row[0] for row in cursor.fetchall()]

            print("PostgreSQL columns:", pg_columns)

            csv_to_pg_column_map = {
                'symboling': 'symboling',
                'normalized-losses': 'normalized-losses',
                'make': 'make',
                'fuel-type': 'fuel-type',
                'aspiration': 'aspiration',
                'num-of-doors': 'num-of-doors',
                'body-style': 'body-style',
                'drive-wheels': 'drive-wheels',
                'engine-location': 'engine-location',
                'wheel-base': 'wheel-base',
                'length': 'length',
                'width': 'width',
                'height': 'height',
                'curb-weight': 'curb-weight',
                'engine-type': 'engine-type',
                'num-of-cylinders': 'num-of-cylinders',
                'engine-size': 'engine-size',
                'fuel-system': 'fuel-system',
                'bore': 'bore',
                'stroke': 'stroke',
                'compression-ratio': 'compression-ratio',
                'horsepower': 'horsepower',
                'peak-rpm': 'peak-rpm',
                'city-mpg': 'city-mpg',
                'highway-mpg': 'highway-mpg',
                'price': 'price'
            }

            df = df.rename(columns=csv_to_pg_column_map)
            df = df[pg_columns]

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




