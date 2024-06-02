import psycopg2

host="172.18.0.1"
database="Data-project"
user="postgres"
password="Cristiano15+"

def connect_to_db(host,database,user,password):
    try:
        connection=psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )

        cursor=connection.cursor()

        cursor.execute("SELECT version()")

        db_version=cursor.fetchone()

        print(f"Connected to {db_version}")

        cursor.close()
        connection.close()

    except Exception as error:
        print(f"Error coonecting to database: {error}")

connect_to_db(host,database,user,password)


