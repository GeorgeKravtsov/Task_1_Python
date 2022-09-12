import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine



def create_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            password = user_password
        )
        print("Connection to MySQL successful")
    except Error as e:
        print(f"The error '{e}' occured")

    return connection

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            password = user_password,
            database = db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occured")
    return connection

def create_database(connection, db_name):
    query = "CREATE DATABASE IF NOT EXISTS" + ' ' + db_name
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occured")

def create_table(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Table created successfully")
    except Error as e:
        print(f"The error '{e}' occured")

host = "localhost"
username = "root"
password = "***"


connection = create_connection(host, username, password)
db_name = "task_1"
create_database(connection, db_name)

connection = create_db_connection(host, username, password, db_name)

query = "CREATE TABLE IF NOT EXISTS students( \
            birthday DATE, \
            id INT(5) PRIMARY KEY NOT NULL, \
            name CHAR(27), \
            room INT(5), \
            sex CHAR(1) \
    )"
create_table(connection, query)

engine = create_engine(f"mysql+pymysql://{username}:{password}@localhost:3306/task_1")

students_df = pd.read_json('students.json')
students_df.birthday = pd.to_datetime(students_df.birthday)
students_df.to_sql(name='students',
                    con=engine,
                    index=False,
                    if_exists='append'
                )

query = "CREATE TABLE IF NOT EXISTS rooms( \
            id INT(5) PRIMARY KEY NOT NULL, \
            name INT(5) NOT NULL, \
            FOREIGN KEY (name) REFERENCES students(room) \
            )"

create_table(connection, query)
rooms_df = pd.read_json('rooms.json')
rooms_df.name = rooms_df.name.apply(lambda x: x[-1])
rooms_df.name = pd.to_numeric(rooms_df.name)
rooms_df.to_sql(name='rooms',
                    con=engine,
                    index=False,
                    if_exists='append'
                )

query = "SELECT room, COUNT(id) AS number_of_sudents \
        FROM students \
        GROUP BY room"

pd.read_sql(query, engine).to_json('number_of_students_in_room.json')
