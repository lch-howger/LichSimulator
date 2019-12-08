import sqlite3
import os
import string
import random
import math

# the filename of the database
filename_db = './tasks.db'
# sql of creating table(num,id,arrival,duration)
sql_create_table = 'CREATE TABLE tasks (num INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, id TEXT NOT NULL, arrival REAL NOT NULL, duration INTEGER NOT NULL)'
# sql of insert task into table
sql_insert_task = "INSERT INTO tasks VALUES(NULL,'{}',{},{})"
# sql of selecting all tasks
sql_select_tasks = 'SELECT * FROM tasks'


# get single random character
def get_random_char():
    s = string.digits + string.ascii_letters + "@_#*-&"
    choice = random.choice(s)
    return choice


# get task id from six random character
def get_task_id():
    task_id = ""
    for i in range(0, 6):
        task_id = task_id + get_random_char()
    return task_id


# get task arrival from random
def get_task_arrival():
    rand = random.random() * 100
    return rand


# get task duration from ceil of expovariate
def get_task_duration():
    expovariate = random.expovariate(1)
    duration = math.ceil(expovariate)
    return duration


# connect to the database from filename
# recreate database and table each time
def connect_db(filename):
    # if database file exists, remove the file
    file_exists = os.path.exists(filename)
    if file_exists:
        os.remove(filename)

    # connect to the database and get the cursor
    db = sqlite3.connect(filename)
    cursor = db.cursor()

    # create database and table
    cursor.execute(sql_create_table)
    db.commit()
    cursor.close()
    return db


# create 100 tasks and insert into the table
def create_tasks(db):
    for i in range(100):
        task_id = get_task_id()
        task_arrival = get_task_arrival()
        task_duration = get_task_duration()
        add_task(db, task_id, task_arrival, task_duration)


# insert single task into the table with id, arrival and duration
def add_task(db, id, arrival, duration):
    cursor = db.cursor()
    cursor.execute(sql_insert_task.format(id, arrival, duration))
    db.commit()
    cursor.close()


# select all the tasks and print
def select_tasks(db):
    # get cursor to fetch all records
    cursor = db.cursor()
    cursor.execute(sql_select_tasks)
    db.commit()
    records = cursor.fetchall()

    # print all the records in pretty format
    print("{:>3} | {:>6} | {:>19} | {:>2}".format('num', 'id', 'arrival', 'duration'))
    print("{0:->3} | {0:->6} | {0:->19} | {0:->3}".format(''))
    for item in records:
        print("{:>3} | {:>6} | {:>19} | {:>2}".format(item[0], item[1], item[2], item[3]))

    # close the cursor
    cursor.close()


# connect to the database
db = connect_db(filename_db)

# create 100 tasks and insert into the table
create_tasks(db)

# select tasks and print
select_tasks(db)

# close the database
db.close()
