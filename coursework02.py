import operator
import random
import os
import sqlite3
import sys
import re

# the filename of the database
filename_db = './tasks.db'
# sql of selecting all tasks
sql_select_tasks = 'SELECT * FROM tasks'


# definition of Clock to control the time
# @time     the present time
# @next_event_time      the time of next event
class Clock():
    def __init__(self, time, next_event_time):
        self.time = time
        self.next_event_time = next_event_time


# definition of Processor to execute tasks
# @id       the id of processor
# @state    the working state of processor
#           value: 0-leisure
#           value: 1-working
# @task     the task being processed
# @finish_time  the end time of task is finished
class Processor():
    def __init__(self, id, state, task, finish_time):
        self.id = id
        self.state = state
        self.task = task
        self.finish_time = finish_time


# definition of Task
# @id           the id of task
# @arrival      the arrival time of task will be processed
# @duration     the time of task will last
class Task():
    def __init__(self, id, arrival, duration):
        self.id = id
        self.arrival = arrival
        self.duration = duration


# get task list from database
def get_task_list():
    # connect to the database and fetch all the records
    db = connect(filename_db)
    cursor = db.cursor()
    cursor.execute(sql_select_tasks)
    db.commit()
    records = cursor.fetchall()

    # create list and append all tasks into list
    task_list = []
    for item in records:
        task = Task(item[1], item[2], item[3])
        task_list.append(task)

    # close the cursor
    cursor.close()

    return task_list


# connect to the database from filename
def connect(filename):
    # if database file not exists, print the warning and exit
    file_exists = os.path.exists(filename)
    if not file_exists:
        print('Fail. \nThe database does not exist. \nPlease check.')
        sys.exit()

    # connect to the database and return db
    db = sqlite3.connect(filename)
    return db


# check the id of the task to satisfy at least 3 rules
def check_task_id(task):
    id = task.id
    number = re.compile(r'[0-9]')
    lowercase = re.compile(r'[a-z]')
    uppercase = re.compile(r'[A-Z]')
    symbol = re.compile(r'[@_#*\-&]')

    flag = 0
    if number.search(id):
        flag += 1
    if lowercase.search(id):
        flag += 1
    if uppercase.search(id):
        flag += 1
    if symbol.search(id):
        flag += 1

    if flag >= 3:
        return True
    else:
        return False


# initialize the task list
def init_task_list():
    task_list = get_task_list()
    attr = operator.attrgetter('arrival')
    task_list.sort(key=attr)
    return task_list


# initialize the processor list
def init_pro_list():
    p0 = Processor(1, 0, None, 0)
    p1 = Processor(2, 0, None, 0)
    p2 = Processor(3, 0, None, 0)
    pro_list = [p0, p1, p2]
    return pro_list


# release the processor
def release_pro(pro):
    pro.task = None
    pro.state = 0
    pro.finish_time = 0


# check the processor list and get the leisure processor
# if there are free processors, choose a processor randomly
# if there is no free processor, return None
def get_free_pro(pro_list):
    free_pro_list = []
    for pro in pro_list:
        if pro.state == 0:
            free_pro_list.append(pro)
    if len(free_pro_list) > 0:
        return random.choice(free_pro_list)
    else:
        return None


# initialize the task list
task_list = init_task_list()

# initialize the processor list
pro_list = init_pro_list()

# initialize the waiting list
wait_list = []

# initialize the clock
clock = Clock(0, 0)

# start the simulation
print('** SYSTEM INITIALISED **')

while True:

    # update the clock
    clock.time = clock.next_event_time

    # check the event of task completing
    for pro in pro_list:
        if pro.task is not None and clock.time == pro.finish_time:
            # task completed
            print('** {} : Task {} completed.'.format(clock.time, pro.task.id))

            # release the processor
            release_pro(pro)

    # get the leisure processor
    free_pro = get_free_pro(pro_list)

    # check the event of task arriving
    if len(task_list) > 0 and clock.time == task_list[0].arrival:
        task = task_list.pop(0)
        print('** {} : Task {} with duration {} enters the system.'.format(clock.time, task.id, task.duration))

        # if task id satisfies the rules, join the waiting list
        if check_task_id(task):
            wait_list.append(task)
            print('** Task {} accepted.'.format(task.id))

            # if there is no leisure processor, task on hold
            if free_pro is None:
                print('** Task {} on hold.'.format(task.id))
        else:
            # discard the task and print
            print('** Task {} unfeasible and discarded'.format(task.id))

    # assign the task to leisure processor
    if free_pro is not None and len(wait_list) > 0:
        task = wait_list.pop(0)
        free_pro.state = 1
        free_pro.task = task
        free_pro.finish_time = clock.time + task.duration
        print('** {} : Task {} assigned to processor {}.'.format(clock.time, task.id, free_pro.id))

    # initialize the event time list
    event_time_list = []

    # check the event time of task arrival
    if len(task_list) > 0:
        event_time_list.append(task_list[0].arrival)

    # check the event time of task finish
    for pro in pro_list:
        if pro.state == 1 and pro.task is not None:
            event_time_list.append(pro.finish_time)

    # update the event time
    if len(event_time_list) == 0:
        print('** {} : SIMULATION COMPLETED. **'.format(clock.time))
        break
    else:
        event_time_list.sort()
        clock.next_event_time = event_time_list[0]
