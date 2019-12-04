import operator
import random
import os
import sqlite3
import sys

# the filename of the database
filename_db = '../coursework/tasks.db'
# sql of selecting all tasks
sql_select_tasks = 'SELECT * FROM tasks'


# definition of Clock to control the time
# @time     the present time
# @nect_event_time      the time of next event
class Clock():
    def __init__(self, time, next_event_time):
        self.time = time
        self.next_event_time = next_event_time


# definition of Processor to execute tasks
# @id       the id of processor
# @state    the working state of processor, 0-leisure, 1-working
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


def print_wait_list(wait_list):
    s = ''
    for i in wait_list:
        s = s + str(i.id + ",")
    print('等待队列是:' + s)


def print_processors(pro_list):
    for pro in pro_list:
        if pro.state == 0:
            print('处理器{}空闲...'.format(pro.id))
        else:
            task = pro.task
            print('处理器{}正在工作,任务为{},arrival为{},duration为{},任务完成时间为:{}'
                  .format(pro.id, task.id, task.arrival, task.duration, pro.finish_time))


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


# 创建task
task_list = get_task_list()

attr = operator.attrgetter('arrival')
task_list.sort(key=attr)

# 创建处理器
p0 = Processor(0, 0, None, 0)
p1 = Processor(1, 0, None, 0)
p2 = Processor(2, 0, None, 0)
pro_list = [p0, p1, p2]

# 创建等待队列
wait_list = []

# 创建时钟
clock = Clock(0, 0)

while True:
    clock.time = clock.next_event_time
    print('现在的时间点是: {}, 下一个事件的时间点是: ???'.format(clock.time))

    # 检查添加任务
    if len(task_list) > 0:
        task = task_list[0]
        # 加入队列
        if clock.time == task.arrival:
            task = task_list.pop(0)
            wait_list.append(task)

    # 检查任务完成
    for pro in pro_list:
        if pro.task is not None:
            if clock.time == pro.finish_time:
                # 完成任务
                task = pro.task
                print('任务{}完成了,arrival为:{},duration为:{},任务完成时间为:{}'.format(task.id, task.arrival, task.duration,
                                                                           pro.finish_time))
                # 释放处理器
                pro.task = None
                pro.state = 0
                pro.finish_time = 0

    # 分配任务
    free_pro_list = []
    for pro in pro_list:
        if pro.state == 0:
            free_pro_list.append(pro)
    if len(free_pro_list) > 0 and len(wait_list) > 0:
        time = clock.time
        task = wait_list.pop(0)
        freePro = random.choice(free_pro_list)
        freePro.state = 1
        freePro.task = task
        freePro.finish_time = time + task.duration
        print('任务{}被分配到了处理器{},arrival是{},duration是{}'.format(task.id, freePro.id, task.arrival, task.duration))
        print('任务{}从现在时间点{}开始执行,完成时间为:{}'.format(task.id, time, freePro.finish_time))

    # 打印处理器和等待队列
    print_processors(pro_list)
    print_wait_list(wait_list)

    # ===============================

    eventTime = []

    # 检查添加任务事件
    if len(task_list) > 0:
        task = task_list[0]
        addEvent = task.arrival
        eventTime.append(addEvent)

    # 检查任务结束事件
    for pro in pro_list:
        if pro.task is not None:
            finishEvent = pro.finish_time
            eventTime.append(finishEvent)

    # 整理事件时间集合
    if len(eventTime) == 0:
        clock.next_event_time = -1
        print('现在的时间点是: {}, 下一个事件的时间点是: {}'.format(clock.time, clock.next_event_time))
        print('结束了')
        break
    else:
        eventTime.sort()
        clock.next_event_time = eventTime[0]

    print('现在的时间点是: {}, 下一个事件的时间点是: {}'.format(clock.time, clock.next_event_time))
    print('==============================')
