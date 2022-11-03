from flask import Flask, render_template
import threading
import time
import sys
import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_url_path='')
test_result = 'failed'
scheduleResult = ''

@app.before_first_request
def execute_this():
    threading.Thread(target=thread_testy).start()
    # Start the scheduler
    sched = BackgroundScheduler()
    sched.start()
    job = sched.add_job(logTimeSinceStart, 'interval', minutes=1)

@app.route('/')
def index():
    return 'Hello! Server is running successfully! Try navigating to /thread-test/ to test the thread or to /schedule/ to see the scheduler'

@app.route('/thread-test/')
@app.route('/thread-test')
def thread_test():
    global test_result
    return test_result

@app.route('/schedule/')
@app.route('/schedule')
def schedule_test():
    global scheduleResult
    return scheduleResult

def thread_testy():
    time.sleep(10)
    print('Thread is printing to console')
    sys.stdout.flush()
    global test_result
    test_result = 'passed'
    return

def logTimeSinceStart():
    global scheduleResult
    scheduleResult = 'Log from schedule made at ' + datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y") + '\n' + scheduleResult
    return

def start_app():
    threading.Thread(target=app.run).start()

if __name__ == "__main__":
    start_app()