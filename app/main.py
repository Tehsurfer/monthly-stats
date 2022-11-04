from flask import Flask, render_template
from app.config import Config
import threading
import time
import sys
import datetime
import requests
from flask import jsonify
import json
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.background import BackgroundScheduler

import boto3
import requests

PENNSIEVE_URL = "https://api.pennsieve.io"
organization = 'N:organization:618e8dd9-f8d2-4dc4-9abb-c6aaab2e78a0'
api_key = Config.PENNSIEVE_API_TOKEN
api_secret = Config.PENNSIEVE_API_SECRET

r = requests.get(f"{PENNSIEVE_URL}/authentication/cognito-config")
r.raise_for_status()

cognito_app_client_id = r.json()["tokenPool"]["appClientId"]
cognito_region = r.json()["region"]

cognito_idp_client = boto3.client(
    "cognito-idp",
    region_name=cognito_region,
    aws_access_key_id="",
    aws_secret_access_key="",
)

login_response = cognito_idp_client.initiate_auth(
    AuthFlow="USER_PASSWORD_AUTH",
    AuthParameters={"USERNAME": api_key, "PASSWORD": api_secret},
    ClientId=cognito_app_client_id,
)

api_key = login_response["AuthenticationResult"]["AccessToken"]

app = Flask(__name__, static_url_path='')
test_result = 'failed. Thread has not updated this text to pass'
scheduleResult = ''

@app.before_first_request
def execute_this():
    # Start the scheduler
    sched = BackgroundScheduler()
    sched.start()
    job = sched.add_job(logTimeSinceStart, 'interval', minutes=1)

    # start the test thread
    threading.Thread(target=thread_testy).start()



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

@app.route('/metrics/')
def metrics():
    return jsonify(getMonthlyStats())

@app.route('/users/')
def users():
    userDownloads = {}
    try:
        userDownloads = getOrcidStats()
    except Exception as e:
        print('ERROR: ', e)
    return app.response_class(json.dumps(userDownloads), mimetype='application/json')

@app.route('/emails/')
def emails():
    user_stats = getOrcidStats()
    global api_key, organization
    r = requests.get(f"{PENNSIEVE_URL}/organizations/{organization}/members", headers={"Authorization": f"Bearer {api_key}"})
    r.raise_for_status()
    user_details = r.json()
    for user in user_details:
        if 'orcid' in user.keys():
            if user['orcid']['orcid'] in user_stats.keys():
                user_stats[user['orcid']['orcid']]['email'] = user['email']


    return app.response_class(json.dumps(user_stats), mimetype='application/json')

def getOrcidStats():
    downloads = getMonthlyStats()
    users = {}
    r = requests.get('https://api.pennsieve.io/discover/datasets',{
        'limit': 1000,
        'ids': [d['datasetId'] for d in downloads]
    })
    datasets = r.json()['datasets']
    for dataset in datasets:
        downloadInfo = [d for d in downloads if dataset['id'] == d['datasetId']]
        for contributor in dataset['contributors']:
            if contributor['orcid'] not in users.keys():
                users[contributor['orcid']] = {}
                users[contributor['orcid']]['datasets'] = downloadInfo
            else:
                users[contributor['orcid']]['datasets'] += downloadInfo
    return users

def thread_testy():
    time.sleep(10)
    print('Thread is printing to console')
    sys.stdout.flush()
    global test_result
    test_result = 'passed'
    return

def logTimeSinceStart():
    global scheduleResult
    scheduleResult = 'Log from schedule made at ' + datetime.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S %Z")+ ' <br>' + scheduleResult
    return

def getMonthlyStats():
    start_date = datetime.datetime.now() - relativedelta(months=1)
    formatted_start_date = start_date.strftime('%Y-%m-%d')

    end_date = datetime.datetime.now()
    formatted_end_date = end_date.strftime('%Y-%m-%d')
    r = requests.get('https://api.pennsieve.io/discover/metrics/dataset/downloads/summary', {
        'startDate': formatted_start_date,
        'endDate': formatted_end_date
    })
    return r.json()


def start_app():
    threading.Thread(target=app.run).start()

if __name__ == "__main__":
    start_app()