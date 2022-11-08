from flask import Flask, render_template
from app.config import Config
import threading
import time
import sys
import datetime
import requests
from flask import jsonify
import json
import sendgrid
from sendgrid.helpers.mail import Content, Email, Mail, To, Attachment, FileName, FileType, Disposition, FileContent, Asm, GroupId, GroupsToDisplay
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.background import BackgroundScheduler

import boto3
import requests


sg_client = sendgrid.SendGridAPIClient(api_key=Config.SENDGRID_API_KEY)


PENNSIEVE_URL = "https://api.pennsieve.io"
UNSUBSCRIBE_GROUP = 112703
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
scheduleResult = ''

@app.before_first_request
def execute_this():
    runSchedule()
    # Start the scheduler
    sched = BackgroundScheduler()
    sched.start()
    job = sched.add_job(runSchedule, 'interval', hours=12)


@app.route('/')
def index():
    return 'Hello! Server is running successfully! Try navigating to /thread-test/ to test the thread or to /schedule/ to see the scheduler'


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
    return app.response_class(get_emails(), mimetype='application/json')

# Places emails on the an object with orcid_ids
def get_emails():
    user_stats = getOrcidStats()
    global api_key, organization
    r = requests.get(f"{PENNSIEVE_URL}/organizations/{organization}/members", headers={"Authorization": f"Bearer {api_key}"})
    r.raise_for_status()
    user_details = r.json()
    for user in user_details:
        if 'orcid' in user.keys():
            orcid_id = user['orcid']['orcid']
            if orcid_id in user_stats.keys():
                user_stats[orcid_id]['email'] = user['email']


    return user_stats

# Find orcid ids associated with datasets
def getOrcidStats():
    downloads = getMonthlyStats()
    users = {}

    # send a request asking for info on the datsets with downloads
    r = requests.get('https://api.pennsieve.io/discover/datasets',{
        'limit': 1000,
        'ids': [d['datasetId'] for d in downloads]
    })
    datasets = r.json()['datasets']
    for dataset in datasets:
        downloadInfo = [d for d in downloads if dataset['id'] == d['datasetId']]
        for contributor in dataset['contributors']:
            orcid_id = contributor['orcid']
            if orcid_id not in users.keys():
                users[orcid_id] = {}
                users[orcid_id]['datasets'] = downloadInfo
            else:
                users[orcid_id]['datasets'] += downloadInfo
    return users

# Use to see when scheduler has run
def logTimeSinceStart():
    global scheduleResult
    scheduleResult = 'Log from schedule made at ' + datetime.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S %Z")+ ' <br>' + scheduleResult
    return

# Get 1 month's metrics from Pennsieve
def getMonthlyStats():
    start_date = datetime.datetime.now() - relativedelta(hours=12)
    formatted_start_date = start_date.strftime('%Y-%m-%d')

    end_date = datetime.datetime.now()
    formatted_end_date = end_date.strftime('%Y-%m-%d')
    r = requests.get('https://api.pennsieve.io/discover/metrics/dataset/downloads/summary', {
        'startDate': formatted_start_date,
        'endDate': formatted_end_date
    })
    return r.json()

def runSchedule():
    logTimeSinceStart()
    sendgrid_email(json.dumps(get_emails()))

# Send email to myself for testing
@app.route('/send-email/')
def sendgrid_email(content="<b>Hello there! There should be download stats here, I don't know what happened!</b>"):
        mail = Mail(
            Email('jessekhora@gmail.com'),
            To('nametaken47@gmail.com'),
            'Download statistics',
            Content("text/html", content),

        )
        mail.asm = Asm(GroupId(UNSUBSCRIBE_GROUP), GroupsToDisplay([UNSUBSCRIBE_GROUP]))
        response = sg_client.send(mail)
        return jsonify(response.status_code)

def start_app():
    threading.Thread(target=app.run).start()

if __name__ == "__main__":
    start_app()