import os
import csv
import datetime
import sys
import psycopg2
import psycopg2.extras
import os.path
from os import path
from pathlib import Path
from src.aws.storage import S3
from dotenv import load_dotenv, find_dotenv
import glob
import uuid
import csvdiff
from validate_email import validate_email
import datetime
import http.client
import time
from common import send_boomi_process_failed_email
from common import send_monthly_dbsync_report
from moxiworks_api import moxiworks_monthly_report_reverse_sync_start
from moxiworks_api import moxiworks_monthly_report_reverse_sync_status
from moxiworks_company_monthly_report import generate_moxiworks_company_monthly_report
from moxiworks_office_monthly_report import generate_moxiworks_office_monthly_report
from moxiworks_agent_monthly_report import generate_moxiworks_agent_monthly_report
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
        'db_user') + " password=" + os.getenv('db_password'))
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
conn.commit()
moxiworks_monthly_report_reverse_sync_start()
print("Boomi monthly report reverse sync process starting...." + str(datetime.datetime.now().time()))
while True:
    time.sleep(180)
    process_status = moxiworks_monthly_report_reverse_sync_status()
    process_status = str(process_status)
    if process_status == 'INPROCESS':
        print("Boomi monthly report reverse sync process is running...."+ str(datetime.datetime.now().time()))
        continue
    if process_status == 'COMPLETE' or process_status == 'COMPLETE_WARN':
        print("Boomi monthly report reverse sync process is finished...."+ str(datetime.datetime.now().time()))
        company_status = 'finished'
        break
    if process_status == 'ERROR':
        print('Boomi monthly report reverse sync process has faced an error......')
        send_boomi_process_failed_email('Moxiworks_montly_reverse_sync_prod')
        sys.exit("Moxiworks import process has failed during boomi process execution")
        break
print('generating company monthly report. Please wait..')
generate_moxiworks_company_monthly_report()
print('generating office monthly report. Please wait..')
generate_moxiworks_office_monthly_report()
print('generating agent monthly report. Please wait..')
generate_moxiworks_agent_monthly_report()

print('Sending monthly report to email..')
#Send the reports to moxiworks
send_monthly_dbsync_report()

cur.execute("TRUNCATE TABLE moxiworks.monthly_company_sfdc_data_store")
conn.commit()
cur.execute("TRUNCATE TABLE moxiworks.monthly_office_sfdc_data_store")
conn.commit()
cur.execute("TRUNCATE TABLE moxiworks.monthly_agent_sfdc_data_store")
conn.commit()
print('Monthly report generation process is done..')