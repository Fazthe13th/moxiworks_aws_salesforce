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
from moxiworks_api import moxiworks_company_import_boomi_process_start
from moxiworks_api import moxiworks_office_import_boomi_process_start
from moxiworks_api import moxiworks_agent_import_boomi_process_start
from moxiworks_api import moxiworks_company_boomi_process_status
from moxiworks_api import moxiworks_office_boomi_process_status
from moxiworks_api import moxiworks_agent_boomi_process_status
from moxiworks_api import moxiworks_agent_sfdc_id_sync
from moxiworks_company_daily_import import import_company_daily_data
from moxiworks_office_daily_import import import_office_daily_data
from moxiworks_agent_daily_import import import_agent_daily_data

#Created a object of the class
import_company_daily_data = import_company_daily_data()
# Created a object of the class
office_daily_obj = import_office_daily_data()
#Created a object of the class
import_agent_daily_data = import_agent_daily_data()
#create the connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
process_status = ''
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
conn.commit()
#call the import_company_inital_data function

#Company Import Block
company_status = import_company_daily_data.import_company_data(conn)
#check if new file exists
if company_status != 'no_new_file':
    moxiworks_company_import_boomi_process_start()
    print("Boomi company import process starting...." + str(datetime.datetime.now().time()))
    while True:
        time.sleep(120)
        process_status = moxiworks_company_boomi_process_status()
        process_status = str(process_status)
        if process_status == 'INPROCESS':
            print("Boomi company import process is running...."+ str(datetime.datetime.now().time()))
            continue
        if process_status == 'COMPLETE' or process_status == 'COMPLETE_WARN':
            print("Boomi company import process is finished...."+ str(datetime.datetime.now().time()))
            company_status = 'finished'
            break
        if process_status == 'ERROR':
            print('Boomi company import process has faced an error......')
            send_boomi_process_failed_email('Moxiworks_Company_Daily_DBSync_Prod')
            sys.exit("Moxiworks import process has failed during boomi process execution")
            break
#company import block end

#office import block 
if company_status == 'finished' or company_status == 'no_new_file':
    office_status = office_daily_obj.import_office_data(conn)
    #check if new file exists
    if office_status != 'no_new_file':
        cur.execute("TRUNCATE TABLE moxiworks.sfdc_company_temp_table")
        conn.commit()
        moxiworks_office_import_boomi_process_start()
        cur.execute("TRUNCATE TABLE moxiworks.moxiworks_account_delta")
        conn.commit()
        print("Boomi office import process starting...." + str(datetime.datetime.now().time()))
        while True:
            time.sleep(120)
            process_status = moxiworks_office_boomi_process_status()
            process_status = str(process_status)
            if process_status == 'INPROCESS':
                print("Boomi office import process is running...."+ str(datetime.datetime.now().time()))
                continue
            if process_status == 'COMPLETE' or process_status == 'COMPLETE_WARN':
                print("Boomi office import process is finished...."+ str(datetime.datetime.now().time()))
                office_status = 'finished'
                break
            if process_status == 'ERROR':
                print('Boomi office import process has faced an error......')
                send_boomi_process_failed_email('Moxiworks_Office_Daily_DBSync_Prod')
                sys.exit("Moxiworks import process has failed during boomi process execution")
                break
#office import block end

#Agent import block
if office_status == 'finished' or office_status == 'no_new_file':
    agent_status = import_agent_daily_data.import_agent_data(conn)
    #check if new file exists
    if agent_status != 'no_new_file':
        cur.execute("TRUNCATE TABLE moxiworks.sfdc_company_temp_table")
        conn.commit()
        moxiworks_agent_import_boomi_process_start()
        cur.execute("TRUNCATE TABLE moxiworks.moxiworks_office_delta")
        conn.commit()
        print("Boomi agent import process starting...." + str(datetime.datetime.now().time()))
        while True:
            time.sleep(120)
            process_status = moxiworks_agent_boomi_process_status()
            process_status = str(process_status)
            if process_status == 'INPROCESS':
                print("Boomi agent import process is running...."+ str(datetime.datetime.now().time()))
                continue
            if process_status == 'COMPLETE' or process_status == 'COMPLETE_WARN':
                print("Boomi agent import process is finished...."+ str(datetime.datetime.now().time()))
                cur.execute("TRUNCATE TABLE moxiworks.moxiworks_contact_delta")
                conn.commit()
                #agent sync process run here
                moxiworks_agent_sfdc_id_sync()
                break
            if process_status == 'ERROR':
                print('Boomi agent import process has faced an error......')
                send_boomi_process_failed_email('Moxiworks_Agent_Daily_DBSync_Prod')
                sys.exit("Moxiworks import process has failed during boomi process execution")
                break
#agent import block end
print("Moxiworks import process has finished")

