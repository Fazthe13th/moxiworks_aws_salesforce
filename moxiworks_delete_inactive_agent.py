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
from moxiworks_api import moxiworks_inactive_agent_deletion_start
from moxiworks_api import moxiworks_inactive_process_deletion_process_status
from common import send_boomi_process_failed_email
from common import to_bool
total_data = 0
total_delta = 0
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
conn.commit()
cur.execute("select * from moxiworks.moxiworks_contact where date_deactivated is not null and agent_active ='false' and isdeleted = 'false' and sfdc_contact_id != ''")
inactive_agent_array = cur.fetchall()
# print(inactive_agent_array)
# sys.exit("Moxiworks import process has failed during boomi process execution")
for inactive in inactive_agent_array:
    try:
        total_data = total_data + 1
        cur.execute('INSERT INTO moxiworks.agent_candidate_for_delete (sfdc_contact_id) VALUES (%s)',(inactive['sfdc_contact_id'],))
        print('Data inserted in agent_candidate_for_delete....')
        conn.commit()
        print('Current data processing ' + str(total_data))
    except Exception as e:
        print("type error: " + str(e))
        conn.rollback()
        break
print('Total data in deletion table ' + str(total_data))
num = 0
while True:
    num = num + 10
    cur.execute('SELECT count(*) from moxiworks.agent_candidate_for_delete')
    count_array = cur.fetchone()
    count = count_array[0]
    # if count <= 0:
    #     break
    if num >= 30:
        break
    cur.execute('SELECT * from moxiworks.agent_candidate_for_delete order by id limit 10')
    get_delta_data = cur.fetchall()
    for delta in get_delta_data:
        try:
            cur.execute('INSERT INTO moxiworks.agent_candidate_for_delete_delta (sfdc_contact_id) VALUES (%s)',(delta['sfdc_contact_id'],))
            print('Data inserted in agent_candidate_for_delete_delta....')
            conn.commit()
            total_delta = total_delta + 1
            print('Total data processed ' + str(total_delta))
        except Exception as e:
            print("type error: " + str(e))
            conn.rollback()
            break
            
    moxiworks_inactive_agent_deletion_start()
    print("Boomi inactive agent deletion process starting...." + str(datetime.datetime.now().time()))
    while True:
        time.sleep(180)
        process_status = moxiworks_inactive_process_deletion_process_status()
        process_status = str(process_status)
        if process_status == 'INPROCESS':
            print("Boomi Moxiworks inactive agent deletion process is running...."+ str(datetime.datetime.now().time()))
            continue
        if process_status == 'COMPLETE' or process_status == 'COMPLETE_WARN':
            print("Boomi Moxiworks inactive agent deletion process is finished...."+ str(datetime.datetime.now().time()))
            cur.execute("TRUNCATE TABLE moxiworks.agent_candidate_for_delete_delta")
            conn.commit()
            break
        if process_status == 'ERROR':
            print('Boomi Moxiworks inactive agent deletion process has faced an error......')
            send_boomi_process_failed_email('Moxiworks inactive agent deletion process')
            sys.exit("Moxiworks import process has failed during boomi process execution")
            break
    for delta in get_delta_data:
        try:
            cur.execute("UPDATE moxiworks.moxiworks_contact SET isdeleted = %s where sfdc_contact_id = %s",(to_bool('t'),delta['sfdc_contact_id']))
            cur.execute('Delete from moxiworks.agent_candidate_for_delete where sfdc_contact_id = %s',(delta['sfdc_contact_id'],))
            print('Data deleted from agent_candidate_for_delete....')
            conn.commit()
        except Exception as e:
            print("type error: " + str(e))
            conn.rollback()
            break
    print('Deleted 200 records')
print('Deletation process is complete. Deteted '+str(total_delta)+ ' records')
    
