import os
import json
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
from common import to_bool
def generate_moxiworks_office_monthly_report():
    conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
        'db_user') + " password=" + os.getenv('db_password'))
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    conn.commit()
    # office report generation
    cur.execute("select office_name,officepublickey,office_addressline1,office_city,office_state,office_zip,office_phone, office_website,companypublickey from moxiworks.monthly_office_sfdc_data_store except select office_name,officepublickey,office_addressline1,office_city,office_state,office_zip,office_phone, office_website,companypublickey from moxiworks.moxiworks_office")
    office_monthly_diffrence_array = cur.fetchall()
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/office_monthly_report'
    full_path = directory + store
    with open(full_path + "/moxiworks_office_monthly_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
        fieldnames = ['officepublickey', 'middleware_data', 'sfdc_data']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for office_monthly_diffrence in office_monthly_diffrence_array:
            cur.execute('select office_name,officepublickey,office_addressline1,office_city,office_state,office_zip,office_phone, office_website,companypublickey from moxiworks.moxiworks_office where officepublickey = %s',(office_monthly_diffrence['officepublickey'],))
            office_middleware_data = cur.fetchone()
            
            cur.execute('select office_name,officepublickey,office_addressline1,office_city,office_state,office_zip,office_phone, office_website,companypublickey from moxiworks.monthly_office_sfdc_data_store where officepublickey = %s',(office_monthly_diffrence['officepublickey'],))
            office_sfdc_data = cur.fetchone()
            if not office_middleware_data:
                continue
            if not office_sfdc_data:
                continue
            dict_middleware = {}
            dict_sfdc = {}
            diffrence = 0
            if office_sfdc_data['office_name'] == None:
                office_sfdc_data['office_name'] = ''
            if office_middleware_data['office_name'] != office_sfdc_data['office_name']:
                dict_middleware['office_name']=office_middleware_data['office_name']
                dict_sfdc['office_name']=office_sfdc_data['office_name']
                diffrence = 1
            if office_sfdc_data['officepublickey'] == None:
                office_sfdc_data['officepublickey'] = ''
            if office_middleware_data['officepublickey'] != office_sfdc_data['officepublickey']:
                dict_middleware['officepublickey']=office_middleware_data['officepublickey']
                dict_sfdc['officepublickey']=office_sfdc_data['officepublickey']
                diffrence = 1
            if office_sfdc_data['office_addressline1'] == None:
                office_sfdc_data['office_addressline1'] = ''
            if office_middleware_data['office_addressline1'].strip() != office_sfdc_data['office_addressline1']:
                dict_middleware['office_addressline1']=office_middleware_data['office_addressline1'].strip()
                dict_sfdc['office_addressline1']=office_sfdc_data['office_addressline1']
                diffrence = 1
            if office_sfdc_data['office_city'] == None:
                office_sfdc_data['office_city'] = ''
            if office_middleware_data['office_city'] != office_sfdc_data['office_city']:
                dict_middleware['office_city']=office_middleware_data['office_city']
                dict_sfdc['office_city']=office_sfdc_data['office_city']
                diffrence = 1
            if office_sfdc_data['office_state'] == None:
                office_sfdc_data['office_state'] = ''
            if office_middleware_data['office_state'] != office_sfdc_data['office_state']:
                dict_middleware['office_state']=office_middleware_data['office_state']
                dict_sfdc['office_state']=office_sfdc_data['office_state']
                diffrence = 1
            if office_sfdc_data['office_zip'] == None:
                office_sfdc_data['office_zip'] = ''
            if office_middleware_data['office_zip'] != office_sfdc_data['office_zip']:
                dict_middleware['office_zip']=office_middleware_data['office_zip']
                dict_sfdc['office_zip']=office_sfdc_data['office_zip']
                diffrence = 1

            if office_sfdc_data['office_phone'] == None:
                temp_office_mobile_number_sfdc = ''
            else:
                temp_office_mobile_number_sfdc = str(office_sfdc_data['office_phone']).replace("(",'')
                temp_office_mobile_number_sfdc = temp_office_mobile_number_sfdc.replace(")",'')
                temp_office_mobile_number_sfdc = temp_office_mobile_number_sfdc.replace(" ",'')
                temp_office_mobile_number_sfdc = temp_office_mobile_number_sfdc.replace("-",'')
            temp_office_mobile_number_middleware = str(office_middleware_data['office_phone']).replace('-','')
            if temp_office_mobile_number_middleware != temp_office_mobile_number_sfdc:
                dict_middleware['office_phone']=office_middleware_data['office_phone']
                dict_sfdc['office_phone']=office_sfdc_data['office_phone']
                diffrence = 1
            
            if office_sfdc_data['office_website'] == None:
                office_sfdc_data['office_website'] = ''
            if office_middleware_data['office_website'] != office_sfdc_data['office_website']:
                dict_middleware['office_website']=office_middleware_data['office_website']
                dict_sfdc['office_website']=office_sfdc_data['office_website']
                diffrence = 1
            if office_sfdc_data['companypublickey'] == None:
                office_sfdc_data['companypublickey'] = ''
            if office_middleware_data['companypublickey'] != office_sfdc_data['companypublickey']:
                dict_middleware['companypublickey']=office_middleware_data['companypublickey']
                dict_sfdc['companypublickey']=office_sfdc_data['companypublickey']
                diffrence = 1
            
            if diffrence == 1:
                writer.writerow({'officepublickey':office_monthly_diffrence['officepublickey'], 'middleware_data':dict_middleware, 'sfdc_data':dict_sfdc})
