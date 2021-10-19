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
def generate_moxiworks_company_monthly_report():
    conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
        'db_user') + " password=" + os.getenv('db_password'))
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    conn.commit()
    # company report generation
    cur.execute("select company_name,companypublickey,company_addressline1,company_city,company_state,company_zipcode,company_phone, company_webpage,company_moxi_hub::boolean,company_moxi_dms::boolean,company_moxi_engage::boolean,company_moxi_present::boolean, company_moxi_websites::boolean,company_moxi_talent::boolean,company_moxi_insights::boolean,show_in_product_marketing::boolean, advertise_your_listing::boolean,ayl_emails::boolean,advertise_your_services::boolean,ays_emails::boolean,direct_marketing::boolean from moxiworks.monthly_company_sfdc_data_store except select company_name,companypublickey,company_addressline1,company_city,company_state,company_zipcode,company_phone, company_webpage,company_moxi_hub,company_moxi_dms,company_moxi_engage,company_moxi_present,company_moxi_websites, company_moxi_talent,company_moxi_insights,show_in_product_marketing,advertise_your_listing,ayl_emails,advertise_your_services, ays_emails,direct_marketing from moxiworks.moxiworks_account")
    company_monthly_diffrence_array = cur.fetchall()
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/company_monthly_report'
    full_path = directory + store
    with open(full_path + "/moxiworks_company_monthly_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
        fieldnames = ['companypublickey', 'middleware_data', 'sfdc_data']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for company_monthly_diffrence in company_monthly_diffrence_array:
            cur.execute('select * from moxiworks.moxiworks_account where companypublickey = %s',(company_monthly_diffrence['companypublickey'],))
            company_middleware_data = cur.fetchone()
            cur.execute('select * from moxiworks.monthly_company_sfdc_data_store where companypublickey = %s',(company_monthly_diffrence['companypublickey'],))
            company_sfdc_data = cur.fetchone()
            if not company_middleware_data:
                continue
            if not company_sfdc_data:
                continue
            dict_middleware = {}
            dict_sfdc = {}
            diffrent = 0
            if company_sfdc_data['company_name'] == None:
                company_sfdc_data['company_name'] = ''
            if company_middleware_data['company_name'] != company_sfdc_data['company_name']:
                dict_middleware['company_name'] = company_middleware_data['company_name']
                dict_sfdc['company_name'] = company_sfdc_data['company_name']
                diffrent = 1
            if company_sfdc_data['companypublickey'] == None:
                company_sfdc_data['companypublickey'] = ''
            if company_middleware_data['companypublickey'] != company_sfdc_data['companypublickey']:
                dict_middleware['companypublickey'] = company_middleware_data['companypublickey']
                dict_sfdc['companypublickey'] = company_sfdc_data['companypublickey']
                diffrent = 1
            if company_sfdc_data['company_addressline1'] == None:
                company_sfdc_data['company_addressline1'] = ''
            if company_middleware_data['company_addressline1'].strip() != company_sfdc_data['company_addressline1']:
                dict_middleware['company_addressline1'] = company_middleware_data['company_addressline1'].strip()
                dict_sfdc['company_addressline1'] = company_sfdc_data['company_addressline1']
                diffrent = 1
            if company_sfdc_data['company_city'] == None:
                company_sfdc_data['company_city'] = ''
            if company_middleware_data['company_city'] != company_sfdc_data['company_city']:
                dict_middleware['company_city'] = company_middleware_data['company_city']
                dict_sfdc['company_city'] = company_sfdc_data['company_city']
                diffrent = 1
            if company_sfdc_data['company_state'] == None:
                company_sfdc_data['company_state'] = ''
            if company_middleware_data['company_state'] != company_sfdc_data['company_state']:
                dict_middleware['company_state'] = company_middleware_data['company_state']
                dict_sfdc['company_state'] = company_sfdc_data['company_state']
                diffrent = 1
            if company_sfdc_data['company_zipcode'] == None:
                company_sfdc_data['company_zipcode'] = ''
            if company_middleware_data['company_zipcode'] != company_sfdc_data['company_zipcode']:
                dict_middleware['company_zipcode'] = company_middleware_data['company_zipcode']
                dict_sfdc['company_zipcode'] = company_sfdc_data['company_zipcode']
                diffrent = 1
            
            if company_sfdc_data['company_phone'] == None:
                temp_company_mobile_number_sfdc = ''
            else:
                temp_company_mobile_number_sfdc = str(company_sfdc_data['company_phone']).replace("(",'')
                temp_company_mobile_number_sfdc = temp_company_mobile_number_sfdc.replace(")",'')
                temp_company_mobile_number_sfdc = temp_company_mobile_number_sfdc.replace(" ",'')
                temp_company_mobile_number_sfdc = temp_company_mobile_number_sfdc.replace("-",'')
            temp_company_mobile_number_middleware = str(company_middleware_data['company_phone']).replace('-','')
            if temp_company_mobile_number_middleware != temp_company_mobile_number_sfdc:
                dict_middleware['company_phone']=company_middleware_data['company_phone']
                dict_sfdc['company_phone']=company_sfdc_data['company_phone']
                diffrent = 1

            if company_sfdc_data['company_webpage'] == None:
                company_sfdc_data['company_webpage'] = ''
            if company_middleware_data['company_webpage'] != company_sfdc_data['company_webpage']:
                dict_middleware['company_webpage'] = company_middleware_data['company_webpage']
                dict_sfdc['company_webpage'] = company_sfdc_data['company_webpage']
                diffrent = 1
            if str(company_middleware_data['company_moxi_hub']).lower() != str(company_sfdc_data['company_moxi_hub']).lower():
                dict_middleware['company_moxi_hub'] = company_middleware_data['company_moxi_hub']
                dict_sfdc['company_moxi_hub'] = company_sfdc_data['company_moxi_hub']
                diffrent = 1
            if str(company_middleware_data['company_moxi_dms']).lower() != str(company_sfdc_data['company_moxi_dms']).lower():
                dict_middleware['company_moxi_dms'] = company_middleware_data['company_moxi_dms']
                dict_sfdc['company_moxi_dms'] = company_sfdc_data['company_moxi_dms']
                diffrent = 1
            if str(company_middleware_data['company_moxi_engage']).lower() != str(company_sfdc_data['company_moxi_engage']).lower():
                dict_middleware['company_moxi_engage'] = company_middleware_data['company_moxi_engage']
                dict_sfdc['company_moxi_engage'] = company_sfdc_data['company_moxi_engage']
                diffrent = 1
            if str(company_middleware_data['company_moxi_present']).lower() != str(company_sfdc_data['company_moxi_present']).lower():
                dict_middleware['company_moxi_present'] = company_middleware_data['company_moxi_present']
                dict_sfdc['company_moxi_present'] = company_sfdc_data['company_moxi_present']
                diffrent = 1
            if str(company_middleware_data['company_moxi_websites']).lower() != str(company_sfdc_data['company_moxi_websites']).lower():
                dict_middleware['company_moxi_websites'] = company_middleware_data['company_moxi_websites']
                dict_sfdc['company_moxi_websites'] = company_sfdc_data['company_moxi_websites']
                diffrent = 1
            if str(company_middleware_data['company_moxi_talent']).lower() != str(company_sfdc_data['company_moxi_talent']).lower():
                dict_middleware['company_moxi_talent'] = company_middleware_data['company_moxi_talent']
                dict_sfdc['company_moxi_talent'] = company_sfdc_data['company_moxi_talent']
                diffrent = 1
            if str(company_middleware_data['company_moxi_talent']).lower() != str(company_sfdc_data['company_moxi_talent']).lower():
                dict_middleware['company_moxi_talent'] = company_middleware_data['company_moxi_talent']
                dict_sfdc['company_moxi_talent'] = company_sfdc_data['company_moxi_talent']
                diffrent = 1
            if str(company_middleware_data['company_moxi_insights']).lower() != str(company_sfdc_data['company_moxi_insights']).lower():
                dict_middleware['company_moxi_insights'] = company_middleware_data['company_moxi_insights']
                dict_sfdc['company_moxi_insights'] = company_sfdc_data['company_moxi_insights']
                diffrent = 1
            if str(company_middleware_data['company_moxi_insights']).lower() != str(company_sfdc_data['company_moxi_insights']).lower():
                dict_middleware['company_moxi_insights'] = company_middleware_data['company_moxi_insights']
                dict_sfdc['company_moxi_insights'] = company_sfdc_data['company_moxi_insights']
                diffrent = 1
            if str(company_middleware_data['show_in_product_marketing']).lower() != str(company_sfdc_data['show_in_product_marketing']).lower():
                dict_middleware['show_in_product_marketing'] = company_middleware_data['show_in_product_marketing']
                dict_sfdc['show_in_product_marketing'] = company_sfdc_data['show_in_product_marketing']
                diffrent = 1
            if str(company_middleware_data['advertise_your_listing']).lower() != str(company_sfdc_data['advertise_your_listing']).lower():
                dict_middleware['advertise_your_listing'] = company_middleware_data['advertise_your_listing']
                dict_sfdc['advertise_your_listing'] = company_sfdc_data['advertise_your_listing']
                diffrent = 1
            if str(company_middleware_data['ayl_emails']).lower() != str(company_sfdc_data['ayl_emails']).lower():
                dict_middleware['ayl_emails'] = company_middleware_data['ayl_emails']
                dict_sfdc['ayl_emails'] = company_sfdc_data['ayl_emails']
                diffrent = 1
            if str(company_middleware_data['advertise_your_services']).lower() != str(company_sfdc_data['advertise_your_services']).lower():
                dict_middleware['advertise_your_services'] = company_middleware_data['advertise_your_services']
                dict_sfdc['advertise_your_services'] = company_sfdc_data['advertise_your_services']
                diffrent = 1
            if str(company_middleware_data['ays_emails']).lower() != str(company_sfdc_data['ays_emails']).lower():
                dict_middleware['ays_emails'] = company_middleware_data['ays_emails']
                dict_sfdc['ays_emails'] = company_sfdc_data['ays_emails']
                diffrent = 1
            if str(company_middleware_data['direct_marketing']).lower() != str(company_sfdc_data['direct_marketing']).lower():
                dict_middleware['direct_marketing'] = company_middleware_data['direct_marketing']
                dict_sfdc['direct_marketing'] = company_sfdc_data['direct_marketing']
            if diffrent == 1:
                writer.writerow({'companypublickey':company_monthly_diffrence['companypublickey'], 'middleware_data':dict_middleware, 'sfdc_data':dict_sfdc})
