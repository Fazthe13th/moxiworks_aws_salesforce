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

def generate_moxiworks_agent_monthly_report():
    conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
        'db_user') + " password=" + os.getenv('db_password'))
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    conn.commit()
    # agent report generation
    cur.execute("select agent_uuid,firstname,lastname,officepublickey,office_addressline1,office_city,office_state,office_zip,primary_email, alternate_email,direct_phone,mobile_phone,currentrolename,title_cleaned,team_name,team_member_type,date_deactivated,agent_active::boolean, agent_moxi_hub::boolean,agent_moxi_dms::boolean,agent_moxi_engage::boolean,agent_moxi_present::boolean,agent_moxi_websites::boolean, agent_moxi_talent::boolean from moxiworks.monthly_agent_sfdc_data_store except select agent_uuid,firstname,lastname,officepublickey,office_addressline1,office_city, office_state,office_zip,primary_email, alternate_email,direct_phone,mobile_phone,currentrolename,title_cleaned,team_name, team_member_type,date_deactivated,agent_active,agent_moxi_hub,agent_moxi_dms,agent_moxi_engage, agent_moxi_present,agent_moxi_websites,agent_moxi_talent from moxiworks.moxiworks_contact")
    agent_monthly_diffrence_array = cur.fetchall()
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_monthly_report'
    full_path = directory + store
    with open(full_path + "/moxiworks_agent_monthly_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
        fieldnames = ['agent_uuid', 'middleware_data', 'sfdc_data']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for agent_monthly_diffrence in agent_monthly_diffrence_array:
            cur.execute('select * from moxiworks.moxiworks_contact where agent_uuid = %s',(agent_monthly_diffrence['agent_uuid'],))
            agent_middleware_data = cur.fetchone()
            
            cur.execute('select * from moxiworks.monthly_agent_sfdc_data_store where agent_uuid = %s',(agent_monthly_diffrence['agent_uuid'],))
            agent_sfdc_data = cur.fetchone()
            if not agent_middleware_data:
                continue
            if not agent_sfdc_data:
                continue
            dict_middleware = {}
            dict_sfdc = {}
            diffrence = 0
            # if agent_sfdc_data['agent_uuid'] == None:
            #     agent_sfdc_data['agent_uuid'] = ''
            # if agent_middleware_data['agent_uuid'] != agent_sfdc_data['agent_uuid']:
            #     dict_middleware['agent_uuid']=agent_middleware_data['agent_uuid']
            #     dict_sfdc['agent_uuid']=agent_sfdc_data['agent_uuid']
            #     diffrence = 1
            if agent_sfdc_data['firstname'] == None:
                agent_sfdc_data['firstname'] = ''
            if agent_middleware_data['firstname'].lower() != agent_sfdc_data['firstname'].lower():
                dict_middleware['firstname']=agent_middleware_data['firstname']
                dict_sfdc['firstname']=agent_sfdc_data['firstname']
                diffrence = 1
            if agent_sfdc_data['lastname'] == None:
                agent_sfdc_data['lastname'] = ''
            if agent_middleware_data['lastname'].lower() != agent_sfdc_data['lastname'].lower():
                dict_middleware['lastname']=agent_middleware_data['lastname']
                dict_sfdc['lastname']=agent_sfdc_data['lastname']
                diffrence = 1
            if agent_sfdc_data['officepublickey'] == None:
                agent_sfdc_data['officepublickey'] = ''
            if agent_middleware_data['officepublickey'] != agent_sfdc_data['officepublickey']:
                dict_middleware['officepublickey']=agent_middleware_data['officepublickey']
                dict_sfdc['officepublickey']=agent_sfdc_data['officepublickey']
                diffrence = 1
            if agent_sfdc_data['office_addressline1'] == None:
                agent_sfdc_data['office_addressline1'] = ''
            if agent_middleware_data['office_addressline1'].strip() != agent_sfdc_data['office_addressline1']:
                dict_middleware['office_addressline1']=agent_middleware_data['office_addressline1'].strip()
                dict_sfdc['office_addressline1']=agent_sfdc_data['office_addressline1']
                diffrence = 1
            if agent_sfdc_data['office_city'] == None:
                agent_sfdc_data['office_city'] = ''
            if agent_middleware_data['office_city'] != agent_sfdc_data['office_city']:
                dict_middleware['office_city']=agent_middleware_data['office_city']
                dict_sfdc['office_city']=agent_sfdc_data['office_city']
                diffrence = 1
            if agent_sfdc_data['office_state'] == None:
                agent_sfdc_data['office_state'] = ''
            if agent_middleware_data['office_state'] != agent_sfdc_data['office_state']:
                dict_middleware['office_state']=agent_middleware_data['office_state']
                dict_sfdc['office_state']=agent_sfdc_data['office_state']
                diffrence = 1
            if agent_sfdc_data['office_zip'] == None:
                agent_sfdc_data['office_zip'] = ''
            if agent_middleware_data['office_zip'] != agent_sfdc_data['office_zip']:
                dict_middleware['office_zip']=agent_middleware_data['office_zip']
                dict_sfdc['office_zip']=agent_sfdc_data['office_zip']
                diffrence = 1
            if agent_sfdc_data['primary_email'] == None:
                agent_sfdc_data['primary_email'] = ''
            if agent_middleware_data['primary_email'].lower() != agent_sfdc_data['primary_email'].lower():
                dict_middleware['primary_email']=agent_middleware_data['primary_email']
                dict_sfdc['primary_email']=agent_sfdc_data['primary_email']
                diffrence = 1
            if agent_sfdc_data['alternate_email'] == None:
                agent_sfdc_data['alternate_email'] = ''
            if agent_middleware_data['alternate_email'].lower() != agent_sfdc_data['alternate_email'].lower():
                dict_middleware['alternate_email']=agent_middleware_data['alternate_email']
                dict_sfdc['alternate_email']=agent_sfdc_data['alternate_email']
                diffrence = 1
            if agent_sfdc_data['direct_phone'] == None:
                temp_agent_direct_number_sfdc = ''
            else:
                temp_agent_direct_number_sfdc = str(agent_sfdc_data['direct_phone']).replace("(",'')
                temp_agent_direct_number_sfdc = temp_agent_direct_number_sfdc.replace(")",'')
                temp_agent_direct_number_sfdc = temp_agent_direct_number_sfdc.replace(" ",'')
                temp_agent_direct_number_sfdc = temp_agent_direct_number_sfdc.replace("-",'')
            temp_agent_direct_number_middleware = str(agent_middleware_data['direct_phone']).replace('-','')
            if temp_agent_direct_number_middleware != temp_agent_direct_number_sfdc:
                dict_middleware['direct_phone']=agent_middleware_data['direct_phone']
                dict_sfdc['direct_phone']=agent_sfdc_data['direct_phone']
                diffrence = 1
            if agent_sfdc_data['mobile_phone'] == None:
                temp_agent_mobile_number_sfdc = ''
            else:
                temp_agent_mobile_number_sfdc = str(agent_sfdc_data['mobile_phone']).replace("(",'')
                temp_agent_mobile_number_sfdc = temp_agent_mobile_number_sfdc.replace(")",'')
                temp_agent_mobile_number_sfdc = temp_agent_mobile_number_sfdc.replace(" ",'')
                temp_agent_mobile_number_sfdc = temp_agent_mobile_number_sfdc.replace("-",'')
            temp_agent_mobile_number_middleware = str(agent_middleware_data['mobile_phone']).replace('-','')
            if temp_agent_mobile_number_middleware != temp_agent_mobile_number_sfdc:
                dict_middleware['mobile_phone']=agent_middleware_data['mobile_phone']
                dict_sfdc['mobile_phone']=agent_sfdc_data['mobile_phone']
                diffrence = 1
            if agent_sfdc_data['currentrolename'] == None:
                agent_sfdc_data['currentrolename'] = ''
            if agent_middleware_data['currentrolename'] != agent_sfdc_data['currentrolename']:
                dict_middleware['currentrolename']=agent_middleware_data['currentrolename']
                dict_sfdc['currentrolename']=agent_sfdc_data['currentrolename']
                diffrence = 1
            if agent_sfdc_data['title_cleaned'] == None:
                agent_sfdc_data['title_cleaned'] = ''
            if agent_middleware_data['title_cleaned'] != agent_sfdc_data['title_cleaned']:
                dict_middleware['title_cleaned']=agent_middleware_data['title_cleaned']
                dict_sfdc['title_cleaned']=agent_sfdc_data['title_cleaned']
                diffrence = 1
            if agent_sfdc_data['team_name'] == None:
                agent_sfdc_data['team_name'] = ''
            if agent_middleware_data['team_name'] != agent_sfdc_data['team_name']:
                dict_middleware['team_name']=agent_middleware_data['team_name']
                dict_sfdc['team_name']=agent_sfdc_data['team_name']
                diffrence = 1
            if agent_sfdc_data['team_member_type'] == None:
                agent_sfdc_data['team_member_type'] = ''
            if agent_middleware_data['team_member_type'] != agent_sfdc_data['team_member_type']:
                dict_middleware['team_member_type']=agent_middleware_data['team_member_type']
                dict_sfdc['team_member_type']=agent_sfdc_data['team_member_type']
                diffrence = 1
            if str(agent_sfdc_data['date_deactivated']) == None:
                agent_sfdc_data['date_deactivated'] = ''
            if str(agent_middleware_data['date_deactivated']) != str(agent_sfdc_data['date_deactivated']):
                dict_middleware['date_deactivated']=str(agent_middleware_data['date_deactivated'])
                dict_sfdc['date_deactivated']=str(agent_sfdc_data['date_deactivated'])
                diffrence = 1
            if str(agent_middleware_data['agent_active']).lower() != str(agent_sfdc_data['agent_active']).lower():
                dict_middleware['agent_active'] = agent_middleware_data['agent_active']
                dict_sfdc['agent_active'] = agent_sfdc_data['agent_active']
                diffrence = 1
            if str(agent_middleware_data['agent_moxi_hub']).lower() != str(agent_sfdc_data['agent_moxi_hub']).lower():
                dict_middleware['agent_moxi_hub'] = agent_middleware_data['agent_moxi_hub']
                dict_sfdc['agent_moxi_hub'] = agent_sfdc_data['agent_moxi_hub']
                diffrence = 1
            if str(agent_middleware_data['agent_moxi_dms']).lower() != str(agent_sfdc_data['agent_moxi_dms']).lower():
                dict_middleware['agent_moxi_dms'] = agent_middleware_data['agent_moxi_dms']
                dict_sfdc['agent_moxi_dms'] = agent_sfdc_data['agent_moxi_dms']
                diffrence = 1
            if str(agent_middleware_data['agent_moxi_engage']).lower() != str(agent_sfdc_data['agent_moxi_engage']).lower():
                dict_middleware['agent_moxi_engage'] = agent_middleware_data['agent_moxi_engage']
                dict_sfdc['agent_moxi_engage'] = agent_sfdc_data['agent_moxi_engage']
                diffrence = 1
            if str(agent_middleware_data['agent_moxi_present']).lower() != str(agent_sfdc_data['agent_moxi_present']).lower():
                dict_middleware['agent_moxi_present'] = agent_middleware_data['agent_moxi_present']
                dict_sfdc['agent_moxi_present'] = agent_sfdc_data['agent_moxi_present']
                diffrence = 1
            if str(agent_middleware_data['agent_moxi_websites']).lower() != str(agent_sfdc_data['agent_moxi_websites']).lower():
                dict_middleware['agent_moxi_websites'] = agent_middleware_data['agent_moxi_websites']
                dict_sfdc['agent_moxi_websites'] = agent_sfdc_data['agent_moxi_websites']
                diffrence = 1
            if str(agent_middleware_data['agent_moxi_talent']).lower() != str(agent_sfdc_data['agent_moxi_talent']).lower():
                dict_middleware['agent_moxi_talent'] = agent_middleware_data['agent_moxi_talent']
                dict_sfdc['agent_moxi_talent'] = agent_sfdc_data['agent_moxi_talent']
                diffrence = 1
            # if diffrence == 1:
            #     print(dict_middleware)
            #     print(dict_sfdc)
            if diffrence == 1:
                writer.writerow({'agent_uuid':agent_monthly_diffrence['agent_uuid'], 'middleware_data':dict_middleware, 'sfdc_data':dict_sfdc})
