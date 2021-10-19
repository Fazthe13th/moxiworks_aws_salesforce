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
from common import to_bool, send_agent_report
import glob
import uuid
import csvdiff
from validate_email import validate_email

load_dotenv(find_dotenv())


def read_csv(file_name):
    result = []
    #Files are in storage folder . we read the csv files from there
    with open('storage/' + file_name, 'r') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            result.append(dict(row))
    fp.close()

    return result


class agent_normalization_class():
    def agent_normalization(self, conn):
        # Initialization 
        total_data = 0
        garbage_count = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Agent/Moxi_Agents_normalization_{}'.format(datetime.date.today()) + '.csv'
        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        local_file_name = 'Moxi_Agents_normalization_{}'.format(datetime.date.today()) + '.csv'
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        for data in csv_raw_data:
            total_data = total_data + 1
            agent_uuid = data['agent_uuid']
            userid = data['userid']
            agent_username = data['agent_username']
            firstname = data['firstname']
            lastname = data['lastname']
            nickname = data['nickname']
            officename_display = data['officename_display']
            officepublickey = data['officepublickey']
            office_addressline1 = str(data['office_addressline1']) + " " + str(data['office_addressline2'])
            office_city = data['office_city']
            office_state = data['office_state']
            office_zip = data['office_zip']
            office_phone = data['office_phone']
            office_extension = data['office_extension']
            company_legalname = data['company_legalname']
            companypublickey = data['companypublickey']
            lastmodified = data['lastmodified']
            primary_email = data['primary_email']
            secondary_email = data['secondary_email']
            alternate_email = data['alternate_email']
            direct_phone = data['direct_phone']
            mobile_phone = data['mobile_phone']
            title_display = data['title_display']
            accred_display = data['accred_display']
            currentrolename = data['currentrolename']
            user_category = data['user_category']
            title_cleaned = data['title_cleaned']
            team_name = data['team_name']
            team_member_type = data['team_member_type']
            date_deactivated = data['date_deactivated']
            agent_active = to_bool(data['agent_active'])
            at_least_one_mls_association = to_bool(data['at_least_one_mls_association'])
            agent_moxi_hub = to_bool(data['agent_moxi_hub'])
            agent_moxi_dms = to_bool(data['agent_moxi_dms'])
            agent_moxi_engage = to_bool(data['agent_moxi_engage'])
            agent_moxi_present = to_bool(data['agent_moxi_present'])
            agent_moxi_websites = to_bool(data['agent_moxi_websites'])
            agent_moxi_talent = to_bool(data['agent_moxi_talent'])