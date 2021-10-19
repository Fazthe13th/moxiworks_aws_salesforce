import os
import csv
import datetime
import sys

import psycopg2
import psycopg2.extras
from pathlib import Path
from src.aws.storage import S3
from dotenv import load_dotenv, find_dotenv
from common import to_bool, send_agent_report
import glob
import uuid
import csvdiff
import json

load_dotenv(find_dotenv())

# Define read_csv function to read the CSV file from local storage directory
def read_csv(file_name):
    result = []

    with open('storage/' + file_name, 'r') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            result.append(dict(row))
    fp.close()

    return result


class import_data():
    def import_data_in_agent(self, conn):

        # Database connection object
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()

        # define agent csv file name which need to download form s3.
        # Here is the check of parse arguments via the parser variable for test purpose. This arguments
        # will be defined by the user on the console. that must be ' v2' along with the filename.
        if (len(sys.argv) > 1):
            if (str(sys.argv[1]).lower() == 'v2'):
                file_name = 'Agent/Moxi_Agents_{}' . format(datetime.date.today()) + '-V2.csv'
            else:
                print('Invalid args! Did you mean v2')
                return 0
        else:
            file_name = 'Agent/Moxi_Agents_{}'.format(datetime.date.today()) + '.csv'

        # Download above file from s3 through s3.download_file function.
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        # NOW TIME TO START PROCESSING...

        # Initialize previous file path which must be exists at storage directory to be compared.
        # Here is the check of parse arguments via the parser variable for test purpose. This arguments
        # will be defined by the user on the console. that must be ' v2' along with the filename.
        if (len(sys.argv) > 1):
            if (str(sys.argv[1]).lower() == 'v2'):
                agent_file_prefix = 'storage/Moxi_Agents_{}' . format(datetime.date.today()) + '-V2'
            else:
                print('Invalid args! Did you mean v2')
                return 0
        else:
            agent_file_prefix = 'storage/Moxi_Agents_{}'.format(datetime.date.today())


        new_file_name = glob.glob(agent_file_prefix + ".csv")

        # Retrieve the last processed file name from moxiworks_report table which has saved after every execution.
        cur.execute(
            "SELECT * FROM moxiworks.moxiworks_report WHERE object_processed = 'agent' ORDER BY created_at desc LIMIT 1")

        report_records = cur.fetchone()
        report_data = []
        # for row in records:
        if (report_records):
            report_data.append(dict(report_records))
            old_file_name = [report_data[0]['last_file_porcessed']]
        else:
            old_file_name = ['storage/Moxi_Agents_initial_file.csv']

        # Diff two CSV files, returning the patch which transforms one into the other.
        # Here agent_uuid is the index key.
        patch = csvdiff.diff_files(''.join(old_file_name),
                                   ''.join(new_file_name), ['agent_uuid'], ',')

        total_inserted = len(patch['added'])

        # For loop that interacts over newly added companies and insert it to
        # moxiworks_account also moxiworks_account_delta with insert status
        for key, data in enumerate(patch['added']):

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
            status = 'Insert'

            if not date_deactivated:
                date_deactivated = None

            try:
                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_contact (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent))
                print('Data Inserted in moxiworks_contact....')
                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_contact_delta (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status))
                print('Data Inserted for moxiworks_contact_delta with Insert status....')
                conn.commit()
                success = 1
            except Exception as e:
                print("type error: " + str(e))
                print("Failed at ID " + str(data['agent_uuid']))
                failed = 1
                conn.rollback()
                continue

        #     os.unlink(file)


        total_updated = len(patch['changed'])
        # For loop that interacts over updated agents and update those agents to moxiworks_contact
        # and insert it to moxiworks_contact_delta table with update status
        for key, data in enumerate(patch['changed']):
            columns = ''
            total_count = len(data['fields'])
            for inner_key, inner_data in enumerate(data['fields']):

                if (inner_data == 'agent_active' or inner_data == 'at_least_one_mls_association'
                        or inner_data == 'agent_moxi_hub' or inner_data == 'agent_moxi_dms'
                        or inner_data == 'agent_moxi_engage' or inner_data == 'agent_moxi_present'
                        or inner_data == 'agent_moxi_websites' or inner_data == 'agent_moxi_talent'):
                    columns += inner_data + '=' + "'" + str(to_bool(data['fields'][inner_data]['to'])) + "'"
                else:
                    if(inner_data == 'date_deactivated'):
                        if not data['fields'][inner_data]['to']:
                            date_deactivated = None
                            columns += inner_data + '=' + "'" + date_deactivated + "'"
                    else:
                        columns += inner_data + '=' + "'" + data['fields'][inner_data]['to'] + "'"
                    #columns += inner_data + '=' + "'" + data['fields'][inner_data]['to'] + "'"


                if (inner_key < total_count - 1):
                    columns += ', '

            query = "update moxiworks.moxiworks_contact set {} where agent_uuid='{}';".format(columns, data['key'][0])
            # print (query)
            cur.execute(query)
            conn.commit()
            print('Id: ' + str(data['key'][0]) + ' has been updated')
            cur.execute("SELECT * from moxiworks.moxiworks_contact where agent_uuid = '{}'".format(data['key'][0]))

            records = cur.fetchone()
            data = []
            #for row in records:
            data.append(dict(records))

            uniq_id = str(uuid.uuid4())
            agent_uuid = data[0]['agent_uuid']
            userid = data[0]['userid']
            agent_username = data[0]['agent_username']
            firstname = data[0]['firstname']
            lastname = data[0]['lastname']
            nickname = data[0]['nickname']
            officename_display = data[0]['officename_display']
            officepublickey = data[0]['officepublickey']
            office_addressline1 = str(data[0]['office_addressline1']) + " " + str(data[0]['office_addressline2'])
            office_city = data[0]['office_city']
            office_state = data[0]['office_state']
            office_zip = data[0]['office_zip']
            office_phone = data[0]['office_phone']
            office_extension = data[0]['office_extension']
            company_legalname = data[0]['company_legalname']
            companypublickey = data[0]['companypublickey']
            lastmodified = data[0]['lastmodified']
            primary_email = data[0]['primary_email']
            secondary_email = data[0]['secondary_email']
            alternate_email = data[0]['alternate_email']
            direct_phone = data[0]['direct_phone']
            mobile_phone = data[0]['mobile_phone']
            title_display = data[0]['title_display']
            accred_display = data[0]['accred_display']
            currentrolename = data[0]['currentrolename']
            user_category = data[0]['user_category']
            title_cleaned = data[0]['title_cleaned']
            team_name = data[0]['team_name']
            team_member_type = data[0]['team_member_type']
            date_deactivated = data[0]['date_deactivated']
            agent_active = data[0]['agent_active']
            at_least_one_mls_association = data[0]['at_least_one_mls_association']
            agent_moxi_hub = data[0]['agent_moxi_hub']
            agent_moxi_dms = data[0]['agent_moxi_dms']
            agent_moxi_engage = data[0]['agent_moxi_engage']
            agent_moxi_present = data[0]['agent_moxi_present']
            agent_moxi_websites = data[0]['agent_moxi_websites']
            agent_moxi_talent = data[0]['agent_moxi_talent']

            if not date_deactivated:
                date_deactivated = None

            try:
                status = 'Update'
                cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact_delta (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status))

                print('Data Inserted in moxiworks_agent_delta with Insert status....')
                conn.commit()
            except Exception as e:
                print("type error: " + str(e))
                print("Failed at delta ID " + str(data[0]['userid']))
                failed = 1
                conn.rollback()
                continue

        cur.execute(
            "INSERT INTO moxiworks.moxiworks_report(object_processed, total_inserted, total_udpated, total_data_processed, last_file_porcessed, created_at) "
            "VALUES ('{}', {}, {}, {}, '{}', NOW())".format('agent', total_inserted, total_updated,
                                                            total_inserted + total_updated, ''.join(new_file_name)))
        conn.commit()


        total_data = total_inserted + total_updated
        print("Total Inserted: " + str(total_inserted))
        print("Total Updated: " + str(total_updated))
        print("Total Data: " + str(total_data))

        send_agent_report(total_data, total_inserted, total_updated)


import_data_obj = import_data()
# Define database connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
import_data_obj.import_data_in_agent(conn)
