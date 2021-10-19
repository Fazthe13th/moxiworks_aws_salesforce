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
import datetime

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
        total_inserted = 0
        total_updated = 0
        garbage_count = 0
        duplicate_count = 0
        reason = ""
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Agent/Moxi_Agents_initial_{}'.format(datetime.date.today()) + '.csv'
        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        local_file_name = 'Moxi_Agents_initial_{}'.format(datetime.date.today()) + '.csv'
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        logf = open("agent_error_{}".format(datetime.date.today()) + ".log", "w")
        for data in csv_raw_data:
            reason_array = []
            total_data = total_data + 1
            print('Data processing ' + str(total_data))
            agent_uuid = data['agent_uuid']
            userid = data['userid']
            agent_username = data['agent_username']
            firstname = data['firstname']
            lastname = data['lastname']
            firstname = data['firstname']
            if len(firstname) > 40:
                firstname = firstname[:40]
            lastname = data['lastname']
            if len(lastname) > 40:
                lastname = lastname[:40]
            nickname = data['nickname']
            officename_display = data['officename_display']
            officepublickey = data['officepublickey']
            office_addressline1 = str(data['office_addressline1']) + " " + str(data['office_addressline2'])
            office_addressline2 = str(data['office_addressline2'])
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
            if len(accred_display) > 40:
                accred_display = accred_display[:40]
            currentrolename = data['currentrolename']
            user_category = data['user_category']
            title_cleaned = data['title_cleaned']
            team_name = data['team_name']
            team_member_type = data['team_member_type']
            date_deactivated_csv = data['date_deactivated']
            if not date_deactivated_csv:
                date_deactivated = None
            else:
                format_str = '%m/%d/%Y' # The format
                date_deactivated = datetime.datetime.strptime(date_deactivated_csv, format_str)
                date_deactivated = str(date_deactivated)
            agent_active = data['agent_active']
            at_least_one_mls_association = data['at_least_one_mls_association']
            agent_moxi_hub = data['agent_moxi_hub']
            agent_moxi_dms = data['agent_moxi_dms']
            agent_moxi_engage = data['agent_moxi_engage']
            agent_moxi_present = data['agent_moxi_present']
            agent_moxi_websites = data['agent_moxi_websites']
            agent_moxi_talent = data['agent_moxi_talent']

            if officepublickey:
                cur.execute('SELECT * from moxiworks.moxiworks_office where officepublickey = %s LIMIT 1', (officepublickey,))
                get_office_data = cur.fetchone()
                
                if not get_office_data:
                    reason = "Officepublickey does not exists in office table"
                    reason_array.append(reason)

                else:
                    sf_parent_id = get_office_data['sfdc_office_id']
            else:
                cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s LIMIT 1', (companypublickey,))
                get_company_data = cur.fetchone()
                if not get_company_data:
                    reason = "Companypublickey does not exists in company table"
                    reason_array.append(reason)
                else:
                    sf_parent_id = get_company_data['sfdc_account_id']

            #Duplicate Count
            cur.execute('SELECT count(*) from moxiworks.moxiworks_contact where agent_uuid = %s', (agent_uuid,))
            check_duplicate_agent = cur.fetchone()
            if check_duplicate_agent[0] > 0:
                duplicate_count = duplicate_count + 1
                reason = "This data is a duplicate of agent uuid: " + str(agent_uuid)
                reason_array.append(reason)
            cur.execute('SELECT * from moxiworks.sfdc_contact_data where sfdc_email = %s', (primary_email,))
            exist_check = cur.fetchone()
            if exist_check:
                status = 'update'
                sfdc_agent_id = exist_check['sfdc_contact_id']
                if not exist_check['sfdc_title']:
                    title = title_cleaned
                else:
                    title = exist_check['sfdc_title']
                print('Agent UUID: ' + agent_uuid + ' SF parent ID: ' + sf_parent_id + ' Status: ' + status)
                
                try:
                    # cur.execute("UPDATE moxiworks.moxiworks_contact SET sf_parent_id = %s WHERE agent_uuid = %s",(sf_parent_id,str(agent_uuid)))
                    # print('Updated moxiworks_contact....')
                    # cur.execute(
                    #     'INSERT INTO moxiworks.moxiworks_contact (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_contact_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    #     (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_agent_id))
                    # print('Data Inserted in moxiworks_contact....')
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact_delta (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id, sfdc_contact_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id,sfdc_agent_id))
                    print('Data Inserted for moxiworks_contact_delta with Update status....')
                    conn.commit()
                    total_updated = total_updated + 1
                except Exception as e:
                    logf.write("Failed to insert uuid {0}: {1}\n".format(str(agent_uuid), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(data['agent_uuid']))
                    conn.rollback()
                    continue
            else:
            
                status = 'insert'
                title_cleaned = data['title_cleaned']
                print('Agent UUID: ' + agent_uuid + ' SF parent ID: ' + sf_parent_id + ' Status: ' + status)
                try:
                    # cur.execute(
                    #     'INSERT INTO moxiworks.moxiworks_contact (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    #     (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id))
                    # print('Data Inserted in moxiworks_contact....')
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact_delta (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id))
                    print('Data Inserted for moxiworks_contact_delta with Insert status....')
                    conn.commit()
                    total_inserted = total_inserted + 1
                except Exception as e:
                    logf.write("Failed to insert uuid {0}: {1}\n".format(str(agent_uuid), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(data['agent_uuid']))
                    conn.rollback()
                    continue
        
        print("Total Inserted: " + str(total_inserted))
        print("Total Updated: " + str(total_updated))
        print("Total Duplicate Data: " + str(duplicate_count))
        print("Total Garbage Data: " + str(garbage_count))
        print("Total Data: " + str(total_data))
#Created a object of the class
import_obj = agent_normalization_class()
#create the connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
#call the import_company_inital_data function
import_obj.agent_normalization(conn)     