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
import http.client

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


class import_agent_daily_data():
    def import_in_main_and_delta_table(self, conn,temp_table_inserted,temp_table_duplicate,temp_table_garbage,temp_table_data_processed):
        total_data = 0
        total_inserted = 0
        total_updated = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        #Agent delete candidate report
        # cur.execute("select * from moxiworks.moxiworks_contact where date_deactivated is not null and agent_active ='false' and isdeleted = 'false'")
        # delete_candidate_agent_array = cur.fetchall()
        # directory = str(os.path.dirname(os.path.realpath(__file__)))
        # store = '/storage/agent_delete_candidate_report'
        # full_path = directory + store
        # with open(full_path + "/moxiworks_agent_delete_candidate_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
        #     # fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'nickname', 'officename_display', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'office_extension', 'company_legalname', 'companypublickey', 'lastmodified', 'primary_email', 'secondary_email', 'alternate_email', 'direct_phone', 'mobile_phone', 'title_display', 'accred_display', 'currentrolename', 'user_category', 'title_cleaned', 'team_name', 'team_member_type', 'date_deactivated', 'agent_active', 'at_least_one_mls_association', 'agent_moxi_hub', 'agent_moxi_dms', 'agent_moxi_engage', 'agent_moxi_present', 'agent_moxi_websites', 'agent_moxi_talent']
        #     fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'officename_display', 'officepublickey', 'company_legalname', 'companypublickey']
        #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        #     writer.writeheader()
        #     for delete_candidate_agent in delete_candidate_agent_array:
        #         writer.writerow({'agent_uuid':delete_candidate_agent['agent_uuid'], 'userid':delete_candidate_agent['userid'], 'agent_username':delete_candidate_agent['agent_username'], 'firstname':delete_candidate_agent['firstname'], 'lastname':delete_candidate_agent['lastname'], 'officename_display':delete_candidate_agent['officename_display'], 'officepublickey':delete_candidate_agent['officepublickey'], 'company_legalname':delete_candidate_agent['company_legalname'], 'companypublickey':delete_candidate_agent['companypublickey']})
        #Agent Delete candidate report end
        #Agent inactive report
        cur.execute("select * from moxiworks.moxiworks_contact where agent_uuid not in ( select agent_uuid from moxiworks.moxiworks_contact_ongoing)")
        inactive_array = cur.fetchall()
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/agent_inactive_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_agent_inactive_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            # fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'nickname', 'officename_display', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'office_extension', 'company_legalname', 'companypublickey', 'lastmodified', 'primary_email', 'secondary_email', 'alternate_email', 'direct_phone', 'mobile_phone', 'title_display', 'accred_display', 'currentrolename', 'user_category', 'title_cleaned', 'team_name', 'team_member_type', 'date_deactivated', 'agent_active', 'at_least_one_mls_association', 'agent_moxi_hub', 'agent_moxi_dms', 'agent_moxi_engage', 'agent_moxi_present', 'agent_moxi_websites', 'agent_moxi_talent']
            fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'officename_display', 'officepublickey', 'company_legalname', 'companypublickey']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for inactive in inactive_array:
                writer.writerow({'agent_uuid':inactive['agent_uuid'], 'userid':inactive['userid'], 'agent_username':inactive['agent_username'], 'firstname':inactive['firstname'], 'lastname':inactive['lastname'], 'officename_display':inactive['officename_display'], 'officepublickey':inactive['officepublickey'], 'company_legalname':inactive['company_legalname'], 'companypublickey':inactive['companypublickey']})
                try:
                    cur.execute(
                    "UPDATE moxiworks.moxiworks_contact SET isactivated = %s where agent_uuid = %s",
                    (to_bool('f'), inactive['agent_uuid']))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    continue
        #Agent inactive report end
        #Agent log report
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/agent_log_report'
        full_path = directory + store
        logf = open(full_path + "/agent_error_{}".format(datetime.date.today()) + ".log", "w")
        cur.execute("select a.* from (SELECT agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_addressline2, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_contact_id FROM moxiworks.moxiworks_contact_ongoing except SELECT agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_addressline2, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_contact_id FROM moxiworks.moxiworks_contact) a, moxiworks.moxiworks_contact b where a.agent_uuid=b.agent_uuid")
        update_array = cur.fetchall()
        cur.execute("select * from moxiworks.moxiworks_contact_ongoing where agent_uuid not in (select agent_uuid from moxiworks.moxiworks_contact)")
        insert_array = cur.fetchall()
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/agent_update_report'
        full_path = directory + store
        #Agent update report
        with open(full_path + "/moxiworks_agent_update_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            # fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'nickname', 'officename_display', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'office_extension', 'company_legalname', 'companypublickey', 'lastmodified', 'primary_email', 'secondary_email', 'alternate_email', 'direct_phone', 'mobile_phone', 'title_display', 'accred_display', 'currentrolename', 'user_category', 'title_cleaned', 'team_name', 'team_member_type', 'date_deactivated', 'agent_active', 'at_least_one_mls_association', 'agent_moxi_hub', 'agent_moxi_dms', 'agent_moxi_engage', 'agent_moxi_present', 'agent_moxi_websites', 'agent_moxi_talent']
            fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'officename_display', 'officepublickey', 'company_legalname', 'companypublickey', 'Status']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for update in update_array:
                cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s LIMIT 1', (update['companypublickey'],))
                get_company_data = cur.fetchone()
                parent_account = get_company_data['sfdc_account_id']
                cur.execute('SELECT * from moxiworks.moxiworks_contact where agent_uuid = %s LIMIT 1', (update['agent_uuid'],))
                get_agent_data = cur.fetchone()
                total_data = total_data + 1
                # writer.writerow({'agent_uuid':update['agent_uuid'], 'userid':update['userid'], 'agent_username':update['agent_username'], 'firstname':update['firstname'], 'lastname':update['lastname'], 'nickname':update['nickname'], 'officename_display':update['officename_display'], 'officepublickey':update['officepublickey'], 'office_addressline1':update['office_addressline1'], 'office_city':update['office_city'], 'office_state':update['office_state'], 'office_zip':update['office_zip'], 'office_phone':update['office_phone'], 'office_extension':update['office_extension'], 'company_legalname':update['company_legalname'], 'companypublickey':update['companypublickey'], 'lastmodified':update['lastmodified'], 'primary_email':update['primary_email'], 'secondary_email':update['secondary_email'], 'alternate_email':update['alternate_email'], 'direct_phone':update['direct_phone'], 'mobile_phone':update['mobile_phone'], 'title_display':update['title_display'], 'accred_display':update['accred_display'], 'currentrolename':update['currentrolename'], 'user_category':update['user_category'], 'title_cleaned':update['title_cleaned'], 'team_name':update['team_name'], 'team_member_type':update['team_member_type'], 'date_deactivated':update['date_deactivated'], 'agent_active':update['agent_active'], 'at_least_one_mls_association':update['at_least_one_mls_association'], 'agent_moxi_hub':update['agent_moxi_hub'], 'agent_moxi_dms':update['agent_moxi_dms'], 'agent_moxi_engage':update['agent_moxi_engage'], 'agent_moxi_present':update['agent_moxi_present'], 'agent_moxi_websites':update['agent_moxi_websites'], 'agent_moxi_talent':update['agent_moxi_talent']})
                writer.writerow({'agent_uuid':update['agent_uuid'], 'userid':update['userid'], 'agent_username':update['agent_username'], 'firstname':update['firstname'], 'lastname':update['lastname'], 'officename_display':update['officename_display'], 'officepublickey':update['officepublickey'], 'company_legalname':update['company_legalname'], 'companypublickey':update['companypublickey'], 'Status': 'Update'})
                if str(update['date_deactivated']) != '' and update['agent_active'] == False:
                    isactive_in_database = 'f'
                else:
                    isactive_in_database = 't'
                status = 'update'
                try:
                    cur.execute(
                    "UPDATE moxiworks.moxiworks_contact SET userid = %s, agent_username = %s, firstname = %s, lastname = %s, nickname = %s, officename_display = %s, officepublickey = %s, office_addressline1 = %s, office_city = %s, office_state = %s, office_zip = %s, office_phone = %s, office_extension = %s, company_legalname = %s, companypublickey = %s, primary_email = %s, secondary_email = %s, alternate_email = %s, direct_phone = %s, mobile_phone = %s, title_display = %s, accred_display = %s, currentrolename = %s, user_category = %s, title_cleaned = %s, team_name = %s, team_member_type = %s, date_deactivated = %s, agent_active = %s, at_least_one_mls_association = %s, agent_moxi_hub = %s, agent_moxi_dms = %s, agent_moxi_engage = %s, agent_moxi_present = %s, agent_moxi_websites = %s, agent_moxi_talent = %s, sf_parent_id = %s,parent_account = %s,isactive = %s  where agent_uuid = %s",
                    (update['userid'], update['agent_username'], update['firstname'], update['lastname'], update['nickname'], update['officename_display'], update['officepublickey'], update['office_addressline1'], update['office_city'], update['office_state'], update['office_zip'], update['office_phone'], update['office_extension'], update['company_legalname'], update['companypublickey'], update['primary_email'], update['secondary_email'], update['alternate_email'], update['direct_phone'], update['mobile_phone'], update['title_display'], update['accred_display'], update['currentrolename'], update['user_category'], update['title_cleaned'], update['team_name'], update['team_member_type'], update['date_deactivated'], update['agent_active'], update['at_least_one_mls_association'], update['agent_moxi_hub'], update['agent_moxi_dms'], update['agent_moxi_engage'], update['agent_moxi_present'], update['agent_moxi_websites'], update['agent_moxi_talent'], update['sf_parent_id'],parent_account,isactive_in_database, update['agent_uuid']))
                    print('Data updated in moxi_contact....')
                    cur.execute(
                    'INSERT INTO moxiworks.moxiworks_contact_delta (userid , agent_username , firstname , lastname , nickname , officename_display , officepublickey , office_addressline1 , office_city , office_state , office_zip , office_phone , office_extension , company_legalname , companypublickey , primary_email , secondary_email , alternate_email , direct_phone , mobile_phone , title_display , accred_display , currentrolename , user_category , title_cleaned , team_name , team_member_type , date_deactivated , agent_active , at_least_one_mls_association , agent_moxi_hub , agent_moxi_dms , agent_moxi_engage , agent_moxi_present , agent_moxi_websites , agent_moxi_talent , sf_parent_id,agent_uuid,sfdc_contact_id,parent_account,isactivated,isactive, status) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)',
                    (update['userid'],update['agent_username'],update['firstname'],update['lastname'],update['nickname'],update['officename_display'],update['officepublickey'],update['office_addressline1'],update['office_city'],update['office_state'],update['office_zip'],update['office_phone'],update['office_extension'],update['company_legalname'],update['companypublickey'],update['primary_email'],update['secondary_email'],update['alternate_email'],update['direct_phone'],update['mobile_phone'],update['title_display'],update['accred_display'],update['currentrolename'],update['user_category'],update['title_cleaned'],update['team_name'],update['team_member_type'],update['date_deactivated'],update['agent_active'],update['at_least_one_mls_association'],update['agent_moxi_hub'],update['agent_moxi_dms'],update['agent_moxi_engage'],update['agent_moxi_present'],update['agent_moxi_websites'],update['agent_moxi_talent'],update['sf_parent_id'],update['agent_uuid'],update['sfdc_contact_id'],parent_account,get_agent_data['isactivated'],isactive_in_database, status))
                    print('Data inserted in moxi_contact_delta....')
                    conn.commit()
                    total_updated = total_updated + 1
                except Exception as e:
                    logf.write("Failed to insert uuid {0}: {1}\n".format(str(update['agent_uuid']), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(update['agent_uuid']))
                    conn.rollback()
                    continue
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/agent_insert_report'
        full_path = directory + store
        #Agent Insert report
        with open(full_path + "/moxiworks_agent_insert_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            # fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'nickname', 'officename_display', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'office_extension', 'company_legalname', 'companypublickey', 'lastmodified', 'primary_email', 'secondary_email', 'alternate_email', 'direct_phone', 'mobile_phone', 'title_display', 'accred_display', 'currentrolename', 'user_category', 'title_cleaned', 'team_name', 'team_member_type', 'date_deactivated', 'agent_active', 'at_least_one_mls_association', 'agent_moxi_hub', 'agent_moxi_dms', 'agent_moxi_engage', 'agent_moxi_present', 'agent_moxi_websites', 'agent_moxi_talent']
            fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'officename_display', 'officepublickey', 'company_legalname', 'companypublickey', 'Status']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for insert in insert_array:
                # writer.writerow({'agent_uuid':insert['agent_uuid'], 'userid':insert['userid'], 'agent_username':insert['agent_username'], 'firstname':insert['firstname'], 'lastname':insert['lastname'], 'nickname':insert['nickname'], 'officename_display':insert['officename_display'], 'officepublickey':insert['officepublickey'], 'office_addressline1':insert['office_addressline1'], 'office_city':insert['office_city'], 'office_state':insert['office_state'], 'office_zip':insert['office_zip'], 'office_phone':insert['office_phone'], 'office_extension':insert['office_extension'], 'company_legalname':insert['company_legalname'], 'companypublickey':insert['companypublickey'], 'lastmodified':insert['lastmodified'], 'primary_email':insert['primary_email'], 'secondary_email':insert['secondary_email'], 'alternate_email':insert['alternate_email'], 'direct_phone':insert['direct_phone'], 'mobile_phone':insert['mobile_phone'], 'title_display':insert['title_display'], 'accred_display':insert['accred_display'], 'currentrolename':insert['currentrolename'], 'user_category':insert['user_category'], 'title_cleaned':insert['title_cleaned'], 'team_name':insert['team_name'], 'team_member_type':insert['team_member_type'], 'date_deactivated':insert['date_deactivated'], 'agent_active':insert['agent_active'], 'at_least_one_mls_association':insert['at_least_one_mls_association'], 'agent_moxi_hub':insert['agent_moxi_hub'], 'agent_moxi_dms':insert['agent_moxi_dms'], 'agent_moxi_engage':insert['agent_moxi_engage'], 'agent_moxi_present':insert['agent_moxi_present'], 'agent_moxi_websites':insert['agent_moxi_websites'], 'agent_moxi_talent':insert['agent_moxi_talent']})
                writer.writerow({'agent_uuid':insert['agent_uuid'], 'userid':insert['userid'], 'agent_username':insert['agent_username'], 'firstname':insert['firstname'], 'lastname':insert['lastname'], 'officename_display':insert['officename_display'], 'officepublickey':insert['officepublickey'], 'company_legalname':insert['company_legalname'], 'companypublickey':insert['companypublickey'], 'Status': 'Insert'})
                cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s LIMIT 1', (insert['companypublickey'],))
                get_company_data = cur.fetchone()
                parent_account = get_company_data['sfdc_account_id']
                total_data = total_data + 1
                if str(insert['date_deactivated']) != '' and insert['agent_active'] == False:
                    isactive_in_database = 'f'
                else:
                    isactive_in_database = 't'
                status = 'insert'
                try:
                    cur.execute(
                    'INSERT INTO moxiworks.moxiworks_contact (userid,agent_username,firstname,lastname,nickname,officename_display,officepublickey,office_addressline1,office_city,office_state,office_zip,office_phone,office_extension,company_legalname,companypublickey,lastmodified,primary_email,secondary_email,alternate_email,direct_phone,mobile_phone,title_display,accred_display,currentrolename,user_category,title_cleaned,team_name,team_member_type,date_deactivated,agent_active,at_least_one_mls_association,agent_moxi_hub,agent_moxi_dms,agent_moxi_engage,agent_moxi_present,agent_moxi_websites,agent_moxi_talent,sf_parent_id,agent_uuid,parent_account,isactive) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)',
                    (insert['userid'],insert['agent_username'],insert['firstname'],insert['lastname'],insert['nickname'],insert['officename_display'],insert['officepublickey'],insert['office_addressline1'],insert['office_city'],insert['office_state'],insert['office_zip'],insert['office_phone'],insert['office_extension'],insert['company_legalname'],insert['companypublickey'],insert['lastmodified'],insert['primary_email'],insert['secondary_email'],insert['alternate_email'],insert['direct_phone'],insert['mobile_phone'],insert['title_display'],insert['accred_display'],insert['currentrolename'],insert['user_category'],insert['title_cleaned'],insert['team_name'],insert['team_member_type'],insert['date_deactivated'],insert['agent_active'],insert['at_least_one_mls_association'],insert['agent_moxi_hub'],insert['agent_moxi_dms'],insert['agent_moxi_engage'],insert['agent_moxi_present'],insert['agent_moxi_websites'],insert['agent_moxi_talent'],insert['sf_parent_id'],insert['agent_uuid'],parent_account,isactive_in_database))
                    print('Data inserted in moxi_contact....')
                    cur.execute(
                    'INSERT INTO moxiworks.moxiworks_contact_delta (userid,agent_username,firstname,lastname,nickname,officename_display,officepublickey,office_addressline1,office_city,office_state,office_zip,office_phone,office_extension,company_legalname,companypublickey,lastmodified,primary_email,secondary_email,alternate_email,direct_phone,mobile_phone,title_display,accred_display,currentrolename,user_category,title_cleaned,team_name,team_member_type,date_deactivated,agent_active,at_least_one_mls_association,agent_moxi_hub,agent_moxi_dms,agent_moxi_engage,agent_moxi_present,agent_moxi_websites,agent_moxi_talent,sf_parent_id,agent_uuid,parent_account,isactive,status) VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s,%s)',
                    (insert['userid'],insert['agent_username'],insert['firstname'],insert['lastname'],insert['nickname'],insert['officename_display'],insert['officepublickey'],insert['office_addressline1'],insert['office_city'],insert['office_state'],insert['office_zip'],insert['office_phone'],insert['office_extension'],insert['company_legalname'],insert['companypublickey'],insert['lastmodified'],insert['primary_email'],insert['secondary_email'],insert['alternate_email'],insert['direct_phone'],insert['mobile_phone'],insert['title_display'],insert['accred_display'],insert['currentrolename'],insert['user_category'],insert['title_cleaned'],insert['team_name'],insert['team_member_type'],insert['date_deactivated'],insert['agent_active'],insert['at_least_one_mls_association'],insert['agent_moxi_hub'],insert['agent_moxi_dms'],insert['agent_moxi_engage'],insert['agent_moxi_present'],insert['agent_moxi_websites'],insert['agent_moxi_talent'],insert['sf_parent_id'],insert['agent_uuid'],parent_account,isactive_in_database, status))
                    print('Data inserted in moxi_contact_delta....')
                    conn.commit()
                    total_inserted = total_inserted + 1
                except Exception as e:
                    logf.write("Failed to insert uuid {0}: {1}\n".format(str(insert['agent_uuid']), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(insert['agent_uuid']))
                    conn.rollback()
                    continue
        print("Temp Table Total Data Inserted: " + str(temp_table_inserted))
        # print("Temp Table Total Duplicate Data: " + str(temp_table_duplicate))
        print("Temp Table Total Error Data: " + str(temp_table_garbage))
        print("Temp Table Total Data Processed: " + str(temp_table_data_processed))
        print("Total Data Inserted: " + str(total_inserted))
        print("Total Data Updated: " + str(total_updated))
        print("Total Data Processed: " + str(total_data))
        # seperator = "','"
        # error_array = seperator.join(error_array)
        # directory = str(os.path.dirname(os.path.realpath(__file__)))
        # store = '/storage/agent_error_report'
        # full_path = directory + store
        # cur.execute("\copy (SELECT agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_addressline2, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, reason FROM moxiworks.moxiworks_contact_ongoing_report where agent_uuid in ('{}".format(error_array)+"')) to '{}".format(full_path)+"/moxiworks_agent_error_report_{}".format(datetime.date.today()) +".csv' with csv header")
        send_agent_report('Agent',total_data, total_inserted, total_updated,temp_table_data_processed,temp_table_garbage,temp_table_inserted)

    def import_agent_data(self, conn):
        # Initialization 
        total_data = 0
        total_inserted = 0
        garbage_count = 0
        duplicate_count = 0
        reason = ""
        error_array = []
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Agent/Moxi-Agents_{}'.format(datetime.date.today()) + '.csv'
        # file_name = 'Agent/Moxi-Agents_2019-02-06.csv'
        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return 'no_new_file'

        local_file_name = 'Moxi-Agents_{}'.format(datetime.date.today()) + '.csv'
        # local_file_name = 'Moxi-Agents_2019-02-06.csv'
        # logf = open("agent_error_{}".format(datetime.date.today()) + ".log", "w")
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        filename = 'storage/' + local_file_name
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/agent_error_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_agent_error_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['agent_uuid', 'userid', 'agent_username', 'firstname', 'lastname', 'nickname', 'officename_display', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'office_extension', 'company_legalname', 'companypublickey', 'lastmodified', 'primary_email', 'secondary_email', 'alternate_email', 'direct_phone', 'mobile_phone', 'title_display', 'accred_display', 'currentrolename', 'user_category', 'title_cleaned', 'team_name', 'team_member_type', 'date_deactivated', 'agent_active', 'at_least_one_mls_association', 'agent_moxi_hub', 'agent_moxi_dms', 'agent_moxi_engage', 'agent_moxi_present', 'agent_moxi_websites', 'agent_moxi_talent', 'reason']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for data in csv_raw_data:
                reason_array = []
                total_data = total_data + 1
                print('Data processing ' + str(total_data))
                agent_uuid = str(data['agent_uuid']).strip()
                userid = str(data['userid']).strip()
                agent_username = str(data['agent_username']).strip()
                firstname = str(data['firstname']).strip()
                lastname = str(data['lastname']).strip()
                firstname = str(data['firstname']).strip()
                if len(firstname) > 40:
                    firstname = str(firstname[:40]).strip()
                lastname = str(data['lastname']).strip()
                if len(lastname) > 40:
                    lastname = str(lastname[:40]).strip()
                nickname = str(data['nickname']).strip()
                officename_display = str(data['officename_display']).strip()
                officepublickey = str(data['officepublickey']).strip()
                office_addressline1 = str(str(data['office_addressline1']).strip()) + " " + str(str(data['office_addressline2']).strip())
                office_city = str(data['office_city']).strip()
                office_state = str(data['office_state']).strip()
                office_zip = str(data['office_zip']).strip()
                office_phone = str(data['office_phone']).strip()
                office_extension = str(data['office_extension']).strip()
                company_legalname = str(data['company_legalname']).strip()
                companypublickey = str(data['companypublickey']).strip()
                lastmodified = str(data['lastmodified']).strip()
                primary_email = str(data['primary_email']).strip()
                secondary_email = str(data['secondary_email']).strip()
                alternate_email = str(data['alternate_email']).strip()
                direct_phone = str(data['direct_phone']).strip()
                mobile_phone = str(data['mobile_phone']).strip()
                title_display = str(data['title_display']).strip()
                accred_display = str(data['accred_display']).strip()
                if len(accred_display) > 40:
                    accred_display = str(accred_display[:40]).strip()
                currentrolename = str(data['currentrolename']).strip()
                user_category = str(data['user_category']).strip()
                title_cleaned = str(data['title_cleaned']).strip()
                team_name = str(data['team_name']).strip()
                team_member_type = str(data['team_member_type']).strip()
                date_deactivated_csv = data['date_deactivated']
                if not date_deactivated_csv:
                    date_deactivated = None
                else:
                    # format_str = '%m/%d/%Y' # The format
                    # date_deactivated = datetime.datetime.strptime(date_deactivated_csv, format_str)
                    # date_deactivated = str(date_deactivated)
                    date_deactivated = data['date_deactivated']
                agent_active = to_bool(data['agent_active'])
                at_least_one_mls_association = to_bool(data['at_least_one_mls_association'])
                agent_moxi_hub = to_bool(data['agent_moxi_hub'])
                agent_moxi_dms = to_bool(data['agent_moxi_dms'])
                agent_moxi_engage = to_bool(data['agent_moxi_engage'])
                agent_moxi_present = to_bool(data['agent_moxi_present'])
                agent_moxi_websites = to_bool(data['agent_moxi_websites'])
                agent_moxi_talent = to_bool(data['agent_moxi_talent'])
                firstname_lower = str(firstname).lower()
                lastname_lower = str(lastname).lower()
                primary_email_lower = str(primary_email).lower()
                alt_email_lower = str(alternate_email).lower()
                agent_username_lower = str(agent_username).lower()
                company_legalname_lower = str(company_legalname).lower()
                officename_display_lower = str(officename_display).lower()
                validation_array_1 = ["training","support","admin","poptartcat","alternate_email","gapps","tester","account","services","administrator","agency","office admin","autobot","borg0","demo1","llc","delete","property mgmt","ecommadmin","licenses","tempcontract","facebook.test","fakey","fakerson","moxiworks.com","@tmren.biz","touchcma","@mw","demoagent","internet","web server","subscriptino","testmgr","demo.account"]
                email_validation_array = ["autobot","apiuser","twosteptest","jane.agent","geotest","testing.email","teading.test","@notreal","@qatest.com","@windermeretest","@testing.com","@faketest.com","testuser@","testing@","@moxiworks.com","@touchcma.com","@sweeper.com","@sweepre.com","@sweeperre.com","qa+","noreply","info@","@windermeresolutions.com","qa-test","@mw.com","moxi","autobot","ecomm","borg000","admin","@tmren.biz","moxi_testagent","information@","ecommadmin","xxx","testsubject","training","@tests","testagent","@test.com","@fake.com","moxiworks","moxitest","fakertest","moxi.works","testmgr","sprunt.qa+marchregression","youremail","moxie_qa","fakemlstest","moxiworksdemo"]
                office_validation_array = ["windermere solutions llc","bean group","automation test","branding demo","demo only","gapps moxiworks","moxiworks","google auth test","hosted moxiworks 2013","moxiworks tst acct","test office","windermeresolutions","moxi works headquarters","client services","test company","moxiworks llc"]
                company_validation_array = ["bean group","automation test","branding demo","demo only","gapps moxiworks","moxiworks","google auth test","hosted moxiworks 2013","moxiworks tst acct","test office","windermeresolutions","moxi works headquarters","client services","test company","moxiworks llc"]
                phone_validation_array = ["222-222-2222","555-555-5555","333-333-3333","206-638-8478","999-999-9999"]
                publickey_validation_array = ["3139213","1624333","11195586"]

                if companypublickey:
                    cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s', (companypublickey,))
                    get_account_data = cur.fetchone()
                    if get_account_data is not None:
                        if get_account_data['isactive'] == 'f':
                            reason = "Related company is not active"
                            reason_array.append(reason)
                        if get_account_data['sfdc_account_id'] == '':
                            reason = "Related company does not exist in Salesforce"
                            reason_array.append(reason)
                else:
                    reason = "Company public key field is empty"
                    reason_array.append(reason)


                #first name validation
                
                if str(firstname) == "Demo":
                    reason = "First name contains Demo"
                    reason_array.append(reason)
                
                if str(firstname) == "Test":
                    reason = "First name contains Test"
                    reason_array.append(reason)
                if str(firstname) == "Data":
                    reason = "First name contains Data"
                    reason_array.append(reason)

                for value in validation_array_1:
                    if firstname_lower.find(value) != -1:
                        reason = "First name contains " + value
                        reason_array.append(reason)

                #first name validation end

                # Last name validation

                if str(lastname) == "Demo":
                    reason = "Last name contains Demo"
                    reason_array.append(reason)
                
                if str(lastname) == "Test":
                    reason = "Last name contains Test"
                    reason_array.append(reason)
                if str(lastname) == "Data":
                    reason = "Last name contains Data"
                    reason_array.append(reason)
                for value in validation_array_1:
                    if lastname_lower.find(value) != -1:
                        reason = "Last name contains " + value
                        reason_array.append(reason)

                # Last name validation end

                # Agent username validation

                if str(agent_username) == "Demo":
                    reason = "Agent username contains Demo"
                    reason_array.append(reason)
                
                if str(agent_username) == "Test":
                    reason = "Agent username contains Test"
                    reason_array.append(reason)
                if str(agent_username) == "Data":
                    reason = "Agent username contains Data"
                    reason_array.append(reason)
                for value in validation_array_1:
                    if agent_username_lower.find(value) != -1:
                        reason = "Agent username contains " + value
                        reason_array.append(reason)

                # Agent username validation end

                # Primary email validation
                if primary_email:
                    if ' ' in primary_email_lower:
                        reason = "Primary email contains space in the address"
                        reason_array.append(reason)
                    is_valid_primary_email = validate_email(primary_email)
                    if is_valid_primary_email == 1:
                        for value in email_validation_array:
                            if primary_email_lower.find(value) != -1:
                                reason = "Primary email contains " + value
                                reason_array.append(reason)
                    else:
                        reason = "Primary email is invalid"
                        reason_array.append(reason)
                else:
                    reason = "Primary email field is empty"
                    reason_array.append(reason)
                # Primary email validation end

                # Alternate email validation
                if alternate_email:
                    if ' ' in alt_email_lower:
                        reason = "Alternate email contains space in the address"
                        reason_array.append(reason)
                    is_valid_alt_email = validate_email(alternate_email)
                    if is_valid_alt_email == 1:
                        for value in email_validation_array:
                            if alt_email_lower.find(value) != -1:
                                reason = "Alternate email contains " + value
                                reason_array.append(reason)
                    else:
                        reason = "Alternate email is invalid"
                        reason_array.append(reason)
                # Alternate email validation end
                # Company legal name validation
                if str(company_legalname) == "Demo":
                    reason = "Company legal name contains Demo"
                    reason_array.append(reason)
                for value in company_validation_array:
                    if company_legalname_lower.find(value) != -1:
                        reason = "Company legal name contains " + value
                        reason_array.append(reason)
                # Company legal name validation end

                # Office name display validation
                if str(officename_display) == "Demo":
                    reason = "Office name display contains Demo"
                    reason_array.append(reason)
                for value in office_validation_array:
                    if officename_display_lower.find(value) != -1:
                        reason = "Office name display contains " + value
                        reason_array.append(reason)
                # Office name display validation end
                # Direct phone validation
                for value in phone_validation_array:
                    if str(direct_phone) == value:
                        reason = "Direct phone contains " + value
                        reason_array.append(reason)
                # Direct phone validation end
                # Mobile phone validation
                for value in phone_validation_array:
                    if str(mobile_phone) == value:
                        reason = "Mobile phone contains " + value
                        reason_array.append(reason)
                # Mobile phone validation end
                # companypublickey validation
                for value in publickey_validation_array:
                    if str(companypublickey) == value:
                        reason = "Compnaypublickey contains " + value
                        reason_array.append(reason)
                # companypublickey validation end
                # officepublickey validation
                for value in publickey_validation_array:
                    if str(officepublickey) == value:
                        reason = "Officepublickey contains " + value
                        reason_array.append(reason)
                # officepublickey validation end
                if officepublickey:
                    cur.execute('SELECT * from moxiworks.moxiworks_office where officepublickey = %s', (officepublickey,))
                    get_office_data = cur.fetchone()
                    
                    if not get_office_data:
                        reason = "Officepublickey does not exists in office table"
                        reason_array.append(reason)

                    else:
                        sf_parent_id = get_office_data['sfdc_office_id']
                else:
                    cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s', (companypublickey,))
                    get_company_data = cur.fetchone()
                    if not get_company_data:
                        reason = "Companypublickey does not exists in company table"
                        reason_array.append(reason)
                    else:
                        sf_parent_id = get_company_data['sfdc_account_id']
                # Duplicate Count
                cur.execute('SELECT count(*) from moxiworks.moxiworks_contact_ongoing where agent_uuid = %s', (agent_uuid,))
                check_duplicate_agent = cur.fetchone()
                if check_duplicate_agent[0] > 0:
                    duplicate_count = duplicate_count + 1
                    reason = "This data is a duplicate of agent uuid: " + str(agent_uuid)
                    reason_array.append(reason)
                cur.execute('SELECT * from moxiworks.moxiworks_contact where agent_uuid = %s', (agent_uuid,))
                get_contact_data = cur.fetchone()
                if not get_contact_data:
                    sfdc_agent_id = ''
                else:
                    sfdc_agent_id = get_contact_data['sfdc_contact_id']
                    if get_contact_data['title_cleaned']:
                        title = get_contact_data['title_cleaned']
                    else:
                        title = title_cleaned
                
                if not reason_array:
                    try:
                        cur.execute(
                                'INSERT INTO moxiworks.moxiworks_contact_ongoing (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_contact_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_agent_id))
                        conn.commit()
                        print('Data Inserted in moxiworks_contact_ongoing....')
                        total_inserted = total_inserted + 1
                    except Exception as e:
                        print("type error: " + str(e))
                        print("Failed at ID " + str(agent_uuid))
                        # logf.write("Failed to insert uuid {0}: {1}\n".format(str(agent_uuid), str(e)))
                        conn.rollback()
                        continue
                else:
                    seperator = ', '
                    reasons = seperator.join(reason_array)
                    error_array.append(str(agent_uuid))
                    writer.writerow({'agent_uuid':agent_uuid, 'userid':userid, 'agent_username':agent_username, 'firstname':firstname, 'lastname':lastname, 'nickname':nickname, 'officename_display':officename_display, 'officepublickey':officepublickey, 'office_addressline1':office_addressline1, 'office_city':office_city, 'office_state':office_state, 'office_zip':office_zip, 'office_phone':office_phone, 'office_extension':office_extension, 'company_legalname':company_legalname, 'companypublickey':companypublickey, 'lastmodified':lastmodified, 'primary_email':primary_email, 'secondary_email':secondary_email, 'alternate_email':alternate_email, 'direct_phone':direct_phone, 'mobile_phone':mobile_phone, 'title_display':title_display, 'accred_display':accred_display, 'currentrolename':currentrolename, 'user_category':user_category, 'title_cleaned':title_cleaned, 'team_name':team_name, 'team_member_type':team_member_type, 'date_deactivated':date_deactivated, 'agent_active':agent_active, 'at_least_one_mls_association':at_least_one_mls_association, 'agent_moxi_hub':agent_moxi_hub, 'agent_moxi_dms':agent_moxi_dms, 'agent_moxi_engage':agent_moxi_engage, 'agent_moxi_present':agent_moxi_present, 'agent_moxi_websites':agent_moxi_websites, 'agent_moxi_talent':agent_moxi_talent, 'reason':reasons})
                    cur.execute('Select count(*) from moxiworks.moxiworks_contact_ongoing_report where agent_uuid = %s',
                                    (agent_uuid,))
                    check_duplicate = cur.fetchone()
                    if check_duplicate[0] > 0:
                        try:
                            cur.execute("UPDATE moxiworks.moxiworks_contact_ongoing_report SET reason = %s where agent_uuid = %s",
                            (reasons, agent_uuid))
                            conn.commit()
                        except Exception as e:
                            print("Failed at Agent public key " + str(agent_uuid))
                            conn.rollback()
                            continue
                    else:
                        try:
                            cur.execute(
                                    'INSERT INTO moxiworks.moxiworks_contact_ongoing_report (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_contact_id,reason) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_agent_id, reasons))
                            conn.commit()
                            print('Data Inserted in moxiworks_contact_ongoing_report....')
                            
                        except Exception as e:
                            # logf.write("Failed to insert uuid {0}: {1}\n".format(str(agent_uuid), str(e)))
                            print("type error: " + str(e))
                            print("Failed at Company public key " + str(agent_uuid))
                            conn.rollback()
                            continue
                    garbage_count = garbage_count + 1
        
        # print("Temp Table Total Data Inserted: " + str(total_inserted))
        # print("Temp Table Total Duplicate Data: " + str(duplicate_count))
        # print("Temp Table Total Garbage Data: " + str(garbage_count))
        # print("Temp Table Total Data Processed: " + str(total_data))
        self.import_in_main_and_delta_table(conn,total_inserted,duplicate_count,garbage_count,total_data)
        os.unlink(filename)
        cur.execute("TRUNCATE TABLE moxiworks.moxiworks_contact_ongoing")
        conn.commit()
      
# #Created a object of the class
# import_agent_daily_data = import_agent_daily_data()
# #create the connection
# conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
#     'db_user') + " password=" + os.getenv('db_password'))
# #call the import_company_inital_data function
# # import_company_daily_data.import_company_data(conn)
# import_agent_daily_data.import_agent_data(conn)
            

