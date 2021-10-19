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
        total_inserted = 0
        garbage_count = 0
        duplicate_count = 0
        insert_flag = 1
        reason = ""
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
        # with open('storage/reports/agent/agent_invalid_data_report.csv', mode='w') as csv_file:
        #     fieldnames = ['agent_uuid','userid','agent_username','firstname','lastname','nickname','officename_display','officepublickey','office_addressline1','office_addressline2','office_city','office_state','office_zip','office_phone','office_extension','company_legalname','companypublickey','lastmodified','primary_email','secondary_email','alternate_email','direct_phone','mobile_phone','title_display','accred_display','currentrolename','user_category','title_cleaned','team_name','team_member_type','date_deactivated','agent_active','at_least_one_mls_association','agent_moxi_hub','agent_moxi_dms','agent_moxi_engage','agent_moxi_present','agent_moxi_websites','agent_moxi_talent','Reason']
        #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        #     writer.writeheader()
        logf = open("error.log", "w")
        for data in csv_raw_data:
            insert_flag = 1
            reason_array = []
            total_data = total_data + 1
            print('Data processing ' + str(total_data))
            agent_uuid = data['agent_uuid']
            userid = data['userid']
            agent_username = data['agent_username']
            firstname = data['firstname']
            lastname = data['lastname']
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
            currentrolename = data['currentrolename']
            user_category = data['user_category']
            title_cleaned = data['title_cleaned']
            team_name = data['team_name']
            team_member_type = data['team_member_type']
            date_deactivated = data['date_deactivated']
            agent_active = data['agent_active']
            at_least_one_mls_association = data['at_least_one_mls_association']
            agent_moxi_hub = data['agent_moxi_hub']
            agent_moxi_dms = data['agent_moxi_dms']
            agent_moxi_engage = data['agent_moxi_engage']
            agent_moxi_present = data['agent_moxi_present']
            agent_moxi_websites = data['agent_moxi_websites']
            agent_moxi_talent = data['agent_moxi_talent']
            firstname_lower = str(firstname).lower()
            lastname_lower = str(lastname).lower()
            primary_email_lower = str(primary_email).lower()
            alt_email_lower = str(alternate_email).lower()
            agent_username_lower = str(agent_username).lower()
            company_legalname_lower = str(company_legalname).lower()
            officename_display_lower = str(officename_display).lower()
            validation_array_1 = ["training","support","admin","poptartcat","alternate_email","gapps","tester","aad","account","services","administrator","agency","ipad","office admin","autobot","borg0","demo1","llc","delete","property mgmt","ecommadmin","licenses","tempcontract","facebook.test","#","fakey","fakerson","moxiworks.com","@tmren.biz","touchcma","@mw","demoagent","internet","web server","subscriptino","testmgr"]
            email_validation_array = ["@moxiworks.com","@touchcma.com","@sweeper.com","@sweepre.com","@sweeperre.com","test","qa+","noreply","info@","@windermeresolutions.com","qa-test","@mw.com","moxi","autobot","ecomm","borg000","admin","@tmren.biz","moxi_testagent","information@","#","ecommadmin","xxx","testsubject","training","@tests","testagent","@test.com","@fake.com","moxiworks","moxitest","fakertest","moxi.works","testmgr","sprunt.qa+marchregression","youremail","moxie_qa","fakemlstest","moxiworksdemo"]
            company_office_validation_array = ["windermere solutions llc","bean group","automation test","branding demo","demo only","gapps moxiworks","moxiworks","google auth test","hosted moxiworks 2013","moxiworks tst acct","test office","windermeresolutions","moxi works headquarters","client services","test company","moxiworks llc"]
            phone_validation_array = ["222-222-2222","555-555-5555","333-333-3333","206-638-8478"]
            publickey_validation_array = ["3139213","1624333","11195586"]
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
                if primary_email_lower.find(" ") != -1:
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
                if alt_email_lower.find(" ") != -1:
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
            # else:
            #     reason = "Alternate email field is empty"
            #     try:
            #         cur.execute(
            #             'INSERT INTO moxiworks.moxiworks_contact_report (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, reason) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            #             (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, reason))
            #         print('Data Inserted for moxiworks_contact_report....')
            #         conn.commit()
            #         garbage_count = garbage_count + 1
            #         insert_flag = 0
            #     except Exception as e:
            #         print("type error: " + str(e))
            #         print("Failed at ID " + str(data['agent_uuid']))
            #         conn.rollback()
            #         continue
            # Alternate email validation end
            # Company legal name validation
            if str(company_legalname) == "Demo":
                reason = "Company legal name contains Demo"
                reason_array.append(reason)
            for value in company_office_validation_array:
                if company_legalname_lower.find(value) != -1:
                    reason = "Company legal name contains " + value
                    reason_array.append(reason)
            # Company legal name validation end

            # Office name display validation
            if str(officename_display) == "Demo":
                reason = "Office name display contains Demo"
                reason_array.append(reason)
            for value in company_office_validation_array:
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
            # Duplicate Count
            cur.execute('SELECT count(*) from moxiworks.moxiworks_contact where agent_uuid = %s', (agent_uuid,))
            check_duplicate_agent = cur.fetchone()
            if check_duplicate_agent[0] > 0:
                duplicate_count = duplicate_count + 1
                reason = "This data is a duplicate of agent uuid: " + str(agent_uuid)
                reason_array.append(reason)
            if not reason_array:
                
                try:
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id))
                    print('Data Inserted in moxiworks_contact....')
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
            else:
                seperator = ', '
                reasons = seperator.join(reason_array)
                
                try:
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact_report (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, reason) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, reasons))
                    print('Data Inserted for moxiworks_contact_report....')
                    conn.commit()
                    garbage_count = garbage_count + 1
                except Exception as e:
                    logf.write("Failed to insert uuid {0}: {1}\n".format(str(agent_uuid), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(data['agent_uuid']))
                    conn.rollback()
                    continue
        print("Total Inserted: " + str(total_inserted))
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
                
                