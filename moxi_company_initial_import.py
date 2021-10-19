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
from common import to_bool, send_company_report
import glob
import uuid
import csvdiff

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


class import_inital_data():
    def import_company_inital_data(self, conn):
        # Initialization 
        total_data = 0
        total_inserted = 0
        total_updated = 0
        garbage_count = 0
        duplicate_count = 0
        collect_duplicate_array = []
        collect_garbage_array = []
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Company/Moxi_Companies_initial_{}'.format(datetime.date.today()) + '.csv'
        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        local_file_name = 'Moxi_Companies_initial_{}'.format(datetime.date.today()) + '.csv'
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))

        for csv_data in csv_raw_data:
            total_data = total_data + 1
            #Ckeck if active customer column is empty in csv. if true take it as false
            if 'active_customer' in csv_data:
                active_customer = str(csv_data['active_customer']).lower()
            else:
                active_customer = 'f'
            #Assign the csv fields to variables
            sf_parent_id = csv_data['sf_parent_id']
            sf_account_id = csv_data['sf_account_id']
            company_legalname = csv_data['company_legalname']
            company_name = csv_data['company_name']
            companypublickey = csv_data['companypublickey']
            company_addressline1 = str(csv_data['company_addressline1']) + " " + str(csv_data['company_addressline2'])
            company_city = csv_data['company_city']
            company_state = csv_data['company_state']
            company_zipcode = csv_data['company_zipcode']
            company_phone = csv_data['company_phone']
            company_webpage = csv_data['company_webpage']
            agent_count = csv_data['agent_count']
            company_moxi_hub = to_bool(csv_data['company_moxi_hub'])
            company_moxi_dms = to_bool(csv_data['company_moxi_dms'])
            company_moxi_engage = to_bool(csv_data['company_moxi_engage'])
            company_moxi_present = to_bool(csv_data['company_moxi_present'])
            company_moxi_websites = to_bool(csv_data['company_moxi_websites'])
            company_moxi_talent = to_bool(csv_data['company_moxi_talent'])
            company_moxi_insights = to_bool(csv_data['company_moxi_insights'])
            show_in_product_marketing = to_bool(csv_data['show_in_product_marketing'])
            advertise_your_listing = to_bool(csv_data['advertise_your_listing'])
            ayl_emails = to_bool(csv_data['ayl_emails'])
            advertise_your_services = to_bool(csv_data['advertise_your_services'])
            ays_emails = to_bool(csv_data['ays_emails'])
            direct_marketing = to_bool(csv_data['direct_marketing'])
            status = 'Update'
            account_type = 'company'
            uniq_id = str(uuid.uuid4())
            duplicate_array = {'sf_account_id': sf_account_id, 'companypublickey': companypublickey}
            garbage_array = {'sf_account_id': sf_account_id, 'companypublickey': companypublickey, 'reason': ''}
            #check if it is a active customer
            if active_customer == 'f':
                continue
            #Check if SFDC ID or companypublickey field is csv is empty
            if not sf_account_id or not companypublickey:
                garbage_array['reason'] = 'sf_account_id and companypublickey both are none.'
                collect_garbage_array.append(garbage_array)
                garbage_count = garbage_count + 1
                continue
            #If found SFDC ID then check if it exists in database
            cur.execute("SELECT * FROM moxiworks.moxiworks_account WHERE sfdc_account_id = '{}' LIMIT 1".format(
                sf_account_id))
            check_sfdc_id = cur.fetchone()

            if check_sfdc_id:
                #if SFDC ID found in database check the companypublickey
                if companypublickey:
                    cur.execute('Select count(*) from moxiworks.moxiworks_account where companypublickey = %s',
                                (companypublickey,))
                    check_duplicate = cur.fetchone()
                    #If companypublickey found also then its a duplicate entry
                    if check_duplicate[0] > 0:
                        collect_duplicate_array.append(duplicate_array)
                        duplicate_count = duplicate_count + 1
                        continue
                    #If not then it is considered as garbage data and skipped
                    else:
                        garbage_array['reason'] = 'SFDC Id duplicate found but companypublickey doesn\'t match.'
                        collect_garbage_array.append(garbage_array)
                        garbage_count = garbage_count + 1
                        continue
                #If sfdc ID not found in database then check if companykey exist
            else:
                if companypublickey:
                    cur.execute('Select count(*) from moxiworks.moxiworks_account where companypublickey = %s',
                                (companypublickey,))
                    check_exists = cur.fetchone()
                    #if companypublickey exists with out SFDC ID then we consider it garbage
                    if check_exists[0] > 0:
                        garbage_array['reason'] = 'companypublickey already exists but sfdc_account_id not found.'
                        collect_garbage_array.append(garbage_array)
                        garbage_count = garbage_count + 1
                        continue
                    else:
                        # New entries are inserted in database
                        try:
                            cur.execute(
                                'INSERT INTO moxiworks.moxiworks_account (internal_key,company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, account_type, sfdc_account_id,sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (uniq_id, company_legalname, company_name, companypublickey,
                                 company_addressline1,
                                 company_city, company_state, company_zipcode, company_phone,
                                 company_webpage, agent_count, company_moxi_hub, company_moxi_dms,
                                 company_moxi_engage,
                                 company_moxi_present, company_moxi_websites, company_moxi_talent,
                                 company_moxi_insights,
                                 show_in_product_marketing, advertise_your_listing, ayl_emails,
                                 advertise_your_services,
                                 ays_emails, direct_marketing, account_type, sf_account_id, sf_parent_id))
                            print('Data Inserted in moxiworks_account....')
                            cur.execute(
                                'INSERT INTO moxiworks.moxiworks_account_delta (internal_key,company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub,company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, account_type, status, sfdc_account_id,sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)',
                                (uniq_id, company_legalname, company_name, companypublickey,
                                 company_addressline1,
                                 company_city, company_state, company_zipcode, company_phone,
                                 company_webpage, agent_count, company_moxi_hub, company_moxi_dms,
                                 company_moxi_engage,
                                 company_moxi_present, company_moxi_websites, company_moxi_talent,
                                 company_moxi_insights,
                                 show_in_product_marketing, advertise_your_listing, ayl_emails,
                                 advertise_your_services,
                                 ays_emails, direct_marketing, account_type, status, sf_account_id,
                                 sf_parent_id))
                            print('Data Inserted in moxiworks_account_delta with Update status....')
                            conn.commit()
                            total_inserted = total_inserted + 1
                        except Exception as e:
                            print("type error: " + str(e))
                            print("Failed at ID " + str(csv_data['companypublickey']))
                            conn.rollback()
                            break
        print ('Printing Duplicate entries and reason:')
        # Exporting a csv of duplicate entries
        with open('company_duplicate_report.csv', mode='w') as csv_file:     
            fieldnames = ['companypublickey','sf_account_id']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for duplicate in collect_duplicate_array:
                print('Office Public Key: ' + duplicate['companypublickey'] +' SF_Account_ID: ' + duplicate['sf_account_id'])
                writer.writerow({'companypublickey':duplicate['companypublickey'],'sf_account_id':duplicate['sf_account_id']})
        print ('Printing garbage entries and reason')
        # Exporting a csv of garbage entries
        with open('company_garbage_report.csv', mode='w') as csv_file:     
            fieldnames = ['companypublickey','sf_account_id','Reason']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for garbage in collect_garbage_array:
                print('Office Public Key: ' + garbage['companypublickey'] +' SF_Account_ID: ' + garbage['sf_account_id']+' Reason: ' + garbage['reason'])
                writer.writerow({'companypublickey':garbage['companypublickey'],'sf_account_id':garbage['sf_account_id'],'Reason':garbage['reason']})

        print("Total Data Inserted: " + str(total_inserted))
        print("Total Duplicate Data: " + str(duplicate_count))
        print("Total Garbage Data: " + str(garbage_count))
        print("Total Data Processed: " + str(total_data))

        # send_company_report('Company', total_data, total_inserted, total_updated)

#Created a object of the class
import_init_data_obj = import_inital_data()
#create the connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
#call the import_company_inital_data function
import_init_data_obj.import_company_inital_data(conn)
