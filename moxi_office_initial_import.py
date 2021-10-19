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
from common import to_bool, send_office_report
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
    def import_office_inital_data(self, conn):
        # Initialization 
        total_data = 0
        total_inserted = 0
        total_updated = 0
        duplicate_count = 0
        garbage_count = 0
        collect_duplicate_array = []
        collect_garbage_array = []
        cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Office/Moxi_Offices_initial_{}'.format(datetime.date.today()) + '.csv'

        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        local_file_name = 'Moxi_Offices_initial_{}'.format(datetime.date.today()) + '.csv'
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))


        for data in csv_raw_data:
            # Assign the csv fields to variables
            total_data = total_data + 1
            active_customer = str(data['Active Customer?']).lower()
            sf_acount_type = data['Account Type']
            sf_parent_id = data['Salesforce Parent ID']
            officepublickey = data['officepublickey']
            office_addressline1 = str(data['office_addressline1']) + " " + str(data['office_addressline2'])
            office_city = data['office_city']
            office_state = data['office_state']
            office_zipcode = data['office_zip']
            office_legalname = data['office_legalname']
            office_name = data['office_name']
            office_phone = data['office_phone']
            office_webpage = data['office_website']
            parent_company_key = data['companypublickey']
            parent_company_legal_name = data['company_legalname']
            status = 'Insert'
            office_name_lower = str(office_name).lower()
            # Check if it is a active customer
            # if active_customer == 'f':
            #     continue
            # Check if office name contains 'test'
            if office_name_lower.find('test ') != -1 or office_name_lower.find(' test') != -1:
                garbage_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Office name was using the word test'}
                collect_garbage_array.append(garbage_array)
                garbage_count = garbage_count + 1
                continue
            # Check if office name contains 'demo'
            if office_name_lower.find('demo ') != -1 or office_name_lower.find(' demo') != -1:
                garbage_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Office name was using the word demo'}
                collect_garbage_array.append(garbage_array)
                garbage_count = garbage_count + 1
                continue
            # If Active customer column is found empty ten consider it as garbage
            # if not active_customer:
            #     garbage_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Active customer column was empty'}
            #     collect_garbage_array.append(garbage_array)
            #     garbage_count = garbage_count + 1
            #     continue
            #if office name not found then consider as garbage
            if not office_name:
                garbage_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Office name column was empty'}
                collect_garbage_array.append(garbage_array)
                garbage_count = garbage_count + 1
                continue
            # check if the given sf parent Id and parent companypublickey is valid
            if sf_parent_id and parent_company_key:
                cur.execute('Select count(*) from moxiworks.moxiworks_account where companypublickey = %s and sfdc_account_id = %s',(parent_company_key,sf_parent_id,))
                check_id_parent_exist = cur.fetchone()
                if check_id_parent_exist[0] == 0:
                    garbage_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Parent company\'s companypublickey & SFDC Id did not match with company table entries'}
                    collect_garbage_array.append(garbage_array)
                    garbage_count = garbage_count + 1
                    continue
            #if empty sf_parent_id or empty companypublickey column found in csv then consider as garbage
            if not sf_parent_id or not parent_company_key:
                garbage_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Parent company key or parent SFDC is missing in CSV'}
                collect_garbage_array.append(garbage_array)
                garbage_count = garbage_count + 1
                continue
            #check duplicate by officepublickey
            if officepublickey:
                cur.execute('Select count(*) from moxiworks.moxiworks_office where officepublickey = %s',(officepublickey,))
                check_duplicate = cur.fetchone()
            if check_duplicate[0] == 0:
                #if a new entry then insert
                try:
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_office (officepublickey, office_addressline1, office_city, office_state, office_zip, office_legalname, office_name, office_phone, office_website, companypublickey, company_legalname,sf_parent_id,sf_account_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (officepublickey, office_addressline1, office_city, office_state, office_zipcode, office_legalname, office_name, office_phone, office_webpage, parent_company_key, parent_company_legal_name,sf_parent_id,sf_acount_type))

                    cur.execute(
                        'INSERT INTO moxiworks.moxi_company_office_rel (parent_account, child_account) VALUES (%s, %s)',
                        (parent_company_key, officepublickey))

                    print('Data Inserted in moxiworks_office & moxi_company_office_rel....')

                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_office_delta (officepublickey, office_addressline1, office_city, office_state, office_zip, office_legalname, office_name, office_phone, office_website, companypublickey, company_legalname,sf_parent_id,sf_account_type, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (officepublickey, office_addressline1, office_city, office_state, office_zipcode, office_legalname, office_name, office_phone, office_webpage, parent_company_key, parent_company_legal_name,sf_parent_id,sf_acount_type, status))
                    print('Data Inserted for office in moxiworks_office_delta with Insert status....')
                    conn.commit()
                    total_inserted = total_inserted + 1
                    
                except Exception as e:
                    print("type error: " + str(e))
                    print("Failed at ID " + str(data['officepublickey']))
                    
                    conn.rollback()
                    continue
            else:
                #else add to the duplicate array
                duplicate_array = {'officepublickey':officepublickey,'companypublickey':parent_company_key,'sf_parent_id':sf_parent_id,'reason':'Same office public key was found in database'}
                duplicate_count = duplicate_count + 1
                collect_duplicate_array.append(duplicate_array)
        print ('Printing Duplicate entries and reason:')
        # Exporting a csv of duplicate entries
        if not os.path.exists('storage/reports/office'):
            os.makedirs('storage/reports/office')

        with open('storage/reports/office/office_duplicate_report.csv', mode='w') as csv_file:
            fieldnames = ['officepublickey','companypublickey', 'sf_parent_id','Reason']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for duplicate in collect_duplicate_array:
                print('Office Public Key: ' + duplicate['officepublickey'] + 'companypublickey: ' + duplicate['companypublickey'] + 'sf_parent_id: ' + duplicate['sf_parent_id'] + 'Reason: ' + duplicate['reason'])
                writer.writerow({'officepublickey':duplicate['officepublickey'],'companypublickey':duplicate['companypublickey'],'sf_parent_id':duplicate['sf_parent_id'],'Reason':duplicate['reason']})
        print ('Printing garbage entries and reason')
        # Exporting a csv of garbage entries
        with open('storage/reports/office/office_garbage_report.csv', mode='w') as csv_file:
            fieldnames = ['officepublickey','companypublickey', 'sf_parent_id','Reason']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for garbage in collect_garbage_array:
                print('Office Public Key: ' + garbage['officepublickey'] + 'companypublickey: ' + garbage['companypublickey'] + 'sf_parent_id: ' + garbage['sf_parent_id'] + 'Reason: ' + garbage['reason'])
                writer.writerow({'officepublickey':garbage['officepublickey'],'companypublickey':garbage['companypublickey'],'sf_parent_id':garbage['sf_parent_id'],'Reason':garbage['reason']})
        print ("Total Data Inserted: " + str(total_inserted))
        print ("Total Duplicate Data: " + str(duplicate_count))
        print ("Total Garbage Data: " + str(garbage_count))
        print ("Total Data Processed: " + str(total_data))
        # send_office_report('Office', total_data, total_inserted, total_updated)
import_init_data_obj = import_inital_data()
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv('db_user') + " password=" + os.getenv('db_password'))
import_init_data_obj.import_office_inital_data(conn)