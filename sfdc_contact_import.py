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
class import_sfdc_contact_data():
    def import_office_inital_data(self, conn):
        # Initialization 
        total_data = 0
        total_inserted = 0
        cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Agent/Moxi_Contact_{}'.format(datetime.date.today()) + '.csv'

        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        local_file_name = 'Moxi_Contact_{}'.format(datetime.date.today()) + '.csv'
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        for data in csv_raw_data:
            total_data = total_data + 1
            sfdc_contact_id = data['Id']
            sfdc_parent_account_id = data['AccountId']
            sfdc_title = data['Title']
            sfdc_email = data['Email']
            try:
                cur.execute(
                    'INSERT INTO moxiworks.sfdc_contact_data (sfdc_contact_id,sfdc_parent_account_id,sfdc_title,sfdc_email) VALUES (%s, %s, %s, %s)',
                    (sfdc_contact_id,sfdc_parent_account_id,sfdc_title,sfdc_email))
                print('Data Inserted in sfdc_contact_data....')
                conn.commit()
                total_inserted = total_inserted + 1
            except Exception as e:
                print("type error: " + str(e))
                conn.rollback()
                continue
        print("Total Inserted: " + str(total_inserted))
        print("Total Data: " + str(total_data))
#Created a object of the class
import_init_data_obj = import_sfdc_contact_data()
#create the connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
#call the import_company_inital_data function
import_init_data_obj.import_office_inital_data(conn)
