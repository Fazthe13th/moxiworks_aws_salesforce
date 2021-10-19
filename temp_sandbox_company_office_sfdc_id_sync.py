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
import glob
import uuid
import csvdiff
import http.client

load_dotenv(find_dotenv())


def read_csv(file_name):
    result = []
    # Files are in storage folder . we read the csv files from there
    with open('storage/' + file_name, 'r') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            result.append(dict(row))
    fp.close()

    return result
def sync_sfdc_id(conn):
    # Initialization
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    conn.commit()
    s3 = S3()
    # file_name = 'Company/Moxi_Offices_{}'.format(datetime.date.today()) + '.csv'
    file_name = 'Company/sfdc_account_sandbox.csv'
    # check if file exists. If exixts then download the file
    status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
    if (status == 404):
        print('No new files')
        return None

    local_file_name = 'sfdc_account_sandbox.csv'
    # Read the csv file
    csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
    for csv_data in csv_raw_data:
        sfdc_id = str(csv_data['ID']).strip()
        sf_parent_id = str(csv_data['PARENTID']).strip()
        companypublickey = str(csv_data['MOXI_ACCOUNT_UUID__C']).strip()
        officepublickey = str(csv_data['OFFICE_UUID__C']).strip()
        if companypublickey and not officepublickey:
            cur.execute(
                "UPDATE moxiworks.moxiworks_account SET sfdc_account_id = %s, sf_parent_id = %s where companypublickey = %s",
                (sfdc_id,sf_parent_id,companypublickey))
            conn.commit()
            print('Updated Company')
        if officepublickey:
            cur.execute(
                "UPDATE moxiworks.moxiworks_office SET sfdc_office_id = %s, sf_parent_id = %s where officepublickey = %s",
                (sfdc_id,sf_parent_id,officepublickey))
            conn.commit()
            print('Updated Office')

conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv('db_user') + " password=" + os.getenv('db_password'))
sync_sfdc_id(conn)