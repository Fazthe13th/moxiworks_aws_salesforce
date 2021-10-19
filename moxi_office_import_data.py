import os
import csv
import datetime
import sys

import psycopg2
import psycopg2.extras
from pathlib import Path
from src.aws.storage import S3
from dotenv import load_dotenv, find_dotenv
from common import to_bool, send_company_report
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
    def import_data_in_office(self, conn):

        # Database connection object
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()

        # define office csv file name which need to download form s3.
        # Here is the check of parse arguments via the parser variable for test purpose. This arguments
        # will be defined by the user on the console. that must be ' v2' along with the filename.
        if (len(sys.argv) > 1):
            if (str(sys.argv[1]).lower() == 'v2'):
                file_name = 'Office/Moxi_Offices_{}' . format(datetime.date.today()) + '-V2.csv'
            else:
                print('Invalid args! Did you mean v2')
                return 0
        else:
            file_name = 'Office/Moxi_Offices_{}'.format(datetime.date.today()) + '.csv'
        print(file_name)

        # Download above file from s3 through s3.download_file function.
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if(status == 404):
            print('No new files')
            return None

        # NOW TIME TO START PROCESSING...

        # Initialize previous file path which must be exists at storage directory to be compared.
        # Here is the check of parse arguments via the parser variable for test purpose. This arguments
        # will be defined by the user on the console. that must be ' v2' along with the filename.
        if (len(sys.argv) > 1):
            if (str(sys.argv[1]).lower() == 'v2'):
                office_file_prefix = 'storage/Moxi_Offices_{}' . format(datetime.date.today()) + '-V2'
            else:
                print('Invalid args! Did you mean v2')
                return 0
        else:
            office_file_prefix = 'storage/Moxi_Offices_{}'.format(datetime.date.today())

        new_file_name = glob.glob(office_file_prefix + ".csv")
        # yesterday = datetime.date.today() - datetime.timedelta(1)

        # Retrieve the last processed file name from moxiworks_report table which has saved after every execution.]
        cur.execute(
            "SELECT * FROM moxiworks.moxiworks_report WHERE object_processed = 'office' ORDER BY created_at desc LIMIT 1")

        report_records = cur.fetchone()
        report_data = []
        # for row in records:
        if (report_records):
            report_data.append(dict(report_records))
            old_file_name = [report_data[0]['last_file_porcessed']]
        else:
            old_file_name = ['storage/Moxi_Offices_initial_file.csv']

        # Diff two CSV files, returning the patch which transforms one into the other.
        # Here officepublickey from office csv file is the index key.
        patch = csvdiff.diff_files(''.join(old_file_name),
                                   ''.join(new_file_name), ['officepublickey'], ',')

        total_inserted = len(patch['added'])

        # For loop that interacts over newly added companies and insert it to
        # moxiworks_account also moxiworks_account_delta with insert status
        for key, data in enumerate(patch['added']):
            companypublickey = data['officepublickey']
            company_addressline1 = str(data['office_addressline1']) + " " + str(data['office_addressline2'])
            company_city = data['office_city']
            company_state = data['office_state']
            company_zipcode = data['office_zip']
            company_legalname = data['office_legalname']
            office_name = data['office_name']
            company_name = data['office_name']
            company_phone = data['office_phone']
            company_webpage = data['office_website']
            parent_company_key = data['companypublickey']
            parent_company_legal_name = data['company_legalname']
            uuid_internal_key = str(uuid.uuid4())
            status = 'Insert'
            account_type = 'office'

            try:
                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account (internal_key, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, account_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (uuid_internal_key, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, account_type))

                cur.execute(
                    'INSERT INTO moxiworks.moxi_company_office_rel (parent_account, child_account) VALUES (%s, %s)',
                    (parent_company_key, companypublickey))

                print('Data Inserted in moxiworks_account & moxi_company_office_rel....')

                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account_delta (internal_key, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, account_type, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (uuid_internal_key, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, account_type, status))
                print('Data Inserted for office in moxiworks_account_delta with Insert status....')
                conn.commit()
                success = 1
            except Exception as e:
                print("type error: " + str(e))
                print("Failed at ID " + str(data['officepublickey']))
                failed = 1
                conn.rollback()
                continue

        #     os.unlink(file)

        total_updated = len(patch['changed'])

        # For loop that interacts over changed information and update those offices to moxiworks_account
        # and insert it to moxiworks_account_delta table with update status
        for key, data in enumerate(patch['changed']):

            columns = ''
            total_count = len(data['fields'])

            for inner_key, inner_data in enumerate(data['fields']):

                if 'officepublickey' == inner_data:
                    # data['fields']['companypublickey'] = data['fields'].pop('officepublickey')
                    columns += 'companypublickey=' + "'" + data['fields']['officepublickey']['to'] + "'"

                if 'office_addressline1' == inner_data:
                    # data['fields']['company_addressline1'] = data['fields'].pop('office_addressline1')
                    columns += 'company_addressline1=' + "'" + data['fields']['office_addressline1']['to'] + "'"

                if 'office_city' == inner_data:
                    # data['fields']['company_city'] = data['fields'].pop('office_city')
                    columns += 'company_city=' + "'" + data['fields']['office_city']['to'] + "'"

                if 'office_state' == inner_data:
                    # data['fields']['company_state'] = data['fields'].pop('office_state')
                    columns += 'company_state=' + "'" + data['fields']['office_state']['to'] + "'"

                if 'office_zip' == inner_data:
                    # data['fields']['company_zipcode'] = data['fields'].pop('office_zip')
                    columns += 'company_zipcode=' + "'" + data['fields']['office_zip']['to'] + "'"

                if 'office_legalname' == inner_data:
                    # data['fields']['company_legalname'] = data['fields'].pop('office_legalname')
                    columns += 'company_legalname=' + "'" + data['fields']['office_legalname']['to'] + "'"

                if 'office_phone' == inner_data:
                    # data['fields']['company_phone'] = data['fields'].pop('office_phone')
                    columns += 'company_phone=' + "'" + data['fields']['office_phone']['to'] + "'"

                if 'office_website' == inner_data:
                    # data['fields']['company_webpage'] = data['fields'].pop('office_website')
                    columns += 'company_webpage=' + "'" + data['fields']['office_website']['to'] + "'"

                if 'companypublickey' == inner_data:
                    # data['fields']['parent_company_key'] = data['fields'].pop('companypublickey')
                    columns += 'parent_company_key=' + "'" + data['fields']['companypublickey']['to'] + "'"

                if 'company_legalname' == inner_data:
                    # data['fields']['parent_company_legal_name'] = data['fields'].pop('company_legalname')
                    columns += 'parent_company_legal_name=' + "'" + data['fields']['company_legalname']['to'] + "'"

                if 'office_name' == inner_data:
                    # data['fields']['parent_company_legal_name'] = data['fields'].pop('company_legalname')
                    columns += 'company_name=' + "'" + data['fields']['office_name']['to'] + "'"

                if (inner_key < total_count - 1):
                    columns += ', '
            # cur.execute(
            #     "UPDATE moxiworks.moxiworks_account SET company_addressline1=%s,company_city=%s,company_state=%s,company_zipcode=%s,company_legalname=%s,office_name=%s,company_name=%s,company_phone=%s,company_webpage=%s,parent_company_key=%s,parent_company_legal_name=%s WHERE companypublickey=%s",
            #     (company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, companypublickey))

            query = "update moxiworks.moxiworks_account set {} where companypublickey='{}';".format(columns,
                                                                                                      data['key'][0])
            cur.execute(query)
            try:
                data['fields']['parent_company_key']['to']
                parent_key = data['fields']['parent_company_key']['to']
                cur.execute(
                    "UPDATE moxiworks.moxi_company_office_rel SET parent_company_key=%s WHERE child_account=%s",
                    (parent_key, data['key'][0]))
            except Exception as e:
                pass

            conn.commit()
            print('Id: ' + data['key'][0] + ' has been updated to moxiworks_account table')

            cur.execute(
                "SELECT * from moxiworks.moxiworks_account where companypublickey = '{}'".format(data['key'][0]))

            records = cur.fetchone()
            data = []
            # for row in records:
            data.append(dict(records))

            companypublickey = data[0]['companypublickey']
            company_addressline1 = str(data[0]['company_addressline1']) + " " + str(data[0]['company_addressline2'])
            company_city = data[0]['company_city']
            company_state = data[0]['company_state']
            company_zipcode = data[0]['company_zipcode']
            company_legalname = data[0]['company_legalname']
            office_name = data[0]['company_name']
            company_name = data[0]['company_name']
            company_phone = data[0]['company_phone']
            company_webpage = data[0]['company_webpage']
            parent_company_key = data[0]['parent_company_key']
            parent_company_legal_name = data[0]['parent_company_legal_name']
            uuid_internal_key = str(uuid.uuid4())
            status = 'Update'
            account_type = 'office'

            try:
                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account_delta (internal_key, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, account_type, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                    (uuid_internal_key, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_legalname, office_name, company_name, company_phone, company_webpage, parent_company_key, parent_company_legal_name, account_type, status))

                print('Data Inserted in moxiworks_account_delta with Insert status....')
                conn.commit()
            except Exception as e:
                print("Update type error: " + str(e))
                print("Failed at delta ID " + str(companypublickey))
                failed = 1
                conn.rollback()
                continue

        cur.execute(
            "INSERT INTO moxiworks.moxiworks_report(object_processed, total_inserted, total_udpated, total_data_processed, last_file_porcessed, created_at) "
            "VALUES ('{}', {}, {}, {}, '{}', NOW())".format('office', total_inserted, total_updated,
                                                            total_inserted + total_updated, ''.join(new_file_name)))
        conn.commit()

        total_data = total_inserted + total_updated
        print("Total Inserted: " + str(total_inserted))
        print("Total Updated: " + str(total_updated))
        print("Total Data: " + str(total_data))

        send_company_report('Office', total_data, total_inserted, total_updated)


import_data_obj = import_data()
# Define database connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
import_data_obj.import_data_in_office(conn)
