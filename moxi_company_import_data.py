import os
import csv
import datetime
import sys

import psycopg2
import psycopg2.extras
import os.path
from os import path
from src.aws.storage import S3
from dotenv import load_dotenv, find_dotenv
from common import to_bool, send_company_report
import glob
import uuid
import csvdiff


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
    def import_data_in_company(self, conn):
        #
        # total_data = 0
        # total_inserted = 0
        # total_updated = 0

        # Database connection object
        cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()

        # define Company csv file name for download form s3.
        # Here is the check of parse arguments via the parser variable for test purpose. This arguments
        # will be defined by the user on the console. that must be ' v2' along with the filename.
        if (len(sys.argv) > 1):
            if (str(sys.argv[1]).lower() == 'v2'):
                file_name = 'Company/Moxi_Companies_{}' . format(datetime.date.today()) + '-V2.csv'
            else:
                print('Invalid args! Did you mean v2')
                return 0
        else:
            file_name = 'Company/Moxi_Companies_{}'.format(datetime.date.today()) + '.csv'

        print(file_name)


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
                company_file_prefix = 'storage/Moxi_Companies_{}' . format(datetime.date.today()) + '-V2'
            else:
                print('Invalid args! Did you mean v2')
                return 0
        else:
            company_file_prefix = 'storage/Moxi_Companies_{}'.format(datetime.date.today())

        new_file_name = glob.glob(company_file_prefix + ".csv") # warp the actual file.

        # Retrieve the last processed file name from moxiworks_report table which has saved after every execution.
        cur.execute("SELECT * FROM moxiworks.moxiworks_report WHERE object_processed = 'company' ORDER BY created_at desc LIMIT 1")
        report_records = cur.fetchone()

        # for row in records:
        report_data = []
        if (report_records):
            report_data.append(dict(report_records))
            old_file_name = [report_data[0]['last_file_porcessed']]
        else:
            old_file_name = ['storage/Moxi_Companies_initial_file.csv']

        # Diff two CSV files, returning the patch which transforms one into the other.
        # Here companypublickey is the index key.
        patch = csvdiff.diff_files(''.join(old_file_name),
                                   ''.join(new_file_name), ['companypublickey'], ',')

        total_inserted = len(patch['added'])

        # For loop that interacts over newly added companies and insert it to
        # moxiworks_account also moxiworks_account_delta with insert status
        for key, data in enumerate(patch['added']):
            company_legalname = data['company_legalname']
            company_name = data['company_name']
            companypublickey = data['companypublickey']
            company_addressline1 = str(data['company_addressline1']) + " " + str(data['company_addressline2'])
            company_city = data['company_city']
            company_state = data['company_state']
            company_zipcode = data['company_zipcode']
            company_phone = data['company_phone']
            company_webpage = data['company_webpage']
            agent_count = data['agent_count']
            company_moxi_hub = to_bool(data['company_moxi_hub'])
            company_moxi_dms = to_bool(data['company_moxi_dms'])
            company_moxi_engage = to_bool(data['company_moxi_engage'])
            company_moxi_present = to_bool(data['company_moxi_present'])
            company_moxi_websites = to_bool(data['company_moxi_websites'])
            company_moxi_talent = to_bool(data['company_moxi_talent'])
            company_moxi_insights = to_bool(data['company_moxi_insights'])
            show_in_product_marketing = to_bool(data['show_in_product_marketing'])
            advertise_your_listing = to_bool(data['advertise_your_listing'])
            ayl_emails = to_bool(data['ayl_emails'])
            advertise_your_services = to_bool(data['advertise_your_services'])
            ays_emails = to_bool(data['ays_emails'])
            direct_marketing = to_bool(data['direct_marketing'])
            status = 'Insert'
            account_type = 'company'
            uniq_id = str(uuid.uuid4())
            try:
                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account (internal_key, company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, account_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (uniq_id, company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone,
                     company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage,
                     company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights,
                     show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services,
                     ays_emails, direct_marketing, account_type))
                print('Data Inserted in moxiworks_account....')
                cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account_delta (internal_key, company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub,company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, account_type, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                    (uniq_id, company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone,
                     company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage,
                     company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights,
                     show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services,
                     ays_emails, direct_marketing, account_type, status))
                print('Data Inserted in moxiworks_account_delta with Insert status....')
                conn.commit()
                success = 1
            except Exception as e:
                print("type error: " + str(e))
                print("Failed at ID " + str(data['companypublickey']))
                failed = 1
                conn.rollback()
                continue

        #     os.unlink(file)

        total_updated = len(patch['changed'])

        # For loop that interacts over updated companies and update those companies to moxiworks_account
        # and insert it to moxiworks_account_delta table with update status
        for key, data in enumerate(patch['changed']):
            columns = ''
            total_count = len(data['fields'])
            for inner_key, inner_data in enumerate(data['fields']):
                if(inner_data == 'company_moxi_hub' or inner_data == 'company_moxi_dms'
                        or inner_data == 'company_moxi_engage' or inner_data == 'company_moxi_present'
                        or inner_data == 'company_moxi_websites' or inner_data == 'company_moxi_talent'
                        or inner_data == 'company_moxi_insights' or inner_data == 'show_in_product_marketing'
                        or inner_data == 'advertise_your_listing' or inner_data == 'ayl_emails'
                        or inner_data == 'advertise_your_services' or inner_data == 'ays_emails'
                        or inner_data == 'direct_marketing') :
                
                    columns += inner_data + '=' + "'" + str(to_bool(data['fields'][inner_data]['to'])) + "'"
                    
                else:
                    columns += inner_data + '=' + "'" + data['fields'][inner_data]['to'] + "'"
                if (inner_key < total_count - 1):
                    columns += ', '

            query = "update moxiworks.moxiworks_account set {} where companypublickey='{}';".format(columns, data['key'][0])
            cur.execute(query)
            conn.commit()
            print('Id: ' + str(data['key'][0]) + ' has been updated')
            cur.execute("SELECT * from moxiworks.moxiworks_account where companypublickey = '{}'".format(data['key'][0]))

            records = cur.fetchone()
            data = []
            #for row in records:
            if (records):
                data.append(dict(records))
            else:
                continue

            uniq_id = str(uuid.uuid4())
            company_legalname = data[0]['company_legalname']
            company_name = data[0]['company_name']
            companypublickey = data[0]['companypublickey']
            company_addressline1 = str(data[0]['company_addressline1']) + " " + str(data[0]['company_addressline2'])
            company_city = data[0]['company_city']
            company_state = data[0]['company_state']
            company_zipcode = data[0]['company_zipcode']
            company_phone = data[0]['company_phone']
            company_webpage = data[0]['company_webpage']
            agent_count = data[0]['agent_count']
            company_moxi_hub = (data[0]['company_moxi_hub'])
            company_moxi_dms = (data[0]['company_moxi_dms'])
            company_moxi_engage = (data[0]['company_moxi_engage'])
            company_moxi_present = (data[0]['company_moxi_present'])
            company_moxi_websites = (data[0]['company_moxi_websites'])
            company_moxi_talent = (data[0]['company_moxi_talent'])
            company_moxi_insights = (data[0]['company_moxi_insights'])
            show_in_product_marketing = (data[0]['show_in_product_marketing'])
            advertise_your_listing = (data[0]['advertise_your_listing'])
            ayl_emails = (data[0]['ayl_emails'])
            advertise_your_services = (data[0]['advertise_your_services'])
            ays_emails = (data[0]['ays_emails'])
            direct_marketing = (data[0]['direct_marketing'])
            status = 'Update'
            account_type = 'company'
            try:
                cur.execute(
                        'INSERT INTO moxiworks.moxiworks_account_delta (internal_key, company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub,company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, account_type, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                        (uniq_id, company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone,
                        company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage,
                        company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights,
                        show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services,
                        ays_emails, direct_marketing, account_type, status))

                print('Data Inserted in moxiworks_account_delta with Insert status....')
                conn.commit()
            except Exception as e:
                print("type error: " + str(e))
                print("Failed at delta ID " + str(data[0]['companypublickey']))
                failed = 1
                conn.rollback()
                continue

        cur.execute("INSERT INTO moxiworks.moxiworks_report(object_processed, total_inserted, total_udpated, total_data_processed, last_file_porcessed, created_at) "
                                       "VALUES ('{}', {}, {}, {}, '{}', NOW())".format('company', total_inserted, total_updated, total_inserted+total_updated, ''.join(new_file_name)))
        conn.commit()

        total_data = total_inserted + total_updated
        print ("Total Inserted: " + str(total_inserted))
        print ("Total Updated: " + str(total_updated))
        print ("Total Data: " + str(total_data))

        send_company_report('Company', total_data, total_inserted, total_updated)



import_data_obj = import_data()

# Define database connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv('db_user') + " password=" + os.getenv('db_password'))

import_data_obj.import_data_in_company(conn)
