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


class import_company_daily_data():
    def import_in_main_and_delta_table(self, conn,temp_table_inserted,temp_table_duplicate,temp_table_garbage,temp_table_data_processed):
        total_data = 0
        total_inserted = 0
        total_updated = 0
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/company_log_report'
        full_path = directory + store
        logf = open(full_path + "/company_error_{}".format(datetime.date.today()) + ".log", "w")
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/company_inactive_report'
        full_path = directory + store
        cur.execute("select * from moxiworks.moxiworks_account where companypublickey not in (select companypublickey from moxiworks.moxiworks_account_ongoing)")
        inactive_array = cur.fetchall()
        with open(full_path + "/moxiworks_company_inactive_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['company_legalname', 'company_name', 'companypublickey', 'company_addressline1', 'company_city', 'company_state', 'company_zipcode', 'company_phone', 'company_webpage', 'agent_count', 'company_moxi_hub', 'company_moxi_dms', 'company_moxi_engage', 'company_moxi_present', 'company_moxi_websites', 'company_moxi_talent', 'company_moxi_insights', 'show_in_product_marketing', 'advertise_your_listing', 'ayl_emails', 'advertise_your_services', 'ays_emails', 'direct_marketing']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for inactive in inactive_array:
                writer.writerow({'company_legalname':inactive['company_legalname'], 'company_name':inactive['company_name'], 'companypublickey':inactive['companypublickey'], 'company_addressline1':inactive['company_addressline1'], 'company_city':inactive['company_city'], 'company_state':inactive['company_state'], 'company_zipcode':inactive['company_zipcode'], 'company_phone':inactive['company_phone'], 'company_webpage':inactive['company_webpage'], 'agent_count':inactive['agent_count'], 'company_moxi_hub':inactive['company_moxi_hub'], 'company_moxi_dms':inactive['company_moxi_dms'], 'company_moxi_engage':inactive['company_moxi_engage'], 'company_moxi_present':inactive['company_moxi_present'], 'company_moxi_websites':inactive['company_moxi_websites'], 'company_moxi_talent':inactive['company_moxi_talent'], 'company_moxi_insights':inactive['company_moxi_insights'], 'show_in_product_marketing':inactive['show_in_product_marketing'], 'advertise_your_listing':inactive['advertise_your_listing'], 'ayl_emails':inactive['ayl_emails'], 'advertise_your_services':inactive['advertise_your_services'], 'ays_emails':inactive['ays_emails'], 'direct_marketing':inactive['direct_marketing']})
                try:
                    cur.execute(
                    "UPDATE moxiworks.moxiworks_account SET isactivated = %s where companypublickey = %s",
                    (to_bool('f'),inactive['companypublickey']))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    continue
        cur.execute("select a.* from (select company_legalname, company_name, companypublickey, company_addressline1, company_addressline2, company_city, company_state, company_zipcode, company_webpage, agent_count, company_phone, company_moxi_hub, company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing FROM moxiworks.moxiworks_account_ongoing except select company_legalname, company_name, companypublickey, company_addressline1, company_addressline2, company_city, company_state, company_zipcode, company_webpage, agent_count, company_phone, company_moxi_hub, company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing FROM moxiworks.moxiworks_account) a, moxiworks.moxiworks_account b where a.companypublickey=b.companypublickey")
        update_array = cur.fetchall()
        cur.execute("select * from moxiworks.moxiworks_account_ongoing where companypublickey not in (select companypublickey from moxiworks.moxiworks_account)")
        insert_array = cur.fetchall()
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/company_update_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_company_update_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['company_legalname', 'company_name', 'companypublickey', 'company_addressline1', 'company_city', 'company_state', 'company_zipcode', 'company_phone', 'company_webpage', 'agent_count', 'company_moxi_hub', 'company_moxi_dms', 'company_moxi_engage', 'company_moxi_present', 'company_moxi_websites', 'company_moxi_talent', 'company_moxi_insights', 'show_in_product_marketing', 'advertise_your_listing', 'ayl_emails', 'advertise_your_services', 'ays_emails', 'direct_marketing']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for update in update_array:
                writer.writerow({'company_legalname':update['company_legalname'], 'company_name':update['company_name'], 'companypublickey':update['companypublickey'], 'company_addressline1':update['company_addressline1'], 'company_city':update['company_city'], 'company_state':update['company_state'], 'company_zipcode':update['company_zipcode'], 'company_phone':update['company_phone'], 'company_webpage':update['company_webpage'], 'agent_count':update['agent_count'], 'company_moxi_hub':update['company_moxi_hub'], 'company_moxi_dms':update['company_moxi_dms'], 'company_moxi_engage':update['company_moxi_engage'], 'company_moxi_present':update['company_moxi_present'], 'company_moxi_websites':update['company_moxi_websites'], 'company_moxi_talent':update['company_moxi_talent'], 'company_moxi_insights':update['company_moxi_insights'], 'show_in_product_marketing':update['show_in_product_marketing'], 'advertise_your_listing':update['advertise_your_listing'], 'ayl_emails':update['ayl_emails'], 'advertise_your_services':update['advertise_your_services'], 'ays_emails':update['ays_emails'], 'direct_marketing':update['direct_marketing']})
                total_data = total_data + 1
                status = 'update'
                try:
                    cur.execute(
                    "UPDATE moxiworks.moxiworks_account SET company_legalname = %s, company_addressline1 = %s, company_city = %s, company_state = %s, company_zipcode = %s, company_webpage = %s, agent_count = %s, company_phone = %s, company_moxi_hub = %s, company_moxi_dms = %s, company_moxi_engage = %s, company_moxi_present = %s, company_moxi_websites = %s, company_moxi_talent = %s, company_moxi_insights = %s, show_in_product_marketing = %s, advertise_your_listing = %s, ayl_emails = %s, advertise_your_services = %s, ays_emails = %s, direct_marketing = %s, company_name = %s where companypublickey = %s",
                    (update['company_legalname'], update['company_addressline1'], update['company_city'], update['company_state'], update['company_zipcode'], update['company_webpage'],  update['agent_count'], update['company_phone'], update['company_moxi_hub'], update['company_moxi_dms'], update['company_moxi_engage'], update['company_moxi_present'], update['company_moxi_websites'], update['company_moxi_talent'], update['company_moxi_insights'], update['show_in_product_marketing'], update['advertise_your_listing'], update['ayl_emails'], update['advertise_your_services'], update['ays_emails'], update['direct_marketing'], update['company_name'],update['companypublickey']))
                    print('Data updated in moxi_account....')
                    cur.execute('Select * from moxiworks.moxiworks_account where companypublickey = %s',(update['companypublickey'],))
                    company_information = cur.fetchone()
                    cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account_delta (company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub,company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing,sfdc_account_id,isactivated, status) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)',
                    (update['company_legalname'], update['company_name'], update['companypublickey'], update['company_addressline1'], update['company_city'], update['company_state'], update['company_zipcode'], update['company_phone'], update['company_webpage'], update['agent_count'], update['company_moxi_hub'],update['company_moxi_dms'], update['company_moxi_engage'], update['company_moxi_present'], update['company_moxi_websites'], update['company_moxi_talent'], update['company_moxi_insights'], update['show_in_product_marketing'], update['advertise_your_listing'], update['ayl_emails'], update['advertise_your_services'], update['ays_emails'], update['direct_marketing'],company_information['sfdc_account_id'],company_information['isactivated'],status))
                    print('Data inserted in moxi_account_delta....')
                    conn.commit()
                    total_updated = total_updated + 1
                except Exception as e:
                    logf.write("Failed to update companypublickey {0}: {1}\n".format(str(update['companypublickey']), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(update['companypublickey']))
                    conn.rollback()
                    continue
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/company_insert_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_company_insert_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['company_legalname', 'company_name', 'companypublickey', 'company_addressline1', 'company_city', 'company_state', 'company_zipcode', 'company_phone', 'company_webpage', 'agent_count', 'company_moxi_hub', 'company_moxi_dms', 'company_moxi_engage', 'company_moxi_present', 'company_moxi_websites', 'company_moxi_talent', 'company_moxi_insights', 'show_in_product_marketing', 'advertise_your_listing', 'ayl_emails', 'advertise_your_services', 'ays_emails', 'direct_marketing']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for insert in insert_array:
                writer.writerow({'company_legalname':insert['company_legalname'], 'company_name':insert['company_name'], 'companypublickey':insert['companypublickey'], 'company_addressline1':insert['company_addressline1'], 'company_city':insert['company_city'], 'company_state':insert['company_state'], 'company_zipcode':insert['company_zipcode'], 'company_phone':insert['company_phone'], 'company_webpage':insert['company_webpage'], 'agent_count':insert['agent_count'], 'company_moxi_hub':insert['company_moxi_hub'], 'company_moxi_dms':insert['company_moxi_dms'], 'company_moxi_engage':insert['company_moxi_engage'], 'company_moxi_present':insert['company_moxi_present'], 'company_moxi_websites':insert['company_moxi_websites'], 'company_moxi_talent':insert['company_moxi_talent'], 'company_moxi_insights':insert['company_moxi_insights'], 'show_in_product_marketing':insert['show_in_product_marketing'], 'advertise_your_listing':insert['advertise_your_listing'], 'ayl_emails':insert['ayl_emails'], 'advertise_your_services':insert['advertise_your_services'], 'ays_emails':insert['ays_emails'], 'direct_marketing':insert['direct_marketing']})
                total_data = total_data + 1
                status = 'insert'
                uniq_id = str(uuid.uuid4())
                try:
                    cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account (internal_key,company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub,company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)',
                    (uniq_id, insert['company_legalname'], insert['company_name'], insert['companypublickey'], insert['company_addressline1'], insert['company_city'], insert['company_state'], insert['company_zipcode'], insert['company_phone'], insert['company_webpage'], insert['agent_count'], insert['company_moxi_hub'],insert['company_moxi_dms'], insert['company_moxi_engage'], insert['company_moxi_present'], insert['company_moxi_websites'], insert['company_moxi_talent'], insert['company_moxi_insights'], insert['show_in_product_marketing'], insert['advertise_your_listing'], insert['ayl_emails'], insert['advertise_your_services'], insert['ays_emails'], insert['direct_marketing']))
                    print('Data inserted in moxi_account_delta....')
                    cur.execute(
                    'INSERT INTO moxiworks.moxiworks_account_delta (company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub,company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)',
                    (insert['company_legalname'], insert['company_name'], insert['companypublickey'], insert['company_addressline1'], insert['company_city'], insert['company_state'], insert['company_zipcode'], insert['company_phone'], insert['company_webpage'], insert['agent_count'], insert['company_moxi_hub'],insert['company_moxi_dms'], insert['company_moxi_engage'], insert['company_moxi_present'], insert['company_moxi_websites'], insert['company_moxi_talent'], insert['company_moxi_insights'], insert['show_in_product_marketing'], insert['advertise_your_listing'], insert['ayl_emails'], insert['advertise_your_services'], insert['ays_emails'], insert['direct_marketing'],status))
                    print('Data inserted in moxi_account_delta....')
                    conn.commit()
                    total_inserted = total_inserted + 1
                except Exception as e:
                    logf.write("Failed to insert companypublickey {0}: {1}\n".format(str(insert['companypublickey']), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(insert['companypublickey']))
                    conn.rollback()
                    continue
        print("Temp Table Total Data Inserted: " + str(temp_table_inserted))
        print("Temp Table Total Error Data: " + str(temp_table_garbage))
        print("Temp Table Total Data Processed: " + str(temp_table_data_processed))
        print("Total Data Inserted: " + str(total_inserted))
        print("Total Data Updated: " + str(total_updated))
        print("Total Data Processed: " + str(total_data))
        send_company_report('Company', total_data, total_inserted, total_updated,temp_table_data_processed,temp_table_garbage,temp_table_inserted)

    def import_company_data(self, conn):
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
        file_name = 'Company/Moxi-Companies_{}'.format(datetime.date.today()) + '.csv'
        # file_name = 'Company/Moxi-Companies_2019-02-06.csv'
        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return 'no_new_file'

        local_file_name = 'Moxi-Companies_{}'.format(datetime.date.today()) + '.csv'
        # local_file_name = 'Moxi-Companies_2019-02-06.csv'
        # logf = open("company_error_{}".format(datetime.date.today()) + ".log", "w")
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        filename = 'storage/' + local_file_name
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/company_error_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_company_error_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['company_legalname', 'company_name', 'companypublickey', 'company_addressline1', 'company_city', 'company_state', 'company_zipcode', 'company_phone', 'company_webpage', 'agent_count', 'company_moxi_hub', 'company_moxi_dms', 'company_moxi_engage', 'company_moxi_present', 'company_moxi_websites', 'company_moxi_talent', 'company_moxi_insights', 'show_in_product_marketing', 'advertise_your_listing', 'ayl_emails', 'advertise_your_services', 'ays_emails', 'direct_marketing', 'reason']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for csv_data in csv_raw_data:
                reason_array = []
                total_data = total_data + 1
                print('Data processing ' + str(total_data))
                company_legalname = str(csv_data['company_legalname']).strip()
                if len(company_legalname) > 40:
                    company_legalname = company_legalname[:40]
                company_name = str(csv_data['company_name']).strip()
                if len(company_name) > 40:
                    company_name = company_name[:40]
                companypublickey = str(csv_data['companypublickey']).strip()
                company_addressline1 = str(csv_data['company_addressline1']).strip() + " " + str(csv_data['company_addressline2']).strip()
                company_city = str(csv_data['company_city']).strip()
                company_state = str(csv_data['company_state']).strip()
                company_zipcode = str(csv_data['company_zipcode']).strip()
                company_phone = str(csv_data['company_phone']).strip()
                company_webpage = str(csv_data['company_webpage']).strip()
                agent_count = str(csv_data['agent_count']).strip()
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
                company_name_lower = str(company_name).lower()
                company_legalname_lower = str(company_legalname).lower()
                companypublickey_validation_array = ['2835448', '2842549', '1553323', '1261393', '1663783', '1664572', '2485132', '1242457', '3205489', '2843338', '2579812', '2980624', '1624333', '3139213', '3016129', '3016918', '1240879', '2958532', '3238627', '1509928', '2934073', '2582968', '3100552']
                comapny_name_validation_array = ['DEMO ONLY','Moxiworks','TEST Company','Test Subscription','TouchCMA','Demo','Automation Test','Smoakhouse Properties','Regression','ECOMMADMIN','Branding','Branding Demo','Google Auth Test','Fake','Training']
                
                for value in companypublickey_validation_array:
                    if str(companypublickey) == value:
                        reason = "Companypublickey contains " + value
                        reason_array.append(reason)
                for value in comapny_name_validation_array:
                    if company_name_lower.find(value) != -1:
                        reason = "Company name contains " + value
                        reason_array.append(reason)
                for value in comapny_name_validation_array:
                    if company_legalname_lower.find(value) != -1:
                        reason = "Company legal name contains " + value
                        reason_array.append(reason)
                if int(agent_count) < 5:
                    reason = "Agent count is less then 5"
                    reason_array.append(reason)
                
                if not companypublickey:
                    reason = "Company public key not exist"
                    reason_array.append(reason)
                    
                else:
                    cur.execute('Select count(*) from moxiworks.moxiworks_account_ongoing where companypublickey = %s',
                                    (companypublickey,))
                    check_duplicate = cur.fetchone()
                    if check_duplicate[0] > 0:
                        reason = "Duplicate company of: " + str(companypublickey)
                        reason_array.append(reason)
                        duplicate_count = duplicate_count + 1
                        
                if not reason_array:
                    try:
                        cur.execute(
                            'INSERT INTO moxiworks.moxiworks_account_ongoing (company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                            (company_legalname, company_name, companypublickey,
                                company_addressline1,
                                company_city, company_state, company_zipcode, company_phone,
                                company_webpage, agent_count, company_moxi_hub, company_moxi_dms,
                                company_moxi_engage,
                                company_moxi_present, company_moxi_websites, company_moxi_talent,
                                company_moxi_insights,
                                show_in_product_marketing, advertise_your_listing, ayl_emails,
                                advertise_your_services,
                                ays_emails, direct_marketing))
                        
                        conn.commit()
                        print('Data Inserted in moxiworks_account_ongoing....')
                        total_inserted = total_inserted + 1
                    except Exception as e:
                        print("type error: " + str(e))
                        print("Failed at ID " + str(csv_data['companypublickey']))
                        # logf.write("Failed to insert uuid {0}: {1}\n".format(str(companypublickey), str(e)))
                        conn.rollback()
                        continue
                else:
                    seperator = ', '
                    reasons = seperator.join(reason_array)
                    error_array.append(str(companypublickey))
                    writer.writerow({'company_legalname':company_legalname, 'company_name':company_name, 'companypublickey':companypublickey, 'company_addressline1':company_addressline1, 'company_city':company_city, 'company_state':company_state, 'company_zipcode':company_zipcode, 'company_phone':company_phone, 'company_webpage':company_webpage, 'agent_count':agent_count, 'company_moxi_hub':company_moxi_hub, 'company_moxi_dms':company_moxi_dms, 'company_moxi_engage':company_moxi_engage, 'company_moxi_present':company_moxi_present, 'company_moxi_websites':company_moxi_websites, 'company_moxi_talent':company_moxi_talent, 'company_moxi_insights':company_moxi_insights, 'show_in_product_marketing':show_in_product_marketing, 'advertise_your_listing':advertise_your_listing, 'ayl_emails':ayl_emails, 'advertise_your_services':advertise_your_services, 'ays_emails':ays_emails, 'direct_marketing':direct_marketing, 'reason':reasons})
                    cur.execute('Select count(*) from moxiworks.moxiworks_account_ongoing_report where companypublickey = %s',
                                    (companypublickey,))
                    check_duplicate = cur.fetchone()
                    if check_duplicate[0] > 0:
                        try:
                            cur.execute("UPDATE moxiworks.moxiworks_account_ongoing_report SET reason = %s where companypublickey = %s",
                            (reasons, companypublickey))
                            conn.commit()
                        except Exception as e:
                            print("Failed at Company public key " + str(companypublickey))
                            conn.rollback()
                            continue
                    else:
                        try:
                            cur.execute(
                                'INSERT INTO moxiworks.moxiworks_account_ongoing_report (company_legalname, company_name, companypublickey, company_addressline1, company_city, company_state, company_zipcode, company_phone, company_webpage, agent_count, company_moxi_hub, company_moxi_dms, company_moxi_engage, company_moxi_present, company_moxi_websites, company_moxi_talent, company_moxi_insights, show_in_product_marketing, advertise_your_listing, ayl_emails, advertise_your_services, ays_emails, direct_marketing, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (company_legalname, company_name, companypublickey,
                                    company_addressline1,
                                    company_city, company_state, company_zipcode, company_phone,
                                    company_webpage, agent_count, company_moxi_hub, company_moxi_dms,
                                    company_moxi_engage,
                                    company_moxi_present, company_moxi_websites, company_moxi_talent,
                                    company_moxi_insights,
                                    show_in_product_marketing, advertise_your_listing, ayl_emails,
                                    advertise_your_services,
                                    ays_emails, direct_marketing, reasons))
                            
                            conn.commit()
                            print('Data Inserted for moxiworks_contact_report....')
                            
                        except Exception as e:
                            # logf.write("Failed to insert uuid {0}: {1}\n".format(str(companypublickey), str(e)))
                            print("type error: " + str(e))
                            print("Failed at Company public key " + str(companypublickey))
                            conn.rollback()
                            continue
                    garbage_count = garbage_count + 1
        
        self.import_in_main_and_delta_table(conn,total_inserted,duplicate_count,garbage_count,total_data)
        os.unlink(filename)
        cur.execute("TRUNCATE TABLE moxiworks.moxiworks_account_ongoing")
        conn.commit()
        
      
# #Created a object of the class
# import_company_daily_data = import_company_daily_data()
# #create the connection
# conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
#     'db_user') + " password=" + os.getenv('db_password'))
# #call the import_company_inital_data function
# # import_company_daily_data.import_company_data(conn)
# import_company_daily_data.import_company_data(conn)
            

