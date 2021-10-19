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


class import_office_daily_data():
    def import_in_main_and_delta_table(self, conn, temp_table_inserted, temp_table_duplicate, temp_table_garbage, temp_table_data_processed):
        total_data = 0
        total_inserted = 0
        total_updated = 0
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        cur.execute("select * from moxiworks.moxiworks_office where officepublickey not in ( select officepublickey from moxiworks.moxiworks_office_ongoing)")
        inactive_array = cur.fetchall()
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/office_inactive_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_office_inactive_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['office_legalname', 'office_name', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'region_name', 'office_website', 'companypublickey', 'company_legalname']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for inactive in inactive_array:
                writer.writerow({'office_legalname':inactive['office_legalname'], 'office_name':inactive['office_name'], 'officepublickey':inactive['officepublickey'], 'office_addressline1':inactive['office_addressline1'], 'office_city':inactive['office_city'], 'office_state':inactive['office_state'], 'office_zip':inactive['office_zip'], 'office_phone':inactive['office_phone'], 'region_name':inactive['region_name'], 'office_website':inactive['office_website'], 'companypublickey':inactive['companypublickey'], 'company_legalname':inactive['company_legalname']})
                try:
                    cur.execute(
                        "UPDATE moxiworks.moxiworks_office SET isactivated = %s where officepublickey = %s",
                        (to_bool('f'), inactive['officepublickey']))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    continue
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/office_log_report'
        full_path = directory + store
        logf = open(full_path + "/office_error_{}".format(datetime.date.today()) + ".log", "w")
        cur.execute("select a.* from (SELECT office_legalname, office_name, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, region_name, office_website, companypublickey, company_legalname,sf_parent_id FROM moxiworks.moxiworks_office_ongoing except SELECT office_legalname, office_name, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, region_name, office_website, companypublickey, company_legalname,sf_parent_id FROM moxiworks.moxiworks_office) a, moxiworks.moxiworks_office b where a.officepublickey=b.officepublickey")
        update_array = cur.fetchall()
        cur.execute(
            "select * from moxiworks.moxiworks_office_ongoing where officepublickey not in (select officepublickey from moxiworks.moxiworks_office)")
        insert_array = cur.fetchall()
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/office_update_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_office_update_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['office_legalname', 'office_name', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'region_name', 'office_website', 'companypublickey', 'company_legalname']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for update in update_array:
                writer.writerow({'office_legalname':update['office_legalname'], 'office_name':update['office_name'], 'officepublickey':update['officepublickey'], 'office_addressline1':update['office_addressline1'], 'office_city':update['office_city'], 'office_state':update['office_state'], 'office_zip':update['office_zip'], 'office_phone':update['office_phone'], 'region_name':update['region_name'], 'office_website':update['office_website'], 'companypublickey':update['companypublickey'], 'company_legalname':update['company_legalname']})
                total_data = total_data + 1
                status = 'update'
                try:
                    cur.execute(
                        "UPDATE moxiworks.moxiworks_office SET office_legalname = %s, office_addressline1 = %s, office_city = %s, office_state = %s, office_zip = %s, office_website = %s, office_phone = %s, office_name = %s, region_name = %s, companypublickey = %s,sf_parent_id=%s, company_legalname = %s where officepublickey = %s",
                        (update['office_legalname'], update['office_addressline1'], update['office_city'],
                        update['office_state'], update['office_zip'], update['office_website'],
                        update['office_phone'],update['office_name'], update['region_name'],
                        update['companypublickey'],update['sf_parent_id'], update['company_legalname'], update['officepublickey']))
                    print('Data updated in moxiworks_office....')
                    cur.execute('Select * from moxiworks.moxiworks_office where officepublickey = %s',(update['officepublickey'],))
                    office_information = cur.fetchone()
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_office_delta (office_legalname, office_addressline1, office_city, office_state, office_zip, office_website, office_phone, office_name, officepublickey, region_name, companypublickey, company_legalname,sfdc_office_id,sf_parent_id,isactivated, status) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                        (update['office_legalname'], update['office_addressline1'], update['office_city'],
                        update['office_state'], update['office_zip'], update['office_website'],
                        update['office_phone'], update['office_name'], update['officepublickey'],
                        update['region_name'], update['companypublickey'],
                        update['company_legalname'],office_information['sfdc_office_id'],update['sf_parent_id'],office_information['isactivated'], status))
                    print('Data inserted in moxiworks_office_delta....')
                    conn.commit()
                    total_updated = total_updated + 1
                except Exception as e:
                    logf.write("Failed to update officepublickey {0}: {1}\n".format(str(update['officepublickey']), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(update['officepublickey']))
                    conn.rollback()
                    continue
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/office_insert_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_office_insert_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['office_legalname', 'office_name', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'region_name', 'office_website', 'companypublickey', 'company_legalname']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for insert in insert_array:
                writer.writerow({'office_legalname':insert['office_legalname'], 'office_name':insert['office_name'], 'officepublickey':insert['officepublickey'], 'office_addressline1':insert['office_addressline1'], 'office_city':insert['office_city'], 'office_state':insert['office_state'], 'office_zip':insert['office_zip'], 'office_phone':insert['office_phone'], 'region_name':insert['region_name'], 'office_website':insert['office_website'], 'companypublickey':insert['companypublickey'], 'company_legalname':insert['company_legalname']})
                total_data = total_data + 1
                status = 'insert'
                try:
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_office (office_legalname, office_addressline1, office_city, office_state, office_zip, office_website, office_phone, office_name, officepublickey, region_name, companypublickey, company_legalname,sf_parent_id) VALUES (%s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                        (insert['office_legalname'], insert['office_addressline1'], insert['office_city'],
                        insert['office_state'], insert['office_zip'], insert['office_website'],
                        insert['office_phone'], insert['office_name'], insert['officepublickey'],
                        insert['region_name'], insert['companypublickey'],
                        insert['company_legalname'],insert['sf_parent_id']))
                    print('Data inserted in moxiworks_office....')
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_office_delta (office_legalname, office_addressline1, office_city, office_state, office_zip, office_website, office_phone, office_name, officepublickey, region_name, companypublickey, company_legalname,sf_parent_id, status) VALUES (%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                        (insert['office_legalname'], insert['office_addressline1'], insert['office_city'],
                        insert['office_state'], insert['office_zip'], insert['office_website'],
                        insert['office_phone'], insert['office_name'], insert['officepublickey'],
                        insert['region_name'], insert['companypublickey'],
                        insert['company_legalname'],insert['sf_parent_id'], status))
                    print('Data inserted in moxiworks_office_delta....')
                    conn.commit()
                    total_inserted = total_inserted + 1
                except Exception as e:
                    logf.write("Failed to insert officepublickey {0}: {1}\n".format(str(insert['officepublickey']), str(e)))
                    print("type error: " + str(e))
                    print("Failed at ID " + str(insert['officepublickey']))
                    conn.rollback()
                    continue
        print("Temp Table Total Data Inserted: " + str(temp_table_inserted))
        print("Temp Table Total Error Data: " + str(temp_table_garbage))
        print("Temp Table Total Data Processed: " + str(temp_table_data_processed))
        print("Total Data Inserted: " + str(total_inserted))
        print("Total Data Updated: " + str(total_updated))
        print("Total Data Processed: " + str(total_data))
        send_office_report('Office',total_data, total_inserted, total_updated,temp_table_data_processed,temp_table_garbage,temp_table_inserted)



    def import_office_data(self, conn):
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
        file_name = 'Office/Moxi-Offices_{}'.format(datetime.date.today()) + '.csv'
        # file_name = 'Office/Moxi-Offices_2019-02-06.csv'
        # check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return 'no_new_file'

        local_file_name = 'Moxi-Offices_{}'.format(datetime.date.today()) + '.csv'
        # local_file_name = 'Moxi-Offices_2019-02-06.csv'
        # logf = open("office_error_{}".format(datetime.date.today()) + ".log", "w")
        # Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        filename = 'storage/' + local_file_name
        directory = str(os.path.dirname(os.path.realpath(__file__)))
        store = '/storage/office_error_report'
        full_path = directory + store
        with open(full_path + "/moxiworks_office_error_report_{}".format(datetime.date.today()) + ".csv", mode='w') as csv_file:
            fieldnames = ['office_legalname', 'office_name', 'officepublickey', 'office_addressline1', 'office_city', 'office_state', 'office_zip', 'office_phone', 'region_name', 'office_website', 'companypublickey', 'company_legalname', 'reason']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for csv_data in csv_raw_data:
                reason_array = []
                total_data = total_data + 1
                print('Data processing ' + str(total_data))

                office_legalname = str(csv_data['office_legalname']).strip()
                if len(office_legalname) > 40:
                    office_legalname = str(office_legalname[:40]).strip()

                office_name = str(csv_data['office_name']).strip()
                if len(office_name) > 40:
                    office_name = str(office_name[:40]).strip()

                officepublickey = str(csv_data['officepublickey']).strip()
                office_addressline1 = str(csv_data['office_addressline1']).strip() + " " + str(csv_data['office_addressline2']).strip()
                office_city = str(csv_data['office_city']).strip()
                office_state = str(csv_data['office_state']).strip()
                office_zip = str(csv_data['office_zip']).strip()
                office_phone = str(csv_data['office_phone']).strip()
                office_website = str(csv_data['office_website']).strip()
                region_name = str(csv_data['region_name']).strip()
                companypublickey = str(csv_data['companypublickey']).strip()
                company_legalname = str(csv_data['company_legalname']).strip()

                office_name_lower = str(office_name).lower()

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
                
                if not officepublickey:
                    reason = "Office public key not exist"
                    reason_array.append(reason)
                    
                    
                else:
                    cur.execute('Select count(*) from moxiworks.moxiworks_office_ongoing where officepublickey = %s',
                                (officepublickey,))
                    check_duplicate = cur.fetchone()
                    if check_duplicate[0] > 0:
                        reason = "Duplicate office of: " + officepublickey
                        reason_array.append(reason)
                        duplicate_count = duplicate_count + 1
                        

                # Check if office name contains 'test'
                if office_name_lower.find('test ') != -1 or office_name_lower.find(' test') != -1:
                    reason = "Office name was using the word test"
                    reason_array.append(reason)
                    
                    

                # Check if office name contains 'demo'
                if office_name_lower.find('demo ') != -1 or office_name_lower.find(' demo') != -1:
                    reason = "Office name was using the word demo"
                    reason_array.append(reason)
                    
                    

                if not office_name:
                    reason = "Office name column was empty"
                    reason_array.append(reason)
                    
                    

                # check if the given parent companypublickey is valid
                if companypublickey:
                    cur.execute('Select count(*) from moxiworks.moxiworks_account where companypublickey = %s',(companypublickey,))
                    check_id_parent_exist = cur.fetchone()
                    if check_id_parent_exist[0] == 0:
                        reason = "Parent company\'s companypublickey did not match with company table entries"
                        reason_array.append(reason)
                    else:
                        cur.execute('Select * from moxiworks.moxiworks_account where companypublickey = %s',(companypublickey,))
                        company_information = cur.fetchone()
                        sf_parent_id = company_information['sfdc_account_id']
                        
                if not companypublickey:
                    reason = "Company public key not exist"
                    reason_array.append(reason)    



                if not reason_array:
                    try:
                        cur.execute(
                            'INSERT INTO moxiworks.moxiworks_office_ongoing (office_legalname, office_name, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, region_name, office_website, companypublickey, company_legalname,sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)',
                            (office_legalname, office_name, officepublickey, office_addressline1,
                            office_city, office_state, office_zip, office_phone, region_name,
                            office_website, companypublickey, company_legalname,sf_parent_id))
                        print('Data Inserted in moxiworks_office_ongoing....')
                        conn.commit()
                        total_inserted = total_inserted + 1
                    except Exception as e:
                        print("type error: " + str(e))
                        print("Failed at ID " + str(csv_data['officepublickey']))
                        # logf.write("Failed to insert uuid {0}: {1}\n".format(str(officepublickey), str(e)))
                        conn.rollback()
                        continue
                else:
                    seperator = ', '
                    reasons = seperator.join(reason_array)
                    error_array.append(str(officepublickey))
                    writer.writerow({'office_legalname':office_legalname, 'office_name':office_name, 'officepublickey':officepublickey, 'office_addressline1':office_addressline1, 'office_city':office_city, 'office_state':office_state, 'office_zip':office_zip, 'office_phone':office_phone, 'region_name':region_name, 'office_website':office_website, 'companypublickey':companypublickey, 'company_legalname':company_legalname, 'reason':reasons})
                    cur.execute('Select count(*) from moxiworks.moxiworks_office_ongoing_report where officepublickey = %s',
                                    (officepublickey,))
                    check_duplicate = cur.fetchone()
                    if check_duplicate[0] > 0:
                        try:
                            cur.execute("UPDATE moxiworks.moxiworks_office_ongoing_report SET reason = %s where officepublickey = %s",
                            (reasons, officepublickey))
                            conn.commit()
                        except Exception as e:
                            print("Failed at Office public key " + str(officepublickey))
                            conn.rollback()
                            continue
                    else:
                        try:
                            cur.execute(
                                'INSERT INTO moxiworks.moxiworks_office_ongoing_report (office_legalname, office_name, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, region_name, office_website, companypublickey, company_legalname, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (office_legalname, office_name, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, region_name, office_website, companypublickey, company_legalname, reasons))
                            print('Data Inserted for moxiworks_office_ongoing_report....')
                            conn.commit()
                            
                        except Exception as e:
                            # logf.write("Failed to insert uuid {0}: {1}\n".format(str(officepublickey), str(e)))
                            print("type error: " + str(e))
                            print("Failed at office public key " + str(officepublickey))
                            conn.rollback()
                            continue
                    garbage_count = garbage_count + 1

        self.import_in_main_and_delta_table(conn, total_inserted, duplicate_count, garbage_count, total_data)
        os.unlink(filename)
        cur.execute("TRUNCATE TABLE moxiworks.moxiworks_office_ongoing")
        conn.commit()


# # Created a object of the class
# office_daily_obj = import_office_daily_data()
# # create the connection
# conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
#     'db_user') + " password=" + os.getenv('db_password'))
# # call the import_company_inital_data function
# # import_company_daily_data.import_company_data(conn)
# office_daily_obj.import_office_data(conn)


