import os
import csv
import datetime
import psycopg2
import psycopg2.extras
from pathlib import Path
from src.aws.storage import S3
from dotenv import load_dotenv, find_dotenv
from common import to_bool, send_agent_report
import glob



load_dotenv(find_dotenv())

def read_csv(file_name):
    result = []
    with open('storage/' + file_name, 'r') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            result.append(dict(row))
    fp.close()

    return result

class import_data():
    def import_data_in_agent(self, conn):
        cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

        s3 = S3()
        agent_prefix = 'Agent/Moxi-Agent-{}'.format(datetime.date.today())
        list_of_files = s3.list_files(bucket=os.getenv('s3_bucket'), prefix=agent_prefix)
        if list_of_files is None:
            print ('No new files')
            return None
        else:
            for file_name in list_of_files:
                s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        agent_file_prefix = 'storage/Moxi-Agent-{}'.format(datetime.date.today())
        filenames = sorted(glob.glob(agent_file_prefix + "*.csv"))
        total_data = 0
        total_inserted = 0
        total_updated = 0
        for file in filenames:
            print (file)        
            csv_raw_data = read_csv(str(Path(os.path.basename(file))))
            success = 0
            failed = 0
            cur.execute('SELECT count(*) from moxiworks.moxiworksagents')
            check_if_empty = cur.fetchone()
            for csv_data in csv_raw_data:
                total_data += 1
                first_name = csv_data['first_name']
                last_name = csv_data['last_name']
                company_legal_name = csv_data['company_legal_name']
                company_uuid = csv_data['company_uuid']
                access_lvl = csv_data['access_lvl']
                primary_email = csv_data['primary_email']
                direct_phone = csv_data['direct_phone']
                mobile_phone = csv_data['mobile_phone']
                other_phone = csv_data['other_phone']
                title = csv_data['title']
                title_2 = csv_data['title_2']
                title_3 = csv_data['title_3']
                active = to_bool(csv_data['active'])
                at_least_1_mls_association = to_bool(csv_data['at_least_1_mls_association'])
                hub = to_bool(csv_data['hub'])
                dms = to_bool(csv_data['dms'])
                moxi_engage = to_bool(csv_data['moxi_engage'])
                moxi_present = to_bool(csv_data['moxi_present'])
                moxi_websites = to_bool(csv_data['moxi_websites'])
                moxi_talent = to_bool(csv_data['moxi_talent'])
                csv_data_array = [first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,other_phone,title,title_2,title_3,active,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_talent,moxi_websites]
                if check_if_empty[0] == 0:
                    status = 'Insert'
                    try:
                        cur.execute(
                            'INSERT INTO moxiworks.moxiworksagents (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,other_phone,title,title_2,title_3,at_least_1_mls_association,hub,dms,moxiengage,moxipresent,moxiwebsites,moxitalent,active) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)',
                            (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                            primary_email, direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent,active))
                        print('Data Inserted in Moxiworksagents....')
                        cur.execute(
                            'INSERT INTO moxiworks.moxiworksagents_delta (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,other_phone,title,title_2,title_3,at_least_1_mls_association,hub,dms,moxiengage,moxipresent,moxiwebsites,moxitalent,active,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)',
                            (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                            primary_email, direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent,active, status))
                        print('Data Inserted in Moxiworksagents_delta with Insert status....')
                        conn.commit()
                        success = 1
                        total_inserted += 1
                    except Exception as e:
                        print("type error: " + str(e))
                        print("Failed at ID " + str(csv_data['uuid']))
                        failed = 1
                        break
                else:
                    cur.execute('SELECT * from moxiworks.moxiworksagents where uuid = %s', (csv_data['uuid'],))
                    get_row_data = cur.fetchone()
                    if get_row_data:
                        # Update Agent
                        database_array = [get_row_data['first_name'],get_row_data['last_name'],get_row_data['company_legal_name'],get_row_data['company_uuid'],get_row_data['access_lvl'],get_row_data['primary_email'],get_row_data['direct_phone'],get_row_data['mobile_phone'],get_row_data['other_phone'],get_row_data['title'],get_row_data['title_2'],get_row_data['title_3'],get_row_data['active'],get_row_data['at_least_1_mls_association'],get_row_data['hub'],get_row_data['dms'],get_row_data['moxiengage'],get_row_data['moxipresent'],get_row_data['moxitalent'],get_row_data['moxiwebsites']]
                        if csv_data_array != database_array:
                            status = 'Update'
                            cur.execute(
                                "UPDATE moxiworks.moxiworksagents SET first_name=%s,last_name=%s,company_legal_name=%s,company_uuid=%s,access_lvl=%s,primary_email=%s,direct_phone=%s,mobile_phone=%s,other_phone=%s,title=%s,title_2=%s,title_3=%s,at_least_1_mls_association=%s,hub=%s,dms=%s,moxiengage=%s,moxipresent=%s,moxiwebsites=%s,moxitalent=%s,active=%s WHERE uuid=%s",
                                (first_name, last_name, company_legal_name, company_uuid, access_lvl, primary_email,
                                direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent, active, str(csv_data['uuid'])))
                            print('Id: ' + str(csv_data['uuid']) + ' has been updated')
                            total_updated += 1
                            cur.execute('SELECT count(uuid) from moxiworks.moxiworksagents_delta where uuid = %s',
                                        (csv_data['uuid'],))
                            
                            check_delta_id_exist = cur.fetchone()
                            if check_delta_id_exist[0] == 0:
                                cur.execute(
                                    'INSERT INTO moxiworks.moxiworksagents_delta (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,other_phone,title,title_2,title_3,at_least_1_mls_association,hub,dms,moxiengage,moxipresent,moxiwebsites,moxitalent,active,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)',
                                    (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                                    primary_email, direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent,active, status))
                                print('Id: ' + str(csv_data['uuid']) + ' has been been inserted in delta table with Update status')
                            else:
                                cur.execute(
                                    "UPDATE moxiworks.moxiworksagents_delta SET first_name=%s,last_name=%s,company_legal_name=%s,company_uuid=%s,access_lvl=%s,primary_email=%s,direct_phone=%s,mobile_phone=%s,other_phone=%s,title=%s,title_2=%s,title_3=%s,at_least_1_mls_association=%s,hub=%s,dms=%s,moxiengage=%s,moxipresent=%s,moxiwebsites=%s,moxitalent=%s,active=%s,status=%s WHERE uuid=%s",
                                    (first_name, last_name, company_legal_name, company_uuid, access_lvl, primary_email,
                                    direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent, active,status, str(csv_data['uuid'])))
                                print('Id: ' + str(csv_data['uuid']) + ' has been been inserted in delta table with Update status')
                            conn.commit()
                    else:
                        status = 'Insert'
                        try:
                            cur.execute(
                                'INSERT INTO moxiworks.moxiworksagents (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,other_phone,title,title_2,title_3,at_least_1_mls_association,hub,dms,moxiengage,moxipresent,moxiwebsites,moxitalent,active) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)',
                                (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                                primary_email, direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent,active))
                            print('Data Inserted in Moxiworksagents....')
                            cur.execute(
                                'INSERT INTO moxiworks.moxiworksagents_delta (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,other_phone,title,title_2,title_3,at_least_1_mls_association,hub,dms,moxiengage,moxipresent,moxiwebsites,moxitalent,active,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)',
                                (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                                primary_email, direct_phone, mobile_phone,other_phone, title,title_2,title_3,at_least_1_mls_association,hub,dms,moxi_engage,moxi_present,moxi_websites,moxi_talent,active, status))
                            print('Data Inserted in Moxiworksagents_delta with Insert Status....')
                            conn.commit()
                            success = 1
                            total_inserted += 1
                        except Exception as e:
                            print("type error: " + str(e))

                            print("Failed at ID " + str(csv_data['uuid']))
                            failed = 1
                            break
                if failed == 1:
                    print('Insertion Failed')
                    break
            if success == 1:
                print('Data inserted successfully')
            os.unlink(file)
            # csv_file.close()
        print ("Total Inserted: " + str(total_inserted))
        print ("Total Updated: " + str(total_updated))
        print ("Total Data: " + str(total_data))
        send_agent_report(total_data, total_inserted, total_updated)

import_data_obj = import_data()
conn = psycopg2.connect(
    "host=moxiworks-demo.crro5wjm3xfn.us-west-1.rds.amazonaws.com dbname=moxiworks_dbsync user=moxiworks_demo password=BB1RbiqKBzP46quW")
import_data_obj.import_data_in_agent(conn)