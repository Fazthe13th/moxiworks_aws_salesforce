import os
import csv
import datetime
import psycopg2
from pathlib import Path
from src.aws.storage import S3
from dotenv import load_dotenv, find_dotenv
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

def to_bool(value):
    valid = {'true': True, 't': True, '1': True,
             'false': False, 'f': False, '0': False,
             }   

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)


class import_data():
    def import_data_in_company(self, conn):
        cur = conn.cursor()

        # path = input("Enter the true path to COMPANY csv file: ")
        # csv_file = open(path, 'r+', newline='')
        # csv_raw_data = csv.reader(csv_file)
        # next(csv_raw_data)

        s3 = S3()
        company_prefix = 'Company/Moxi-Company-{}'.format(datetime.date.today())
        list_of_files = s3.list_files(bucket=os.getenv('s3_bucket'), prefix=company_prefix)
        if list_of_files is None:
            print ('No new files')
            return None
        else:
            for file_name in list_of_files:
                s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        company_file_prefix = 'storage/Moxi-Company-{}'.format(datetime.date.today())
        filenames = glob.glob(company_file_prefix + "*.csv")
        for file in filenames:           
            csv_raw_data = read_csv(str(Path(os.path.basename(file))))
            success = 0
            failed = 0
            cur.execute('SELECT count(*) from moxiworkscompany')
            check_if_empty = cur.fetchone()
            for csv_data in csv_raw_data:
                company_legal_name = csv_data['company_legal_name']
                address_line_1 = csv_data['address_line_1']
                address_line_2 = csv_data['address_line_1']
                city = csv_data['city']
                state = csv_data['state']
                zip_code = csv_data['zip_code']
                phone = csv_data['phone']
                website = csv_data['website']
                hub = to_bool(csv_data['hub'])
                dms = to_bool(csv_data['dms'])
                engage = to_bool(csv_data['engage'])
                present = to_bool(csv_data['present'])
                moxiwebsites = to_bool(csv_data['moxiwebsites'])
                moxitalents = to_bool(csv_data['moxitalents'])
                moxiinsights = to_bool(csv_data['moxiinsights'])
                show_in_product_marketing = to_bool(csv_data['show_in_product_marketing'])
                ayl = to_bool(csv_data['ayl'])
                ayl_emails = to_bool(csv_data['ayl_emails'])
                ays = to_bool(csv_data['ays'])
                ays_emails = to_bool(csv_data['ays_emails'])
                
                if check_if_empty[0] == 0:
                    status = 'Insert'
                    try:
                        cur.execute(
                            'INSERT INTO public.moxiworkscompany (uuid,company_legal_name,address_line_1,address_line_2,city,state,zip_code,phone,website,hub,dms,engage,present,moxiwebsites,moxitalents,moxiinsights,show_in_product_marketing,ayl,ayl_emails,ays,ays_emails) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)',
                            (str(csv_data['uuid']), company_legal_name, address_line_1, address_line_2, city, state,
                            zip_code,
                            phone, website, hub, dms, engage, present, moxiwebsites, moxitalents, moxiinsights,
                            show_in_product_marketing, ayl, ayl_emails, ays, ays_emails))
                        print('Data Inserted in MoxiworksCompany....')
                        cur.execute(
                            'INSERT INTO public.moxiworkscompany_delta (uuid,company_legal_name,address_line_1,address_line_2,city,state,zip_code,phone,website,hub,dms,engage,present,moxiwebsites,moxitalents,moxiinsights,show_in_product_marketing,ayl,ayl_emails,ays,ays_emails,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s)',
                            (str(csv_data['uuid']), company_legal_name, address_line_1, address_line_2, city, state,
                            zip_code,
                            phone, website, hub, dms, engage, present, moxiwebsites, moxitalents, moxiinsights,
                            show_in_product_marketing, ayl, ayl_emails, ays, ays_emails, status))
                        print('Data Inserted in MoxiworksCompany_delta....')
                        conn.commit()
                        success = 1
                    except Exception as e:
                        print("type error: " + str(e))
                        print("Failed at ID " + str(csv_data['uuid']))
                        failed = 1
                        break
                else:
                    cur.execute('SELECT count(uuid) from public.moxiworkscompany where uuid = %s', (csv_data['uuid'],))
                    check_id_exist = cur.fetchone()
                    if check_id_exist[0] != 0:
                        status = 'Update'
                        cur.execute(
                            "UPDATE public.moxiworkscompany SET company_legal_name=%s,address_line_1=%s,address_line_2=%s,city=%s,state=%s,zip_code=%s,phone=%s,website=%s,hub=%s,dms=%s,engage=%s,present=%s,moxiwebsites=%s,moxitalents=%s,moxiinsights=%s,show_in_product_marketing=%s,ayl=%s,ayl_emails=%s,ays=%s,ays_emails=%s  WHERE uuid=%s",
                            (company_legal_name, address_line_1, address_line_2, city, state, zip_code, phone, website, hub,
                            dms, engage, present, moxiwebsites, moxitalents, moxiinsights, show_in_product_marketing, ayl,
                            ayl_emails, ays, ays_emails, str(csv_data['uuid'])))
                        print('Id: ' + str(csv_data['uuid']) + ' has been updated')
                        cur.execute('SELECT count(uuid) from public.moxiworkscompany_delta where uuid = %s',
                                    (csv_data['uuid'],))
                        check_delta_id_exist = cur.fetchone()
                        if check_delta_id_exist[0] == 0:
                            cur.execute(
                                'INSERT INTO public.moxiworkscompany_delta (uuid,company_legal_name,address_line_1,address_line_2,city,state,zip_code,phone,website,hub,dms,engage,present,moxiwebsites,moxitalents,moxiinsights,show_in_product_marketing,ayl,ayl_emails,ays,ays_emails,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s)',
                                (
                                    str(csv_data['uuid']), company_legal_name, address_line_1, address_line_2, city, state,
                                    zip_code,
                                    phone, website, hub, dms, engage, present, moxiwebsites, moxitalents, moxiinsights,
                                    show_in_product_marketing, ayl, ayl_emails, ays, ays_emails, status))
                            print('Id: ' + str(
                                csv_data['uuid']) + ' has been been inserted in delta table with insert status')
                        else:
                            cur.execute(
                                "UPDATE public.moxiworkscompany_delta SET company_legal_name=%s,address_line_1=%s,address_line_2=%s,city=%s,state=%s,zip_code=%s,phone=%s,website=%s,hub=%s,dms=%s,engage=%s,present=%s,moxiwebsites=%s,moxitalents=%s,moxiinsights=%s,show_in_product_marketing=%s,ayl=%s,ayl_emails=%s,ays=%s,ays_emails=%s,status=%s  WHERE uuid=%s",
                                (company_legal_name, address_line_1, address_line_2, city, state, zip_code, phone, website,
                                hub, dms, engage, present, moxiwebsites, moxitalents, moxiinsights,
                                show_in_product_marketing, ayl, ayl_emails, ays, ays_emails, status,
                                str(csv_data['uuid'])))
                            print('Id: ' + str(
                                csv_data['uuid']) + ' has been been inserted in delta table with update status')
                        conn.commit()
                    else:
                        status = 'Insert'
                        try:
                            cur.execute(
                                'INSERT INTO public.moxiworkscompany (uuid,company_legal_name,address_line_1,address_line_2,city,state,zip_code,phone,website,hub,dms,engage,present,moxiwebsites,moxitalents,moxiinsights,show_in_product_marketing,ayl,ayl_emails,ays,ays_emails) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)',
                                (
                                    str(csv_data['uuid']), company_legal_name, address_line_1, address_line_2, city, state,
                                    zip_code,
                                    phone, website, hub, dms, engage, present, moxiwebsites, moxitalents, moxiinsights,
                                    show_in_product_marketing, ayl, ayl_emails, ays, ays_emails))
                            print('Data Inserted in MoxiworksCompany....')
                            cur.execute(
                                'INSERT INTO public.moxiworkscompany_delta (uuid,company_legal_name,address_line_1,address_line_2,city,state,zip_code,phone,website,hub,dms,engage,present,moxiwebsites,moxitalents,moxiinsights,show_in_product_marketing,ayl,ayl_emails,ays,ays_emails,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,%s)',
                                (
                                    str(csv_data['uuid']), company_legal_name, address_line_1, address_line_2, city, state,
                                    zip_code,
                                    phone, website, hub, dms, engage, present, moxiwebsites, moxitalents, moxiinsights,
                                    show_in_product_marketing, ayl, ayl_emails, ays, ays_emails, status))
                            print('Data Inserted in MoxiworksCompany_delta....')
                            conn.commit()
                            success = 1
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
            # csv_file.close()

    def import_data_in_agent(self, conn):
        cur = conn.cursor()
        # path = input("Enter the true path to AGENTS csv file: ")
        # csv_file = open(path, 'r+', newline='')
        # csv_raw_data = csv.reader(csv_file)
        # next(csv_raw_data)

        s3 = S3()
        agent_file_name = 'Agent/Moxi-Agent-{}.csv'.format(datetime.date.today())
        s3.download_file(bucket=os.getenv('s3_bucket'), file=agent_file_name)
        csv_raw_data = read_csv(str(Path(os.path.basename(agent_file_name))))

        success = 0
        failed = 0
        cur.execute('SELECT count(*) from moxiworksagents')
        check_if_empty = cur.fetchone()
        for csv_data in csv_raw_data:
            first_name = csv_data['first_name']
            last_name = csv_data['last_name']
            company_legal_name = csv_data['company_legal_name']
            company_uuid = csv_data['company_uuid']
            access_lvl = csv_data['access_lvl']
            primary_email = csv_data['primary_email']
            direct_phone = csv_data['direct_phone']
            mobile_phone = csv_data['mobile_phone']
            title = csv_data['title']
            active = to_bool(csv_data['active'])
            if check_if_empty[0] == 0:
                status = 'Insert'
                try:
                    cur.execute(
                        'INSERT INTO public.moxiworksagents (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,title,active) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)',
                        (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                         primary_email, direct_phone, mobile_phone, title, active))
                    print('Data Inserted in Moxiworksagents....')
                    cur.execute(
                        'INSERT INTO public.moxiworksagents_delta (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,title,active,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                        (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                         primary_email, direct_phone, mobile_phone, title, active, status))
                    print('Data Inserted in Moxiworksagents_delta....')
                    conn.commit()
                    success = 1
                except Exception as e:
                    print("type error: " + str(e))
                    print("Failed at ID " + str(csv_data['uuid']))
                    failed = 1
                    break
            else:
                cur.execute('SELECT count(uuid) from public.moxiworksagents where uuid = %s', (csv_data['uuid'],))
                check_id_exist = cur.fetchone()
                if check_id_exist[0] != 0:
                    # Update Agent
                    status = 'Update'
                    cur.execute(
                        "UPDATE public.moxiworksagents SET first_name=%s,last_name=%s,company_legal_name=%s,company_uuid=%s,access_lvl=%s,primary_email=%s,direct_phone=%s,mobile_phone=%s,title=%s,active=%s WHERE uuid=%s",
                        (first_name, last_name, company_legal_name, company_uuid, access_lvl, primary_email,
                         direct_phone, mobile_phone, title, active, str(csv_data['uuid'])))
                    print('Id: ' + str(csv_data['uuid']) + ' has been updated')
                    cur.execute('SELECT count(uuid) from public.moxiworksagents_delta where uuid = %s',
                                (csv_data['uuid'],))
                    check_delta_id_exist = cur.fetchone()
                    if check_delta_id_exist[0] == 0:
                        cur.execute(
                            'INSERT INTO public.moxiworksagents_delta (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,title,active,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                            (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                             primary_email, direct_phone, mobile_phone, title, active, status))
                        print('Id: ' + str(csv_data['uuid']) + ' has been been inserted in delta table with insert status')
                    else:
                        cur.execute(
                            "UPDATE public.moxiworksagents SET first_name=%s,last_name=%s,company_legal_name=%s,company_uuid=%s,access_lvl=%s,primary_email=%s,direct_phone=%s,mobile_phone=%s,title=%s,active=%s,status=%s WHERE uuid=%s",
                            (first_name, last_name, company_legal_name, company_uuid, access_lvl, primary_email,
                             direct_phone, mobile_phone, title, active, status, str(csv_data['uuid'])))
                        print('Id: ' + str(csv_data['uuid']) + ' has been been inserted in delta table with update status')
                    conn.commit()
                else:
                    status = 'Insert'
                    try:
                        cur.execute(
                            'INSERT INTO public.moxiworksagents (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,title,active) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)',
                            (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                             primary_email, direct_phone, mobile_phone, title, active))
                        print('Data Inserted in Moxiworksagents....')
                        cur.execute(
                            'INSERT INTO public.moxiworksagents_delta (uuid,first_name,last_name,company_legal_name,company_uuid,access_lvl,primary_email,direct_phone,mobile_phone,title,active,status) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)',
                            (str(csv_data['uuid']), first_name, last_name, company_legal_name, company_uuid, access_lvl,
                             primary_email, direct_phone, mobile_phone, title, active, status))
                        print('Data Inserted in Moxiworksagents_delta....')
                        conn.commit()
                        success = 1
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
        # csv_file.close()


import_data_obj = import_data()
# conn = psycopg2.connect("host=localhost dbname=csv user=postgres password=secret")
conn = psycopg2.connect(
    "host=moxiworks-demo.crro5wjm3xfn.us-west-1.rds.amazonaws.com dbname=postgres user=moxiworks_demo password=BB1RbiqKBzP46quW")
# import_data_obj.import_data_in_company(conn)
# import_data_obj.import_data_in_agent(conn)
which_table = input('Data insert for Company(1) or Agent(2) : ')
val = int(which_table)
if which_table is not None:
    if val == 1:
        import_data_obj.import_data_in_company(conn)
    elif val == 2:
        import_data_obj.import_data_in_agent(conn)
    else:
        print('Invalid choise!! Plase enter 1 or 2 as input')



# import boto3
# bucket = os.getenv('s3_bucket')
# #Make sure you provide / in the end
# prefix = 'Company/Moxi-Company-{}'.format(datetime.date.today())

# client = boto3.client('s3', aws_access_key_id=os.getenv('aws_access_key_id'), 
#             aws_secret_access_key=os.getenv('aws_secret_access_key'), 
#             region_name=os.getenv('region_name'))
# result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
# for o in result.get('Contents'):
#     print ('sub folder : ', o['Key'])