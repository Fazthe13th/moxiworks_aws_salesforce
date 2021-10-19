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
from common import to_bool, send_agent_report
import glob
import uuid
import csvdiff
from validate_email import validate_email

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
    def import_agent_inital_data(self, conn):
        # Initialization 
        total_data = 0
        total_inserted = 0
        total_updated = 0
        garbage_count = 0
        duplicate_count = 0
        collect_garbage_array = []
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conn.commit()
        s3 = S3()
        file_name = 'Agent/Moxi_Agents_initial_{}'.format(datetime.date.today()) + '.csv'
        #check if file exists. If exixts then download the file
        status = s3.download_file(bucket=os.getenv('s3_bucket'), file=file_name)
        if (status == 404):
            print('No new files')
            return None

        local_file_name = 'Moxi_Agents_initial_{}'.format(datetime.date.today()) + '.csv'
        #Read the csv file
        csv_raw_data = read_csv(str(Path(os.path.basename(local_file_name))))
        for data in csv_raw_data:
            total_data = total_data + 1
            print('Currently processing data no: ' + str(total_data))
            agent_uuid = data['agent_uuid']
            userid = data['userid']
            agent_username = data['agent_username']
            firstname = data['firstname']
            if len(firstname) > 40:
                firstname = firstname[:40]
            lastname = data['lastname']
            if len(lastname) > 40:
                lastname = lastname[:40]
            nickname = data['nickname']
            officename_display = data['officename_display']
            officepublickey = data['officepublickey']
            office_addressline1 = str(data['office_addressline1']) + " " + str(data['office_addressline2'])
            office_city = data['office_city']
            office_state = data['office_state']
            office_zip = data['office_zip']
            office_phone = data['office_phone']
            office_extension = data['office_extension']
            company_legalname = data['company_legalname']
            companypublickey = data['companypublickey']
            lastmodified = data['lastmodified']
            primary_email = data['primary_email']
            secondary_email = data['secondary_email']
            alternate_email = data['alternate_email']
            direct_phone = data['direct_phone']
            mobile_phone = data['mobile_phone']
            title_display = data['title_display']
            accred_display = data['accred_display']
            currentrolename = data['currentrolename']
            user_category = data['user_category']
            title_cleaned = data['title_cleaned']
            team_name = data['team_name']
            team_member_type = data['team_member_type']
            date_deactivated = data['date_deactivated']
            agent_active = to_bool(data['agent_active'])
            at_least_one_mls_association = to_bool(data['at_least_one_mls_association'])
            agent_moxi_hub = to_bool(data['agent_moxi_hub'])
            agent_moxi_dms = to_bool(data['agent_moxi_dms'])
            agent_moxi_engage = to_bool(data['agent_moxi_engage'])
            agent_moxi_present = to_bool(data['agent_moxi_present'])
            agent_moxi_websites = to_bool(data['agent_moxi_websites'])
            agent_moxi_talent = to_bool(data['agent_moxi_talent'])
            firstname_lower = str(firstname).lower()
            lastname_lower = str(lastname).lower()
            primary_email_lower = str(primary_email).lower()
            alt_email_lower = str(alternate_email).lower()
            #Updated check logic

            # First name logic
            if firstname_lower.find('test ') != -1 or firstname_lower.find(' test') != -1:
                garbage_count = garbage_count + 1
                print('Garbage count ' + str(garbage_count))
                continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains word test'})
            if firstname_lower.find('demo ') != -1 or firstname_lower.find(' demo') != -1:
                garbage_count = garbage_count + 1
                print('Garbage count ' + str(garbage_count))
                continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains word demo'})
            # if firstname_lower.find('qa ') != -1 or firstname_lower.find(' qa') != -1:
            #     garbage_count = garbage_count + 1
            #     print('Garbage count ' + str(garbage_count))
            #     continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains QA'})
            if firstname_lower.find('noreply ') != -1 or firstname_lower.find(' noreply') != -1:
                garbage_count = garbage_count + 1
                print('Garbage count ' + str(garbage_count))
                continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains noreply'})
            # Last name logic
            if lastname_lower.find('test ') != -1 or lastname_lower.find(' test') != -1:
                garbage_count = garbage_count + 1
                print('Garbage count ' + str(garbage_count))
                continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains word test'})
            if lastname_lower.find('demo ') != -1 or lastname_lower.find(' demo') != -1:
                garbage_count = garbage_count + 1
                print('Garbage count ' + str(garbage_count))
                continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains word demo'})
            # if lastname_lower.find('qa ') != -1 or lastname_lower.find(' qa') != -1:
            #     garbage_count = garbage_count + 1
            #     print('Garbage count ' + str(garbage_count))
            #     continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains QA'})
            if lastname_lower.find('noreply ') != -1 or lastname_lower.find(' noreply') != -1:
                garbage_count = garbage_count + 1
                print('Garbage count ' + str(garbage_count))
                continue
                # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'First name contains word noreply'})
            # Primary email logic
            if primary_email:
                is_valid_primary_email = validate_email(primary_email)
                if is_valid_primary_email == 1:
                    if primary_email_lower.find('test') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word test'})
                    if primary_email_lower.find('demo') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word demo'})
                    # if primary_email_lower.find('qa') != -1:
                    #     garbage_count = garbage_count + 1
                    #     print('Garbage count ' + str(garbage_count))
                    #     continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word QA'})
                    if primary_email_lower.find('noreply') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word noreply'})
                    if primary_email_lower.find('@moxiworks.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @moxiworks.com'})
                    if primary_email_lower.find('@touchcma.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @touchcma.com'})
                    if primary_email_lower.find('@sweeper.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @sweeper.com'})
                    if primary_email_lower.find('@sweepre.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @sweepre.com'})
                    if primary_email_lower.find('@sweeperre.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @sweeperre.com'})
                    if primary_email_lower.find('qa+') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word qa+'})
                    if primary_email_lower.find('info') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word info'})
                    if primary_email_lower.find('@windermeresolutions.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @windermeresolutions.com'})
                    if primary_email_lower.find('qa-test') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word qa-test'})
                    if primary_email_lower.find('@mw.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word @mw.com'})
                    if primary_email_lower.find('moxi') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word moxi'})
                    if primary_email_lower.find('autoboot') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word autoboot'})
                    if primary_email_lower.find('ecomm') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word ecomm'})
                    if primary_email_lower.find('borg000') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word borg000'})
                    if primary_email_lower.find('admin') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email contains word admin'})
                else:
                    garbage_count = garbage_count + 1
                    print('Garbage count ' + str(garbage_count))
                    continue
                    # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Primary email is not valid'})
            # Alt email Logic
            if alternate_email:
                is_valid_alt_email = validate_email(alternate_email)
                if is_valid_alt_email == 1:
                    if alt_email_lower.find('test') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word test'})
                    if alt_email_lower.find('demo') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word demo'})
                    # if alt_email_lower.find('qa') != -1:
                    #     garbage_count = garbage_count + 1
                    #     print('Garbage count ' + str(garbage_count))
                    #     continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word QA'})
                    if alt_email_lower.find('noreply') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word noreply'})
                    if alt_email_lower.find('@moxiworks.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @moxiworks.com'})
                    if alt_email_lower.find('@touchcma.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @touchcma.com'})
                    if alt_email_lower.find('@sweeper.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @sweeper.com'})
                    if alt_email_lower.find('@sweepre.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @sweepre.com'})
                    if alt_email_lower.find('@sweeperre.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @sweeperre.com'})
                    if alt_email_lower.find('qa+') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word qa+'})
                    if alt_email_lower.find('info') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word info'})
                    if alt_email_lower.find('@windermeresolutions.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @windermeresolutions.com'})
                    if alt_email_lower.find('qa-test') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word qa-test'})
                    if alt_email_lower.find('@mw.com') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word @mw.com'})
                    if alt_email_lower.find('moxi') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word moxi'})
                    if alt_email_lower.find('autoboot') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word autoboot'})
                    if alt_email_lower.find('ecomm') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word ecomm'})
                    if alt_email_lower.find('borg000') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word borg000'})
                    if alt_email_lower.find('admin') != -1:
                        garbage_count = garbage_count + 1
                        print('Garbage count ' + str(garbage_count))
                        continue
                        # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email contains word admin'})
                else:
                    garbage_count = garbage_count + 1
                    print('Garbage count ' + str(garbage_count))
                    continue
                    # writer.writerow({'agent_uuid':agent_uuid,'userid':userid,'agent_username':agent_username,'firstname':firstname,'lastname':lastname,'nickname':nickname,'officename_display':officename_display,'officepublickey':officepublickey,'office_addressline1':office_addressline1,'office_addressline2':office_addressline2,'office_city':office_city,'office_state':office_state,'office_zip':office_zip,'office_phone':office_phone,'office_extension':office_extension,'company_legalname':company_legalname,'companypublickey':companypublickey,'lastmodified':lastmodified,'primary_email':primary_email,'secondary_email':secondary_email,'alternate_email':alternate_email,'direct_phone':direct_phone,'mobile_phone':mobile_phone,'title_display':title_display,'accred_display':accred_display,'currentrolename':currentrolename,'user_category':user_category,'title_cleaned':title_cleaned,'team_name':team_name,'team_member_type':team_member_type,'date_deactivated':date_deactivated,'agent_active':agent_active,'at_least_one_mls_association':at_least_one_mls_association,'agent_moxi_hub':agent_moxi_hub,'agent_moxi_dms':agent_moxi_dms,'agent_moxi_engage':agent_moxi_engage,'agent_moxi_present':agent_moxi_present,'agent_moxi_websites':agent_moxi_websites,'agent_moxi_talent':agent_moxi_talent,'Reason':'Alternate email not valid'})

            #Previous agent check logic
            cur.execute('SELECT * from moxiworks.sfdc_contact_data where sfdc_email = %s', (primary_email,))
            exist_check = cur.fetchone()
            if exist_check:
                status = 'update'
                cur.execute('SELECT count(*) from moxiworks.moxiworks_contact where agent_uuid = %s', (agent_uuid,))
                check_duplicate_agent = cur.fetchone()
                if check_duplicate_agent[0] > 0:
                    duplicate_count = duplicate_count + 1
                    continue
                sfdc_agent_id = exist_check['sfdc_contact_id']
                if not exist_check['sfdc_title']:
                    title = title_cleaned
                else:
                    title = exist_check['sfdc_title']
                if officepublickey:
                    cur.execute('SELECT * from moxiworks.moxiworks_office where officepublickey = %s LIMIT 1', (officepublickey,))
                    get_office_data = cur.fetchone()
                    if not get_office_data:
                        garbage_count = garbage_count + 1
                        garbage_array = {'agent_uuid':agent_uuid,'firstname':firstname,'lastname':lastname,'primary_email':primary_email,'alternate_email':alternate_email,'Reason':'Officepublickey is not in the office table'}
                        collect_garbage_array.append(garbage_array)
                        continue
                    else:
                        sf_parent_id = get_office_data['sfdc_office_id']
                else:
                    cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s LIMIT 1', (companypublickey,))
                    get_company_data = cur.fetchone()
                    if not get_company_data:
                        garbage_count = garbage_count + 1
                        garbage_array = {'agent_uuid':agent_uuid,'firstname':firstname,'lastname':lastname,'primary_email':primary_email,'alternate_email':alternate_email,'Reason':'Companypublickey is not in company table'}
                        collect_garbage_array.append(garbage_array)
                        continue
                    else:
                        sf_parent_id = get_company_data['sfdc_account_id']
                try:
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_contact_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id, sfdc_agent_id))
                    print('Data Inserted in moxiworks_contact....')
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact_delta (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id, sfdc_contact_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id,sfdc_agent_id))
                    print('Data Inserted for moxiworks_contact_delta with Update status....')
                    conn.commit()
                    total_updated = total_updated + 1
                except Exception as e:
                    print("type error: " + str(e))
                    print("Failed at ID " + str(data['agent_uuid']))
                    conn.rollback()
                    continue
            else:
                status = 'insert'
                title_cleaned = data['title_cleaned']
                if primary_email:
                    is_valid_primary_email = validate_email(primary_email)
                    if is_valid_primary_email == 0:
                        garbage_count = garbage_count + 1
                        garbage_array = {'agent_uuid':agent_uuid,'firstname':firstname,'lastname':lastname,'primary_email':primary_email,'alternate_email':alternate_email,'Reason':'Primary email is not valid'}
                        collect_garbage_array.append(garbage_array)
                        continue
                if alternate_email:
                    is_valid_alt_email = validate_email(alternate_email)
                    if is_valid_alt_email == 0:
                        garbage_count = garbage_count + 1
                        garbage_array = {'agent_uuid':agent_uuid,'firstname':firstname,'lastname':lastname,'primary_email':primary_email,'alternate_email':alternate_email,'Reason':'Alt email is not valid'}
                        collect_garbage_array.append(garbage_array)
                        continue
                
                cur.execute('SELECT count(*) from moxiworks.moxiworks_contact where agent_uuid = %s', (agent_uuid,))
                check_duplicate_agent = cur.fetchone()
                if check_duplicate_agent[0] > 0:
                    duplicate_count = duplicate_count + 1
                    continue
                if officepublickey:
                    cur.execute('SELECT * from moxiworks.moxiworks_office where officepublickey = %s LIMIT 1', (officepublickey,))
                    get_office_data = cur.fetchone()
                    
                    if not get_office_data:
                        garbage_count = garbage_count + 1
                        garbage_array = {'agent_uuid':agent_uuid,'firstname':firstname,'lastname':lastname,'primary_email':primary_email,'alternate_email':alternate_email,'Reason':'Officepublickey is not in the office table'}
                        collect_garbage_array.append(garbage_array)
                        continue
                    else:
                        sf_parent_id = get_office_data['sfdc_office_id']
                else:
                    cur.execute('SELECT * from moxiworks.moxiworks_account where companypublickey = %s LIMIT 1', (companypublickey,))
                    get_company_data = cur.fetchone()
                    if not get_company_data:
                        garbage_count = garbage_count + 1
                        garbage_array = {'agent_uuid':agent_uuid,'firstname':firstname,'lastname':lastname,'primary_email':primary_email,'alternate_email':alternate_email,'Reason':'Companypublickey is not in company table'}
                        collect_garbage_array.append(garbage_array)
                        continue
                    else:
                        sf_parent_id = get_company_data['sfdc_account_id']
                try:
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, sf_parent_id))
                    print('Data Inserted in moxiworks_contact....')
                    cur.execute(
                        'INSERT INTO moxiworks.moxiworks_contact_delta (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (agent_uuid, userid, agent_username, firstname, lastname, nickname, officename_display, officepublickey, office_addressline1, office_city, office_state, office_zip, office_phone, office_extension, company_legalname, companypublickey, lastmodified, primary_email, secondary_email, alternate_email, direct_phone, mobile_phone, title_display, accred_display, currentrolename, user_category, title_cleaned, team_name, team_member_type, date_deactivated, agent_active, at_least_one_mls_association, agent_moxi_hub, agent_moxi_dms, agent_moxi_engage, agent_moxi_present, agent_moxi_websites, agent_moxi_talent, status, sf_parent_id))
                    print('Data Inserted for moxiworks_contact_delta with Insert status....')
                    conn.commit()
                    total_inserted = total_inserted + 1
                except Exception as e:
                    print("type error: " + str(e))
                    print("Failed at ID " + str(data['agent_uuid']))
                    conn.rollback()
                    continue
        # with open('storage/reports/agent/agent_invalid_email.csv', mode='w') as csv_file:
        #     fieldnames = ['agent_uuid','firstname','lastname', 'primary_email','alternate_email','Reason']
        #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        #     writer.writeheader()
        #     for garbage in collect_garbage_array:
        #         writer.writerow({'agent_uuid':garbage['agent_uuid'],'firstname':garbage['firstname'],'lastname':garbage['lastname'],'primary_email':garbage['primary_email'],'alternate_email':garbage['alternate_email'],'Reason':garbage['Reason']})
        print("Total Inserted: " + str(total_inserted))
        print("Total Updated: " + str(total_updated))
        print("Total Duplicate Data: " + str(duplicate_count))
        print("Total Garbage Data: " + str(garbage_count))
        print("Total Data: " + str(total_data))
#Created a object of the class
import_init_data_obj = import_inital_data()
#create the connection
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv(
    'db_user') + " password=" + os.getenv('db_password'))
#call the import_company_inital_data function
import_init_data_obj.import_agent_inital_data(conn)
