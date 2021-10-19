import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv, find_dotenv

# from moxi_company_import_data import import_data as moxi_company_import_alias
# from moxi_office_import_data import import_data as moxi_office_import_alias
# from moxi_agent_import_data import import_data as moxi_agent_import_alias


load_dotenv(find_dotenv())

class utilities():

    # def seeder(self, conn):
    #     print("Company... ")
    #     company_obj = moxi_company_import_alias()
    #     company_obj.import_data_in_company(conn)
    #
    #     print("\nOffice... ")
    #     office_obj = moxi_office_import_alias()
    #     office_obj.import_data_in_office(conn)
    #
    #     print("\nAgent... ")
    #     agent_obj = moxi_agent_import_alias()
    #     agent_obj.import_data_in_agent(conn)


    def truncate_tables(self, conn):
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("TRUNCATE TABLE moxiworks.moxi_company_office_rel, moxiworks.moxiworks_report, moxiworks.moxiworks_account, moxiworks.moxiworks_account_delta, moxiworks.moxiworks_contact, moxiworks.moxiworks_contact_delta;")
        conn.commit()

        if(cur):
            print('Truncated tables: \n moxi_company_office_rel\n moxiworks_report\n moxiworks_account\n moxiworks_account_delta\n moxiworks_contact\n moxiworks_contact_delta')


    def check_function(self, conn):
        # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # object_processed = 'officse'
        # cur.execute("SELECT * FROM moxiworks.moxiworks_report WHERE object_processed = '{}' LIMIT 1". format(object_processed))
        # report_records = cur.fetchone()
        #
        # if(report_records):
        #     print(report_records)
        # else:
        #     print(8)
        collect_garbage_array = []
        sf_parent_id = 'hhh'

        garbage_array = {'sf_parent_id': sf_parent_id, 'reason': ''}
        garbage_array['reason'] = 'Reason .. . ..  '
        collect_garbage_array.append(garbage_array)

        for prb in collect_garbage_array:
            print (prb['sf_parent_id'] + ' ID::' + prb['reason'])



utilitie_obj = utilities()
conn = psycopg2.connect("host=" + os.getenv('db_host') + " dbname=" + os.getenv('db_name') + " user=" + os.getenv('db_user') + " password=" + os.getenv('db_password'))
utilitie_obj.check_function(conn)