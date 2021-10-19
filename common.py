from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import boto3
from botocore.exceptions import ClientError
import datetime
from dotenv import load_dotenv, find_dotenv
import os

def to_bool(value):
    valid = {
        'true': True, 't': True, '1': True,
        'false': False, 'f': False, '0': False, '': False,
    }

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)

def send_company_report(report_title, total_data, total_inserted, total_updated,total_data_processed,garbage_count,valid_data_from_csv):
    
    SENDER = os.getenv('sender_email') # "Moxiworks <mahdi@cloudly.io>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = os.getenv('to_email') #"bokul@cloudly.io"

    AWS_REGION = os.getenv('region_email') #"us-east-1"
    DATE = str(format(datetime.date.today()))
    SUBJECT = report_title + " Report of %s " % (DATE)
    
    client = boto3.client('ses', aws_access_key_id=os.getenv('email_aws_access_key_id'),
                          aws_secret_access_key=os.getenv('email_aws_secret_access_key'),
                          region_name=AWS_REGION)
    message = MIMEMultipart()
    message['Subject'] = SUBJECT
    message['From'] = SENDER
    message['To'] = RECIPIENT

    

    BODY_HTML = """\
            <html>
                <head></head>
                <body>
                    <h1>Report of {date} </h1>
                    <table style="width: 50%; float: left;" border="2">
                        <tr>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Inserted</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Updated</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Valid Data from CSV</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Error Count</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Processed from CSV</strong></td>
                        </tr>
                        <tr>
                            <td style="border: 1px solid black;text-align:left;">{total_inserted}</td>
                            <td style="border: 1px solid black;text-align:left;">{total_updated}</td>
                            <td style="border: 1px solid black;text-align:left;">{valid_data_from_csv}</td>
                            <td style="border: 1px solid black;text-align:left;">{garbage_count}</td>
                            <td style="border: 1px solid black;text-align:left;">{total_data_processed}</td>
                        </tr>
                    </table>
                </body>
            </html>
            """.format(date=DATE, total_inserted=total_inserted, total_updated=total_updated, valid_data_from_csv=valid_data_from_csv,garbage_count = garbage_count,total_data_processed=total_data_processed)
    # message body
    part = MIMEText(BODY_HTML, 'html')
    message.attach(part)

    # attachment error report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/company_error_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_company_error_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_company_error_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_company_error_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment insert report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/company_insert_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_company_insert_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_company_insert_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_company_insert_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment update report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/company_update_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_company_update_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_company_update_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_company_update_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment inactive report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/company_inactive_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_company_inactive_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_company_inactive_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_company_inactive_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    # The character encoding for the email.
    # CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    # client = boto3.client('ses', region_name=AWS_REGION)
    print (SENDER)
    
    
    # Try to send the email.
    try:
        response = client.send_raw_email(
            Source = message['From'],
            Destinations = [message['To']],
            RawMessage = {
                'Data': message.as_string()
            }
        )
        # # Provide the contents of the email.
        # response = client.send_email(
        #     Destination={
        #         'ToAddresses': [
        #             RECIPIENT,
        #         ],
        #     },
        #     Message={
        #         'Body': {
        #             'Html': {
        #                 'Charset': CHARSET,
        #                 'Data': BODY_HTML,
        #             },
        #         },
        #         'Subject': {
        #             'Charset': CHARSET,
        #             'Data': SUBJECT,
        #         },
        #     },
        #     Source=SENDER,
        #     # If you are not using a configuration set, comment or delete the
        #     # following line
        #     # ConfigurationSetName=CONFIGURATION_SET,
        # )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def send_office_report(report_title, total_data, total_inserted, total_updated,total_data_processed,garbage_count,valid_data_from_csv):
    SENDER = os.getenv('sender_email') # "Moxiworks <mahdi@cloudly.io>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = os.getenv('to_email') #"bokul@cloudly.io"

    AWS_REGION = os.getenv('region_email') #"us-east-1"
    DATE = str(format(datetime.date.today()))
    SUBJECT = report_title + " Report of %s " % (DATE)
    
    client = boto3.client('ses', aws_access_key_id=os.getenv('email_aws_access_key_id'),
                          aws_secret_access_key=os.getenv('email_aws_secret_access_key'),
                          region_name=AWS_REGION)
    message = MIMEMultipart()
    message['Subject'] = SUBJECT
    message['From'] = SENDER
    message['To'] = RECIPIENT

    

    BODY_HTML = """\
            <html>
                <head></head>
                <body>
                    <h1>Report of {date} </h1>
                    <table style="width: 50%; float: left;" border="2">
                        <tr>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Inserted</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Updated</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Valid Data from CSV</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Error Count</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Processed from CSV</strong></td>
                        </tr>
                        <tr>
                            <td style="border: 1px solid black;text-align:left;">{total_inserted}</td>
                            <td style="border: 1px solid black;text-align:left;">{total_updated}</td>
                            <td style="border: 1px solid black;text-align:left;">{valid_data_from_csv}</td>
                            <td style="border: 1px solid black;text-align:left;">{garbage_count}</td>
                            <td style="border: 1px solid black;text-align:left;">{total_data_processed}</td>
                        </tr>
                    </table>
                </body>
            </html>
            """.format(date=DATE, total_inserted=total_inserted, total_updated=total_updated, valid_data_from_csv=valid_data_from_csv,garbage_count = garbage_count,total_data_processed=total_data_processed)
    # message body
    part = MIMEText(BODY_HTML, 'html')
    message.attach(part)

    
    # attachment error report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/office_error_report'
    full_path = directory + store
    
    if os.path.isfile(full_path + '/moxiworks_office_error_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_office_error_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_office_error_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)

    # attachment insert report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/office_insert_report'
    full_path = directory + store
    
    if os.path.isfile(full_path + '/moxiworks_office_insert_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_office_insert_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_office_insert_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    # attachment update report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/office_update_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_office_update_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_office_update_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_office_update_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)

    # attachment inactive report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/office_inactive_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_office_inactive_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_office_inactive_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_office_inactive_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    # The character encoding for the email.
    # CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    # client = boto3.client('ses', region_name=AWS_REGION)
    print (SENDER)
    
    
    # Try to send the email.
    try:
        response = client.send_raw_email(
            Source = message['From'],
            Destinations = [message['To']],
            RawMessage = {
                'Data': message.as_string()
            }
        )
        # # Provide the contents of the email.
        # response = client.send_email(
        #     Destination={
        #         'ToAddresses': [
        #             RECIPIENT,
        #         ],
        #     },
        #     Message={
        #         'Body': {
        #             'Html': {
        #                 'Charset': CHARSET,
        #                 'Data': BODY_HTML,
        #             },
        #         },
        #         'Subject': {
        #             'Charset': CHARSET,
        #             'Data': SUBJECT,
        #         },
        #     },
        #     Source=SENDER,
        #     # If you are not using a configuration set, comment or delete the
        #     # following line
        #     # ConfigurationSetName=CONFIGURATION_SET,
        # )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def send_agent_report(report_title, total_data, total_inserted, total_updated,total_data_processed,garbage_count,valid_data_from_csv):
    SENDER = os.getenv('sender_email') # "Moxiworks <mahdi@cloudly.io>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = os.getenv('to_email') #"bokul@cloudly.io"

    AWS_REGION = os.getenv('region_email') #"us-east-1"
    DATE = str(format(datetime.date.today()))
    SUBJECT = report_title + " Report of %s " % (DATE)
    
    client = boto3.client('ses', aws_access_key_id=os.getenv('email_aws_access_key_id'),
                          aws_secret_access_key=os.getenv('email_aws_secret_access_key'),
                          region_name=AWS_REGION)
    message = MIMEMultipart()
    message['Subject'] = SUBJECT
    message['From'] = SENDER
    message['To'] = RECIPIENT

    

    BODY_HTML = """\
            <html>
                <head></head>
                <body>
                    <h1>Report of {date} </h1>
                    <table style="width: 50%; float: left;" border="2">
                        <tr>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Inserted</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Updated</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Valid Data from CSV</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Error Count</strong></td>
                            <td style="border: 1px solid black;text-align:left;"><strong>Total Data Processed from CSV</strong></td>
                        </tr>
                        <tr>
                            <td style="border: 1px solid black;text-align:left;">{total_inserted}</td>
                            <td style="border: 1px solid black;text-align:left;">{total_updated}</td>
                            <td style="border: 1px solid black;text-align:left;">{valid_data_from_csv}</td>
                            <td style="border: 1px solid black;text-align:left;">{garbage_count}</td>
                            <td style="border: 1px solid black;text-align:left;">{total_data_processed}</td>
                        </tr>
                    </table>
                </body>
            </html>
            """.format(date=DATE, total_inserted=total_inserted, total_updated=total_updated, valid_data_from_csv=valid_data_from_csv,garbage_count = garbage_count,total_data_processed=total_data_processed)
    # message body
    part = MIMEText(BODY_HTML, 'html')
    message.attach(part)

    # attachment error report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_error_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_agent_error_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_agent_error_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_agent_error_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)

    # attachment insert report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_insert_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_agent_insert_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_agent_insert_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_agent_insert_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)

    # attachment update report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_update_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_agent_update_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_agent_update_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_agent_update_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment inactive report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_inactive_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_agent_inactive_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_agent_inactive_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_agent_inactive_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment delete candidate report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_delete_candidate_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_agent_delete_candidate_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_agent_delete_candidate_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_agent_delete_candidate_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    # The character encoding for the email.
    # CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    # client = boto3.client('ses', region_name=AWS_REGION)
    print (SENDER)
    
    
    # Try to send the email.
    try:
        response = client.send_raw_email(
            Source = message['From'],
            Destinations = [message['To']],
            RawMessage = {
                'Data': message.as_string()
            }
        )
        # # Provide the contents of the email.
        # response = client.send_email(
        #     Destination={
        #         'ToAddresses': [
        #             RECIPIENT,
        #         ],
        #     },
        #     Message={
        #         'Body': {
        #             'Html': {
        #                 'Charset': CHARSET,
        #                 'Data': BODY_HTML,
        #             },
        #         },
        #         'Subject': {
        #             'Charset': CHARSET,
        #             'Data': SUBJECT,
        #         },
        #     },
        #     Source=SENDER,
        #     # If you are not using a configuration set, comment or delete the
        #     # following line
        #     # ConfigurationSetName=CONFIGURATION_SET,
        # )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def send_boomi_process_failed_email(process_name):
    
    SENDER = os.getenv('sender_email') # "Moxiworks <mahdi@cloudly.io>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = os.getenv('to_email') #"bokul@cloudly.io"

    AWS_REGION = os.getenv('region_email') #"us-east-1"

    DATE = str(format(datetime.date.today()))

    SUBJECT = process_name + " of %s " % (DATE) + "has failed"

    BODY_HTML = """\
            <html>
                <head></head>
                <body>
                    <h1>Dear concern</h1>
                     <p>A Boomi process named <b>{process_name}</b> has failed. Please check the Boomi process reporting console more information.</p>
                     <p>Thank you and sorry for your inconvenience</p>
                </body>
            </html>
            """.format(process_name=process_name)

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    # client = boto3.client('ses', region_name=AWS_REGION)
    print (SENDER)
    client = boto3.client('ses', aws_access_key_id=os.getenv('email_aws_access_key_id'),
                          aws_secret_access_key=os.getenv('email_aws_secret_access_key'),
                          region_name=AWS_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # If you are not using a configuration set, comment or delete the
            # following line
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def send_monthly_dbsync_report():
    
    SENDER = os.getenv('sender_email') # "Moxiworks <mahdi@cloudly.io>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = os.getenv('to_email') #"bokul@cloudly.io"

    AWS_REGION = os.getenv('region_email') #"us-east-1"
    DATE = str(format(datetime.date.today()))
    SUBJECT = "Monthly" + " Report of Moxiworks DBSync process %s " % (DATE)
    
    client = boto3.client('ses', aws_access_key_id=os.getenv('email_aws_access_key_id'),
                          aws_secret_access_key=os.getenv('email_aws_secret_access_key'),
                          region_name=AWS_REGION)
    message = MIMEMultipart()
    message['Subject'] = SUBJECT
    message['From'] = SENDER
    message['To'] = RECIPIENT

    

    BODY_HTML = """\
            <html>
                <head></head>
                <body>
                    <h1>Dear concern,</h1>
                     <p>Hope everything going well at Moxiworks. This mail is sent to deliver you the monthly report of Moxiworks SFDC DBSync process monthly report.</p>
                     <p>With this mail we are attaching the monthly report for <b>Company</b>, <b>Office</b> & <b>Agent</b>.</p>
                     <p>Thank you for your time.</p>
                </body>
            </html>
            """
    
    part = MIMEText(BODY_HTML, 'html')
    message.attach(part)

    # attachment company monthly report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/company_monthly_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_company_monthly_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_company_monthly_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_company_monthly_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment office monthly report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/office_monthly_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_office_monthly_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_office_monthly_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_office_monthly_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    
    # attachment agent monthly report
    directory = str(os.path.dirname(os.path.realpath(__file__)))
    store = '/storage/agent_monthly_report'
    full_path = directory + store
    if os.path.isfile(full_path + '/moxiworks_agent_monthly_report_'+ format(datetime.date.today()) + '.csv'):
        attachment_file = full_path + '/moxiworks_agent_monthly_report_'+ format(datetime.date.today()) + '.csv'
        part = MIMEApplication(open(attachment_file, 'rb').read())
        filename_attachment = 'moxiworks_agent_monthly_report_'+ format(datetime.date.today()) + '.csv'
        part.add_header('Content-Disposition', 'attachment', filename=filename_attachment)
        message.attach(part)
    # The character encoding for the email.
    # CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    # client = boto3.client('ses', region_name=AWS_REGION)
    print (SENDER)
    
    
    # Try to send the email.
    try:
        response = client.send_raw_email(
            Source = message['From'],
            Destinations = [message['To']],
            RawMessage = {
                'Data': message.as_string()
            }
        )
        
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])