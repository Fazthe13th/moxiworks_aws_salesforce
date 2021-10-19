import http.client
import xml.etree.ElementTree as ET
import datetime

def date_range(hours, date_obj):
    if type(date_obj) is datetime.datetime and type(hours) is int:
        start_date_obj = date_obj - datetime.timedelta(hours=hours)

        start_date = start_date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        response = {'start_date':  start_date, 'end_date': end_date}

        return response
    else:
        raise TypeError('Worng arguments type!')
# def moxiworks_agent_boomi_process_status():
#     date_response = date_range(22, datetime.datetime.utcnow())
#     payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["e944dab0-a6a8-4307-83e2-d449e731fa3c"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

#     headers = {
#         'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
#         'content-type': "application/json",
#         'cache-control': "no-cache"
#         }

#     conn = http.client.HTTPSConnection("api.boomi.com")
#     conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

#     res = conn.getresponse()
#     data = res.read()


#     xmlstring=data.decode("utf-8")

#     xml_new_string = xmlstring.replace('bns:', '')


#     tree = ET.ElementTree(ET.fromstring(xml_new_string))
#     root = tree.getroot()

#     for result in root.findall('result'):
#         status = result.find('status').text
#     print(status)
# moxiworks_agent_boomi_process_status()
