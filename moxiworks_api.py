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

def moxiworks_company_import_boomi_process_start():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "f656f4e0-510e-40b2-896a-942d6a044119",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_office_import_boomi_process_start():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "d3d31fe4-9790-4650-a9f6-4b00e66aeaba",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_agent_import_boomi_process_start():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "4750a00f-04fe-424b-bf20-5ebe2ee05285",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_monthly_report_reverse_sync_start():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "e944dab0-a6a8-4307-83e2-d449e731fa3c",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_sync_manually_inserted_company_process_start():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "eec3de15-b4a1-4864-8953-40e9a6929178",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_inactive_agent_deletion_start():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "e723544d-f713-4015-8063-8a922f695830",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_agent_sfdc_id_sync():
    conn = http.client.HTTPSConnection("api.boomi.com")
    payload = '''
        {
            "ProcessProperties" : {
                "@type" : "ProcessProperties",
                "ProcessProperty" : [
                    {
                        "@type" : "",
                        "Name" : "priority",
                        "Value": "medium"
                    }
                ]
            },
            "processId" : "a25fb6e7-3bfd-4446-8c58-0b9a34439c61",
            "atomId" : "3bfcf40e-5e4b-4639-8abe-d9862801a326"
        }'''

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/executeProcess", payload, headers)

def moxiworks_company_boomi_process_status():
    date_response = date_range(10, datetime.datetime.utcnow())
    payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["f656f4e0-510e-40b2-896a-942d6a044119"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn = http.client.HTTPSConnection("api.boomi.com")
    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

    res = conn.getresponse()
    data = res.read()

    xmlstring=data.decode("utf-8")

    xml_new_string = xmlstring.replace('bns:', '')


    tree = ET.ElementTree(ET.fromstring(xml_new_string))
    root = tree.getroot()

    for result in root.findall('result'):
        status = result.find('status').text
    return status

def moxiworks_office_boomi_process_status():
    date_response = date_range(10, datetime.datetime.utcnow())
    payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["d3d31fe4-9790-4650-a9f6-4b00e66aeaba"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn = http.client.HTTPSConnection("api.boomi.com")
    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

    res = conn.getresponse()
    data = res.read()


    xmlstring=data.decode("utf-8")

    xml_new_string = xmlstring.replace('bns:', '')


    tree = ET.ElementTree(ET.fromstring(xml_new_string))
    root = tree.getroot()

    for result in root.findall('result'):
        status = result.find('status').text
    return status

def moxiworks_agent_boomi_process_status():
    date_response = date_range(10, datetime.datetime.utcnow())
    payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["4750a00f-04fe-424b-bf20-5ebe2ee05285"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn = http.client.HTTPSConnection("api.boomi.com")
    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

    res = conn.getresponse()
    data = res.read()


    xmlstring=data.decode("utf-8")

    xml_new_string = xmlstring.replace('bns:', '')


    tree = ET.ElementTree(ET.fromstring(xml_new_string))
    root = tree.getroot()

    for result in root.findall('result'):
        status = result.find('status').text
    return status

def moxiworks_monthly_report_reverse_sync_status():
    date_response = date_range(10, datetime.datetime.utcnow())
    payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["e944dab0-a6a8-4307-83e2-d449e731fa3c"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn = http.client.HTTPSConnection("api.boomi.com")
    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

    res = conn.getresponse()
    data = res.read()

    xmlstring=data.decode("utf-8")

    xml_new_string = xmlstring.replace('bns:', '')


    tree = ET.ElementTree(ET.fromstring(xml_new_string))
    root = tree.getroot()

    for result in root.findall('result'):
        status = result.find('status').text
    return status

def moxiworks_manually_inserted_company_sync_status():
    date_response = date_range(10, datetime.datetime.utcnow())
    payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["eec3de15-b4a1-4864-8953-40e9a6929178"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn = http.client.HTTPSConnection("api.boomi.com")
    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

    res = conn.getresponse()
    data = res.read()

    xmlstring=data.decode("utf-8")

    xml_new_string = xmlstring.replace('bns:', '')


    tree = ET.ElementTree(ET.fromstring(xml_new_string))
    root = tree.getroot()

    for result in root.findall('result'):
        status = result.find('status').text
    return status

def moxiworks_inactive_process_deletion_process_status():
    date_response = date_range(10, datetime.datetime.utcnow())
    payload = '{"QueryFilter":{"expression":{"operator":"and","nestedExpression":[{"argument":["%s","%s"],"operator":"BETWEEN","property":"executionTime"},{"argument":["e723544d-f713-4015-8063-8a922f695830"],"operator":"EQUALS","property":"processId"}]}}}' % (date_response['start_date'], date_response['end_date'])

    headers = {
        'authorization': "Basic c2hhaEBjbG91ZGx5LmlvOlBhdWxAYnRoNw==",
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    conn = http.client.HTTPSConnection("api.boomi.com")
    conn.request("POST", "/api/rest/v1/moxiworks-C5DDL7/ExecutionRecord/query", payload, headers)

    res = conn.getresponse()
    data = res.read()

    xmlstring=data.decode("utf-8")

    xml_new_string = xmlstring.replace('bns:', '')


    tree = ET.ElementTree(ET.fromstring(xml_new_string))
    root = tree.getroot()

    for result in root.findall('result'):
        status = result.find('status').text
    return status