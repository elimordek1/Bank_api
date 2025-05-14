import requests
import xml.etree.ElementTree as Et
import xmltodict
import json
import os
import pandas as pd

# Constants
TBC_CERT_BASE_PATH = r"C:\Users\arkik\DataspellProjects\POLI_BANK\TBC_Cert"

# Abbreviation map
CERTIFICATE_COMPANIES = {
    'BRG': 'BEST RETAIL GEORGIA LLC',
    'FRG': 'FASHION RETAIL GEORGIA LLC',
    'GAG': 'GLOBAL APPAREL GEORGIA LLC',
    'MHR': 'MASTER HOME RETAIL LLC',
    'MRG': 'MASTER RETAIL GEORGIA LLC',
    'MSG': 'MEGA STORE GEORGIA LLC',
    'PRG': 'PRO RETAIL GEORGIA LLC',
    'RGG': 'RETAIL GROUP GEORGIA LLC',
    'RGH': 'RETAIL GROUP HOLDING LLC',
    'SRG': 'SPANISH RETAIL GEORGIA LLC',
}

# TBC credentials
TBC_CREDENTIALS = {
    'SRG': {'username': 'SRG_1', 'password': 'CCQ1BZ2F'},
    'RGG': {'username': 'RGG_1', 'password': 'QY8VNDBK'},
    'MRG': {'username': 'MRG_1', 'password': 'R5VCE3GM'},
    'BRG': {'username': 'BRG_1', 'password': 'LTGZZHW5'},
    'PRG': {'username': 'PRG_1', 'password': 'V7CP3ERP'},
    'MSG': {'username': 'MSG_1', 'password': 'PB9FMS46'},
    'FRG': {'username': 'FRG_1', 'password': 'GG4PRYBK'},
    'MHR': {'username': 'MHR_1', 'password': '6D2MTAYD'},
    'RGH': {'username': 'RGH_1', 'password': '8NG1SBNU'},
    'GAG': {'username': 'GAG_1', 'password': 'YXXXX9PB'},
}

# SOAP Setup
TBC_URL = "https://secdbi.tbconline.ge/dbi/dbiService"
HEADERS = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://www.mygemini.com/schemas/mygemini/GetAccountMovements'
}
NAMESPACES = {
    'ns2': 'http://www.mygemini.com/schemas/mygemini'
}
SOAP_PAYLOAD = '''
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:myg="http://www.mygemini.com/schemas/mygemini" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <soapenv:Header>
        <wsse:Security>
            <wsse:UsernameToken>
                <wsse:Username>{username}</wsse:Username>
                <wsse:Password>{password}</wsse:Password>
                <wsse:Nonce>{digipass}</wsse:Nonce>
            </wsse:UsernameToken>
        </wsse:Security>
    </soapenv:Header>
    <soapenv:Body>
        <myg:GetAccountMovementsRequestIo>
            <myg:accountMovementFilterIo>
                <myg:accountNumber>{account_number}</myg:accountNumber>
                <myg:accountCurrencyCode>{currency}</myg:accountCurrencyCode>
                <myg:periodFrom>{start_datetime}</myg:periodFrom>
                <myg:periodTo>{end_datetime}</myg:periodTo>
            </myg:accountMovementFilterIo>
        </myg:GetAccountMovementsRequestIo>
    </soapenv:Body>
</soapenv:Envelope>
'''

def remove_namespaces(data):
    if isinstance(data, dict):
        return {key.split(':')[-1]: remove_namespaces(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [remove_namespaces(item) for item in data]
    return data

def get_cert_paths(company_abbr):
    company_name = CERTIFICATE_COMPANIES[company_abbr]
    folder_path = os.path.join(TBC_CERT_BASE_PATH, company_name)
    return (
        os.path.join(folder_path, 'server_cert.pem'),
        os.path.join(folder_path, 'key_unencrypted.pem'),
    )

def get_transactions(company_abbr, account_number, currency, start_datetime, end_datetime):
    creds = TBC_CREDENTIALS[company_abbr]
    cert_path = get_cert_paths(company_abbr)

    payload = SOAP_PAYLOAD.format(
        username=creds['username'],
        password=creds['password'],
        digipass='1111',  # Constant value works for now
        account_number=account_number,
        currency=currency,
        start_datetime=start_datetime,
        end_datetime=end_datetime
    )

    response = requests.post(
        TBC_URL,
        data=payload,
        headers=HEADERS,
        cert=cert_path,
        verify=True
    )

    if not response.ok:
        raise Exception(f"TBC API Error: {response.status_code} {response.text}")

    xml_response = response.content.decode('utf-8')
    root = Et.fromstring(xml_response)

    # Find relevant section
    result_node = root.find('.//ns2:result', namespaces=NAMESPACES)
    if result_node is None:
        raise Exception("No 'result' node found in response")

    raw_data = xmltodict.parse(Et.tostring(root, encoding='unicode'))
    cleaned_data = remove_namespaces(raw_data)
    return json.dumps(cleaned_data, indent=4, ensure_ascii=False)

# Example usage
if __name__ == "__main__":
    company_abbr = "RGG"
    account_number = "GE29TB1234567890123456"
    currency = "GEL"
    start = "2025-05-01T00:00:00.000"
    end = "2025-05-10T23:59:59.999"

    data = get_transactions(company_abbr, account_number, currency, start, end)
    print(data)
