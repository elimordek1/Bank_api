import requests
import os
import xml.etree.ElementTree as Et
import xmltodict
import json

# Constants
TBC_URL = "https://secdbi.tbconline.ge/dbi/dbiService"
TBC_CERT_BASE_PATH = r"C:\Users\arkik\DataspellProjects\POLI_BANK\TBC_Cert"

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

NAMESPACES = {
    'ns2': 'http://www.mygemini.com/schemas/mygemini'
}

def get_cert_paths(company_abbr):
    company_name = CERTIFICATE_COMPANIES[company_abbr]
    folder_path = os.path.join(TBC_CERT_BASE_PATH, company_name)
    return (
        os.path.join(folder_path, 'server_cert.pem'),
        os.path.join(folder_path, 'key_unencrypted.pem'),
    )

def remove_namespaces(data):
    if isinstance(data, dict):
        return {key.split(':')[-1]: remove_namespaces(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [remove_namespaces(item) for item in data]
    return data

def change_password_with_cert(company_abbr, username, current_password, new_password,digipass):
    cert = get_cert_paths(company_abbr)

    payload = f'''
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:myg="http://www.mygemini.com/schemas/mygemini"
                      xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <soapenv:Header>
        <wsse:Security>
          <wsse:UsernameToken>
            <wsse:Username>{username}</wsse:Username>
            <wsse:Password>{current_password}</wsse:Password>
            <wsse:Nonce>{digipass}</wsse:Nonce>
          </wsse:UsernameToken>
        </wsse:Security>
      </soapenv:Header>
      <soapenv:Body>
        <myg:ChangePasswordRequestIo>
          <myg:newPassword>{new_password}</myg:newPassword>
        </myg:ChangePasswordRequestIo>
      </soapenv:Body>
    </soapenv:Envelope>
    '''

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://www.mygemini.com/schemas/mygemini/ChangePassword'
    }

    response = requests.post(
        TBC_URL,
        data=payload,
        headers=headers,
        cert=cert,
        verify=True  # Ensure TLS 1.2
    )

    if not response.ok:
        raise Exception(f"Password change failed: {response.status_code} - {response.text}")

    xml_response = response.content.decode('utf-8')
    root = Et.fromstring(xml_response)

    raw_data = xmltodict.parse(xml_response)
    cleaned = remove_namespaces(raw_data)

    return json.dumps(cleaned, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    result = change_password_with_cert(
        company_abbr="RGG",
        username="RGG_1",
        current_password="QY8VNDBK",
        new_password="ASDasd12334!@",
        digipass="059750"
    )
    print(result)
