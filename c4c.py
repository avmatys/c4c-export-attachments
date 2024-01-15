import datetime
import json
import requests
import simplejson as simplejson
from main import MODE, Mode, ObjectType
from logger import *

# first file logger
logging = setup_logger('c4capi', 'c4c/api/logging.log')

#logging.basicConfig(level=logging.INFO, filename="c4c/api/logging.log", filemode="a",
#                    format="%(asctime)s;%(levelname)s;%(message)s;")

auth = 'X', 'X'

URLS = {
    Mode.test.name: {
        ObjectType.account.name: "https://myXXXXXX.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection?$filter={filter}&$select=AccountID,Name,ObjectID,CorporateAccountAttachmentFolder/ObjectID,CorporateAccountAttachmentFolder/SizeInkB,CorporateAccountAttachmentFolder/LastUpdatedBy,CorporateAccountAttachmentFolder/LastUpdatedOn,CorporateAccountAttachmentFolder/DocumentLink,CorporateAccountAttachmentFolder/TypeCode,CorporateAccountAttachmentFolder/TypeCodeText,CorporateAccountAttachmentFolder/MimeType,CorporateAccountAttachmentFolder/Binary,CorporateAccountAttachmentFolder/Name&$expand=CorporateAccountAttachmentFolder&$format=json"
    },
    Mode.prod.name: {
        ObjectType.account.name: "https://myXXXXXX.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection?$filter={key}&$select=AccountID,Name,ObjectID,CorporateAccountAttachmentFolder/ObjectID,CorporateAccountAttachmentFolder/SizeInkB,CorporateAccountAttachmentFolder/LastUpdatedBy,CorporateAccountAttachmentFolder/LastUpdatedOn,CorporateAccountAttachmentFolder/DocumentLink,CorporateAccountAttachmentFolder/TypeCode,CorporateAccountAttachmentFolder/TypeCodeText,CorporateAccountAttachmentFolder/MimeType,CorporateAccountAttachmentFolder/Binary,CorporateAccountAttachmentFolder/Name&$expand=CorporateAccountAttachmentFolder&$format=json"
    }
}


def form_filter(object_type, keys):
    filter_con = None
    if object_type.name == ObjectType.account.name or object_type.name == ObjectType.activity.name:
        for i, key in enumerate(keys):
            key = f"ObjectID eq '{key}'"
            keys[i] = key
        filter_con = ' or '.join(keys)
    return filter_con


def get_url(object_type):
    url = URLS[MODE.name][object_type.name]
    return url


def get_data(keys, object_type, client=None):
    if client is None:
        client = requests.session()
    url_template = get_url(object_type)
    if url_template is None:
        logging.error(f"GET;{url_template};URL template for {object_type.name} doesn't exist")
        return None
    if keys is None:
        logging.error(f"GET;{url_template};Keys don't set")
        return None
    if object_type is None:
        logging.error(f"GET;{url_template};Object type doesn't set")
        return None
    filter_con = form_filter(object_type, keys)
    if filter_con is None:
        logging.error(f"GET;{url_template};Filter condition is not set for {keys} {object_type.name}")
        return None
    url = url_template.format(filter=filter_con)
    logging.info(f"GET;{url};Requesting data")
    # Get data
    try:
        response = client.get(url=url, auth=auth, headers={"x-csrf-token": "fetch", "Accept": "application/json"})
        if response is None:
            logging.warning(f"GET;{url};Response is None")
    except requests.exceptions.RequestException as e:
        error = repr(e)
        logging.error(f"GET;{url};Request exception {error}")
        return None
    # Parse data
    try:
        response_json = response.json()
    except (ValueError, simplejson.JSONDecodeError, json.JSONDecodeError):
        logging.error(f"GET;{url};Can't parse response {response}")
        return None
    # Check data
    if len(response_json) == 0 or "d" not in response_json or "results" not in response_json["d"]:
        logging.error(f"GET;{url};Can't understand response {response}")
        return None
    if 'error' in response:
        logging.error(f"GET;{url};Error in response {response['error']}")
        return None
    # Response is correct
    response_json = response_json["d"]["results"]
    return response_json
