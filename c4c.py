import datetime
import json
import requests
import simplejson as simplejson
from main import MODE, Mode, ObjectType
from logger import *

# first file logger
logging = setup_logger('c4capi', 'c4c/api/logging.log')

# logging.basicConfig(level=logging.INFO, filename="c4c/api/logging.log", filemode="a",
#                    format="%(asctime)s;%(levelname)s;%(message)s;")

auth = 'am1', 'Password1234'

URLS = {
    Mode.test.name: {
        ObjectType.account.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection?$filter={filter}&$select=AccountID,Name,ObjectID,CorporateAccountAttachmentFolder/ObjectID,CorporateAccountAttachmentFolder/SizeInkB,CorporateAccountAttachmentFolder/LastUpdatedBy,CorporateAccountAttachmentFolder/LastUpdatedOn,CorporateAccountAttachmentFolder/DocumentLink,CorporateAccountAttachmentFolder/TypeCode,CorporateAccountAttachmentFolder/TypeCodeText,CorporateAccountAttachmentFolder/MimeType,CorporateAccountAttachmentFolder/Binary,CorporateAccountAttachmentFolder/Name&$expand=CorporateAccountAttachmentFolder&$format=json",
        ObjectType.activity.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_activity/ActivityCollection?$filter={filter}&$select=ID,SubjectName,TypeCode,ObjectID,ActivityAttachmentFolder/ObjectID,ActivityAttachmentFolder/SizeInkB,ActivityAttachmentFolder/LastUpdatedBy,ActivityAttachmentFolder/LastUpdatedOn,ActivityAttachmentFolder/DocumentLink,ActivityAttachmentFolder/TypeCode,ActivityAttachmentFolder/TypeCodeText,ActivityAttachmentFolder/MimeType,ActivityAttachmentFolder/Binary,ActivityAttachmentFolder/Name&$expand=ActivityAttachmentFolder&$format=json",
        ObjectType.oppty.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/OpportunityCollection?$filter={filter}&$select=ID,Name,ObjectID,ProcessingTypeCode,OpportunityAttachmentFolder/ObjectID,OpportunityAttachmentFolder/LastUpdatedBy,OpportunityAttachmentFolder/LastUpdatedOn,OpportunityAttachmentFolder/DocumentLink,OpportunityAttachmentFolder/TypeCode,OpportunityAttachmentFolder/TypeCodeText,OpportunityAttachmentFolder/MimeType,OpportunityAttachmentFolder/Name,OpportunityAttachmentFolder/Binary&$expand=OpportunityAttachmentFolder&$format=json",
        ObjectType.charinoopty.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_charinopp/CharacteristicsInOpportunityRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID,CharacteristicsDocument",
        ObjectType.detspecifhistory.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_detspecifhistory/DetailedSpecificationHistoryRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID,RequestForConditionsID_RC",
        ObjectType.specifhistory.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_specifhistory/SpecificationHistoryRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.discountinoopty.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_discountinopp/DiscountInOpportunityRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID,RequestForConditionsID",
        ObjectType.expertadvice.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_expertadvice/ExpertAdviceParticipationDecisionPlanCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID,Task",
        ObjectType.pilotbatch.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_pilotbatch/PilotBatchOrderForProductionOfPilotBatchCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.specpaymentterms.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/att_specpaymentterms/SpecialPaymentTermsRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID,PeymTerms",
        ObjectType.attachment.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/attachment_folder_export/AttachmentRootCollection?$filter={filter}&$select=AttachmentID,ObjectID,AttachmentAttachmentFolder/ObjectID,AttachmentAttachmentFolder/SizeInkB,AttachmentAttachmentFolder/LastUpdatedBy,AttachmentAttachmentFolder/LastUpdatedOn,AttachmentAttachmentFolder/DocumentLink,AttachmentAttachmentFolder/TypeCode,AttachmentAttachmentFolder/TypeCodeText,AttachmentAttachmentFolder/MimeType,AttachmentAttachmentFolder/Binary,AttachmentAttachmentFolder/Name&$expand=AttachmentAttachmentFolder&$format=json",
        ObjectType.techtask.name: "https://my327208.crm.ondemand.com/sap/c4c/odata/cust/v1/attachment_folder_export/AttachmentRootCollection?$filter={filter}&$select=AttachmentID,ObjectID,AttachmentAttachmentFolder/ObjectID,AttachmentAttachmentFolder/SizeInkB,AttachmentAttachmentFolder/LastUpdatedBy,AttachmentAttachmentFolder/LastUpdatedOn,AttachmentAttachmentFolder/DocumentLink,AttachmentAttachmentFolder/TypeCode,AttachmentAttachmentFolder/TypeCodeText,AttachmentAttachmentFolder/MimeType,AttachmentAttachmentFolder/Binary,AttachmentAttachmentFolder/Name&$expand=AttachmentAttachmentFolder&$format=json"
    },
    Mode.prod.name: {
        ObjectType.account.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection?$filter={key}&$select=AccountID,Name,ObjectID,CorporateAccountAttachmentFolder/ObjectID,CorporateAccountAttachmentFolder/SizeInkB,CorporateAccountAttachmentFolder/LastUpdatedBy,CorporateAccountAttachmentFolder/LastUpdatedOn,CorporateAccountAttachmentFolder/DocumentLink,CorporateAccountAttachmentFolder/TypeCode,CorporateAccountAttachmentFolder/TypeCodeText,CorporateAccountAttachmentFolder/MimeType,CorporateAccountAttachmentFolder/Binary,CorporateAccountAttachmentFolder/Name&$expand=CorporateAccountAttachmentFolder&$format=json",
        ObjectType.pricereq.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_requestforconditions/RequestForConditionsRootCollection?$filter={filter}&$select=ID,Name,ObjectID,AttachmentAttachmentFolder/ObjectID,AttachmentAttachmentFolder/SizeInkB,AttachmentAttachmentFolder/LastUpdatedBy,AttachmentAttachmentFolder/LastUpdatedOn,AttachmentAttachmentFolder/DocumentLink,AttachmentAttachmentFolder/TypeCode,AttachmentAttachmentFolder/TypeCodeText,AttachmentAttachmentFolder/MimeType,AttachmentAttachmentFolder/Binary,AttachmentAttachmentFolder/Name&$expand=AttachmentAttachmentFolder&$format=json",
        ObjectType.techtask.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_requestforconditions/RequestForConditionsRootCollection?$filter={filter}&$select=ID,Name,ObjectID,AttachmentAttachmentFolder/ObjectID,AttachmentAttachmentFolder/SizeInkB,AttachmentAttachmentFolder/LastUpdatedBy,AttachmentAttachmentFolder/LastUpdatedOn,AttachmentAttachmentFolder/DocumentLink,AttachmentAttachmentFolder/TypeCode,AttachmentAttachmentFolder/TypeCodeText,AttachmentAttachmentFolder/MimeType,AttachmentAttachmentFolder/Binary,AttachmentAttachmentFolder/Name&$expand=AttachmentAttachmentFolder&$format=json",
        ObjectType.activity.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_activity/ActivityCollection?$filter={filter}&$select=ID,SubjectName,TypeCode,ObjectID,ActivityAttachmentFolder/ObjectID,ActivityAttachmentFolder/SizeInkB,ActivityAttachmentFolder/LastUpdatedBy,ActivityAttachmentFolder/LastUpdatedOn,ActivityAttachmentFolder/DocumentLink,ActivityAttachmentFolder/TypeCode,ActivityAttachmentFolder/TypeCodeText,ActivityAttachmentFolder/MimeType,ActivityAttachmentFolder/Binary,ActivityAttachmentFolder/Name&$expand=ActivityAttachmentFolder&$format=json",
        ObjectType.oppty.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/OpportunityCollection?$filter={filter}&$select=ID,Name,ObjectID,ProcessingTypeCode,OpportunityAttachmentFolder/ObjectID,OpportunityAttachmentFolder/LastUpdatedBy,OpportunityAttachmentFolder/LastUpdatedOn,OpportunityAttachmentFolder/DocumentLink,OpportunityAttachmentFolder/TypeCode,OpportunityAttachmentFolder/TypeCodeText,OpportunityAttachmentFolder/MimeType,OpportunityAttachmentFolder/Binary&$expand=OpportunityAttachmentFolder&$format=json",
        ObjectType.charinoopty.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_charinopp/CharacteristicsInOpportunityRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.detspecifhistory.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_detspecifhistory/DetailedSpecificationHistoryRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.specifhistory.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_specifhistory/SpecificationHistoryRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.discountinoopty.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_discountinopp/DiscountInOpportunityRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.expertadvice.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_expertadvice/ExpertAdviceParticipationDecisionPlanCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.pilotbatch.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_pilotbatch/PilotBatchOrderForProductionOfPilotBatchCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
        ObjectType.specpaymentterms.name: "https://my336064.crm.ondemand.com/sap/c4c/odata/cust/v1/att_specpaymentterms/SpecialPaymentTermsRootCollection?$filter={filter}&$select=ObjectID,OpportunityID,AttachmentID",
    }
}


def form_filter(object_type, keys):
    filter_con = None
    result_keys = keys.copy()
    if object_type.name == ObjectType.account.name or object_type.name == ObjectType.activity.name or \
            object_type.name == ObjectType.oppty.name or object_type.name == ObjectType.techtask.name:
        for i, key in enumerate(keys):
            key = f"ObjectID eq '{key}'"
            result_keys[i] = key
        filter_con = ' or '.join(result_keys)
    if object_type.name == ObjectType.attachment.name:
        for i, key in enumerate(keys):
            key = f"AttachmentID eq '{key}'"
            result_keys[i] = key
        filter_con = ' or '.join(result_keys)
    if object_type.name == ObjectType.charinoopty.name or object_type.name == ObjectType.detspecifhistory.name or object_type.name == ObjectType.specifhistory.name or \
            object_type.name == ObjectType.discountinoopty.name or object_type.name == ObjectType.expertadvice.name  or object_type.name == ObjectType.pilotbatch.name or object_type.name == ObjectType.specpaymentterms.name:
        for i, key in enumerate(keys):
            key = f"OpportunityID eq '{key}'"
            result_keys[i] = key
        filter_con = ' or '.join(result_keys)
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
