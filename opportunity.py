import base64
from datetime import datetime

import requests
import c4c
import file_utils
from main import ObjectType

TECH_NAMES = {
    ObjectType.charinoopty.name: "сharinoppty",
    ObjectType.detspecifhistory.name: "detspechistory",
    ObjectType.specifhistory.name: " specifhistory",
    ObjectType.discountinoopty.name: "discountinOppty",
    ObjectType.expertadvice.name: "expertadvice",
    ObjectType.pilotbatch.name: "pilotbatch",
    ObjectType.specpaymentterms.name: "specpaymentterms"
}

PARAMETERS = {
    ObjectType.charinoopty.name: "Characteristics in oppty",
    ObjectType.detspecifhistory.name: "Det spec history",
    ObjectType.specifhistory.name: " Specif history",
    ObjectType.discountinoopty.name: "Discount in oppty",
    ObjectType.expertadvice.name: "Expert advice",
    ObjectType.pilotbatch.name: "Pilot batch",
    ObjectType.specpaymentterms.name: "Spec payment terms"
}


def mapping_line(object, attachment, att_path, object_type_name):
    if attachment is None or object is None or "Name" not in attachment:
        return ""
    att_name = attachment.get("Name", "")
    att_uuid = attachment.get("ObjectID", "")
    att_size = attachment.get("SizeInkB", "")
    att_creation_date = attachment.get("LastUpdatedOn", "")
    # Get timestamp
    timestamp = int(att_creation_date.replace("/Date(", "").replace(")/", ""))
    # Calculate standard date format (year-month-day hours:minutes:seconds) for UTC+0
    att_creation_date_form = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    att_creator = attachment.get("LastUpdatedBy", "")
    att_link = attachment.get("DocumentLink", "")
    att_mime = attachment.get("MimeType", "")
    att_type = attachment.get("TypeCode", "")
    att_type_text = attachment.get("TypeCodeText", "")
    uuid = object.get("ObjectID", "")
    id, name, type_code, subobjectuuid = "", "", "", ""
    if object_type_name == "Opportunity":
        id = object.get("ID", "")
        name = object.get("Name", "")
        type_code = object.get("ProcessingTypeCode", "")
    else:
        subobjectuuid = object.get("ObjectID", "")
        id = object.get("OpportunityID", "")
        name = object.get("OpportunityName", "")
        type_code = object.get("OpportunityType", "")
    subobjecttype = object_type_name
    line = f"{att_path};{att_name};Opportunity;{uuid};{id};{name};{type_code};{subobjecttype};{subobjectuuid};{att_uuid};{att_size};{att_creator};{att_creation_date_form};{att_link};{att_mime};{att_type};{att_type_text}"
    return line


def download_attachments(keys_path="/", file_folder="/", mapping_path="/", error_path="/", log_path="/", package=10):
    keys = []
    keys_oppty_id = []
    key_data_map = {}
    key_lines = []
    with open(keys_path, encoding="utf8") as fin:
        c4c_client = requests.session()
        lines = fin.readlines()
        counter = len(lines)
        alllines = counter
        for i, line in enumerate(lines):
            current_time = datetime.now()
            print(f"{current_time} Thread {keys_path} Status {i}/{alllines}")
            # Prepare line
            line = line.rstrip()
            # Iterate one by one
            counter -= 1
            # Skip header
            if i == 0:
                continue
            # Get key
            splitted_line = line.split(";")
            uuid = None
            id = None
            if len(splitted_line) > 2:
                uuid = splitted_line[0].replace("-", "")
                id = splitted_line[1]
                name = splitted_line[2]
            if uuid is None or id is None:
                continue
            # Store in collection
            keys.append(uuid)
            keys_oppty_id.append(id)
            key_data_map[id] = {"line": line, "ID": id, "Name": name, "ObjectID": uuid, "TypeCode": ""}
            key_lines.append(line)
            # Check if package should be processed
            if len(keys) == package or counter == 0:
                # Read data from C4C
                data = c4c.get_data(keys, ObjectType.oppty, c4c_client)
                # Some error during read - store into the error area
                if data is None:
                    for key_line in key_lines:
                        file_utils.write_to_file(error_path, f"{key_line}, Check logs in c4c/api/logging.log")
                    continue
                # Iterate through data
                for item in data:
                    item_id = item.get("ID", None)
                    key_data = key_data_map.get(item_id, {})
                    key_line = key_data.get("line", "")
                    # Add params
                    key_data_map[item_id]["TypeCode"] = item.get("ProcessingTypeCode", "")
                    # Check if attachments exists
                    if "OpportunityAttachmentFolder" not in item or len(
                            item["OpportunityAttachmentFolder"]) == 0:
                        file_utils.write_to_file(log_path, f"{key_line}; oppty; 0")
                        continue
                    # Save data into the file
                    atts = item["OpportunityAttachmentFolder"]
                    for att in atts:
                        file_content = att.get('Binary', None)
                        filename = att.get('Name', None)
                        mime_code = att.get('MimeType', None)
                        if file_content is None or filename is None:
                            file_utils.write_to_file(error_path, f"{key_line}; oppty; Binary is not available")
                            continue
                        att_name = f"{item_id}_oppty_{filename}"
                        att_path = f"{file_folder}/{att_name}"
                        binary = base64.b64decode(file_content)
                        # Skip links (urls)
                        if mime_code is not None and len(mime_code) > 0:
                            with open(att_path, 'wb') as f:
                                f.write(binary)
                        # Prepare mapping
                        map_line = mapping_line(item, att, att_name, "Opportunity")
                        file_utils.write_to_file(mapping_path, f"{map_line}")
                    # Save number of atts for customer
                    file_utils.write_to_file(log_path, f"{key_line}; oppty; {len(atts)}")

                # Clear collections
                keys.clear()
                key_lines.clear()

                # Get attachment ids for oppty id
                charinoopty_data = c4c.get_data(keys_oppty_id, ObjectType.charinoopty, c4c_client)
                detspecifhistory_data = c4c.get_data(keys_oppty_id, ObjectType.detspecifhistory, c4c_client)
                specifhistory_data = c4c.get_data(keys_oppty_id, ObjectType.specifhistory, c4c_client)
                discountinoopty_data = c4c.get_data(keys_oppty_id, ObjectType.discountinoopty, c4c_client)
                expertadvice_data = c4c.get_data(keys_oppty_id, ObjectType.expertadvice, c4c_client)
                pilotbatch_data = c4c.get_data(keys_oppty_id, ObjectType.pilotbatch, c4c_client)
                specpaymentterms_data = c4c.get_data(keys_oppty_id, ObjectType.specpaymentterms, c4c_client)

                data_of_all_objects = {ObjectType.charinoopty: charinoopty_data,
                                       ObjectType.specifhistory: specifhistory_data,
                                       ObjectType.discountinoopty: discountinoopty_data,
                                       ObjectType.expertadvice: expertadvice_data,
                                       ObjectType.pilotbatch: pilotbatch_data,
                                       ObjectType.specpaymentterms: specpaymentterms_data,
                                       ObjectType.detspecifhistory: detspecifhistory_data}

                # Store attachment ids into one collection of key
                key_att_id = []
                key_objects_data_map = {}
                for key, value in data_of_all_objects.items():
                    for item in value:
                        att_subtype = ""
                        if key.name == ObjectType.charinoopty.name:
                            att_subtype = item.get('CharacteristicsDocument', "")
                        if key.name == ObjectType.expertadvice.name:
                            att_subtype = item.get('Task', "")
                        if key.name == ObjectType.specifhistory.name:
                            att_subtype = "sh"
                        if key.name == ObjectType.discountinoopty.name:
                            att_subtype = item.get('RequestForConditionsID', "")
                        if key.name == ObjectType.pilotbatch.name:
                            att_subtype = "pb"
                        if key.name == ObjectType.specpaymentterms.name:
                            att_subtype = item.get('PeymTerms', "")
                        if key.name == ObjectType.detspecifhistory.name:
                            att_subtype = item.get('RequestForConditionsID_RC', "")
                        attachment_id = item.get('AttachmentID', "")
                        opportunity_id = item.get('OpportunityID', "")
                        subobject_uuid = item.get("ObjectID", "")
                        key_data = key_data_map.get(opportunity_id, {})
                        key_line = key_data.get("line", "")
                        opp_type = key_data.get("TypeCode", "")
                        opp_name = key_data.get("Name", "")
                        key_att_id.append(attachment_id)
                        key_objects_data_map[attachment_id] = {"Parameter": PARAMETERS.get(key.name, ""),
                                                               "TechName": TECH_NAMES.get(key.name, ""),
                                                               "OpportunityID": opportunity_id,
                                                               "OpportunityName": opp_name,
                                                               "OpportunityType": opp_type, "ObjectID": subobject_uuid,
                                                               "AttSubType": att_subtype, "line": key_line}

                # Get max lenght
                att_count = len(key_att_id)
                ATT_PACKAGE = 2 * package
                start = 0
                end = ATT_PACKAGE

                while 1:
                    print(f"Start {start} End {end} All {att_count}")
                    if start >= att_count:
                        break
                    if end > att_count:
                        end = att_count + 1
                    # Get keys
                    sliced_keys = key_att_id[start:end]
                    start = end
                    end += ATT_PACKAGE

                    # Get attachment data by attachment keys
                    attchments_data = c4c.get_data(sliced_keys, ObjectType.attachment, c4c_client)

                    if attchments_data is None:
                        for key_line_id in sliced_keys:
                            key_opp_line = key_objects_data_map.get(key_line_id, {})
                            err_line = key_opp_line.get("line", "")
                            file_utils.write_to_file(error_path, f"{err_line}, Check logs in c4c/api/logging.log")
                        continue

                    # Process response
                    for item in attchments_data:
                        att_id = item.get('AttachmentID', "")
                        key_data = key_objects_data_map.get(att_id, {})
                        key_line = key_data.get("line", "")
                        parameter = key_data.get("Parameter", "")
                        tech_name = key_data.get("TechName", "")
                        att_type_code = key_data.get("AttSubType", "")
                        oppt_id = key_data.get("OpportunityID", "")
                        # Check if attachments exists
                        if "AttachmentAttachmentFolder" not in item or len(
                                item["AttachmentAttachmentFolder"]) == 0:
                            file_utils.write_to_file(log_path, f"{line};{parameter}; 0")
                            continue
                        # Save data into the file
                        atts = item["AttachmentAttachmentFolder"]
                        for att in atts:
                            file_content = att.get('Binary', None)
                            filename = att.get('Name', None)
                            mime_code = att.get('MimeType', None)
                            if file_content is None or filename is None:
                                file_utils.write_to_file(error_path, f"{key_line};{parameter}; Binary is not available")
                                continue
                            att_name = f"{oppt_id}_{tech_name}_{att_type_code}_{filename}"
                            att_path = f"{file_folder}/{att_name}"
                            binary = base64.b64decode(file_content)
                            # Skip links (urls)
                            if mime_code is not None and len(mime_code) > 0:
                                with open(att_path, 'wb') as f:
                                    f.write(binary)
                            # Prepare mapping
                            map_line = mapping_line(key_data, att, att_name, parameter)
                            file_utils.write_to_file(mapping_path, f"{map_line}")
                        # Save number of atts for customer
                        file_utils.write_to_file(log_path, f"{key_line};{parameter};{len(atts)}")

                # Clear
                keys_oppty_id.clear()
                key_att_id.clear()
                key_objects_data_map.clear()

        key_data_map.clear()


if __name__ == '__main__':
    debug = 1

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
