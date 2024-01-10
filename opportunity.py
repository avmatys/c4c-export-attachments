import base64
from datetime import datetime

import requests
import c4c
import file_utils
from main import ObjectType

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
    # att_size = attachment.get("SizeInkB", "")
    att_creation_date = attachment.get("LastUpdatedOn", "")
    # Get timestamp
    timestamp = int(att_creation_date.replace("/Date(", "").replace(")/", ""))
    # Calculate standard date format (year-month-day hours:minutes:seconds) for UTC+0
    att_creation_date_form = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    att_creator = attachment.get("LastUpdatedBy", "")
    att_link = attachment.get("DocumentLink", "")
    att_mime = attachment.get("MimeType", "")
    att_type = attachment.get("TypeCode", "")
    # att_type_text = attachment.get("TypeCodeText", "")
    uuid = object.get("ObjectID", "")
    id = ""
    name = ""
    type_code = ""
    type_desc = ""
    if object_type_name == 'oppty':
        id = object.get("ID", "")
        name = object.get("Name", "")
        type_code = object.get("ProcessingTypeCode", "")
        type_desc = object.get("ProcessingTypeCodeText", "")
    parameter = object_type_name
    line = f"{att_path};{att_name};{uuid};{id};{name};{type_code};{type_desc};{att_uuid};{att_creator};{att_creation_date_form};{att_link};{att_mime};{att_type}"
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
            key_data_map[id] = {"line": line, "ID": id, "Name": name, "ObjectID": uuid}
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
                    # Check if attachments exists
                    if "OpportunityAttachmentFolder" not in item or len(
                            item["OpportunityAttachmentFolder"]) == 0:
                        file_utils.write_to_file(log_path, f"{line}; Oppty; 0")
                        continue
                    # Save data into the file
                    atts = item["OpportunityAttachmentFolder"]
                    item_id = item.get("ID", None)
                    key_data = key_data_map.get(item_id, {})
                    key_line = key_data.get("line", "")
                    for att in atts:
                        file_content = att.get('Binary', None)
                        filename = att.get('Name', None)
                        mime_code = att.get('MimeType', None)
                        if file_content is None or filename is None:
                            file_utils.write_to_file(error_path, f"{key_line}; Oppty; Binary is not available")
                            continue
                        att_name = f"{item_id}_oppty_{filename}"
                        att_path = f"{file_folder}/{att_name}"
                        binary = base64.b64decode(file_content)
                        # Skip links (urls)
                        if mime_code is not None and len(mime_code) > 0:
                            with open(att_path, 'wb') as f:
                                f.write(binary)
                        # Prepare mapping
                        line = mapping_line(item, att, att_name, ObjectType.oppty.name)
                        file_utils.write_to_file(mapping_path, f"{key_line}")
                    # Save number of atts for customer
                    file_utils.write_to_file(log_path, f"{key_line}; Oppty; {len(atts)}")

                # Clear collections
                keys.clear()
                key_data_map.clear()
                key_lines.clear()

            counter = len(lines)
            alllines = counter

            # Iterate by oppty
            for i, line in enumerate(lines):

                # Prepare line
                line = line.rstrip()

                # Skip header
                if i == 0:
                    continue

                # Get oppty id
                splitted_line = line.split(";")
                oppty_id = []
                item_oppty_id = ''
                if len(splitted_line) > 1:
                    oppty_id.append(splitted_line[1])
                    item_oppty_id = splitted_line[1]

                # Get attachment ids for oppty id
                charinoopty_data = c4c.get_data(oppty_id, ObjectType.charinoopty, c4c_client)
                detspecifhistory_data = c4c.get_data(oppty_id, ObjectType.detspecifhistory, c4c_client)
                specifhistory_data = c4c.get_data(oppty_id, ObjectType.specifhistory, c4c_client)
                discountinoopty_data = c4c.get_data(oppty_id, ObjectType.discountinoopty, c4c_client)
                expertadvice_data = c4c.get_data(oppty_id, ObjectType.expertadvice, c4c_client)
                pilotbatch_data = c4c.get_data(oppty_id, ObjectType.pilotbatch, c4c_client)
                specpaymentterms_data = c4c.get_data(oppty_id, ObjectType.specpaymentterms, c4c_client)

                data_of_all_objects = {ObjectType.charinoopty: charinoopty_data,
                                       ObjectType.specifhistory: specifhistory_data,
                                       ObjectType.discountinoopty: discountinoopty_data,
                                       ObjectType.expertadvice: expertadvice_data,
                                       ObjectType.pilotbatch: pilotbatch_data,
                                       ObjectType.specpaymentterms: specpaymentterms_data,
                                       ObjectType.detspecifhistory: detspecifhistory_data}

                # Store attachment ids into one collection of key
                key_att_id = []
                key_data_map = {}
                for key, value in data_of_all_objects.items():
                    for item in value:
                        attachment_id = item.get('AttachmentID', None)
                        key_att_id.append(attachment_id)
                        key_data_map[attachment_id] = {"ObjectType" : PARAMETERS.get(key.name, ""), "ID" : item_oppty_id, "line" : line}
                # Get attachment data by attachment keys
                attchments_data = c4c.get_data(key_att_id, ObjectType.attachment, c4c_client)

                if attchments_data is None:
                    for key_line in key_lines:
                        file_utils.write_to_file(error_path, f"{line}, Check logs in c4c/api/logging.log")
                    continue

                # Process response
                for item in attchments_data:

                    att_id = item.get('AttachmentID', None)
                    key_data = key_data_map.get(att_id, {})
                    key_line = key_data.get("line", "")
                    objectType = key_data.get("ObjectType","")
                    oppt_id = key_data_map.get("ID","")

                    # Check if attachments exists
                    if "AttachmentAttachmentFolder" not in item or len(
                            item["AttachmentAttachmentFolder"]) == 0:
                        file_utils.write_to_file(log_path, f"{line};{objectType}; 0")
                        continue
                    # Save data into the file
                    atts = item["AttachmentAttachmentFolder"]
                    for att in atts:
                        file_content = att.get('Binary', None)
                        filename = att.get('Name', None)
                        mime_code = att.get('MimeType', None)
                        if file_content is None or filename is None:
                            file_utils.write_to_file(error_path, f"{key_line};{objectType}; Binary is not available")
                            continue
                        att_name = f"{oppt_id}_{objectType}_{filename}"
                        parameter = PARAMETERS.get(key.name, "")
                        att_path = f"{file_folder}/{att_name}"
                        binary = base64.b64decode(file_content)
                        # Skip links (urls)
                        if mime_code is not None and len(mime_code) > 0:
                            with open(att_path, 'wb') as f:
                                f.write(binary)
                        # Prepare mapping
                        map_line = mapping_line(item, att, att_name, objectType)
                        file_utils.write_to_file(mapping_path, f"{map_line}")
                    # Save number of atts for customer
                    file_utils.write_to_file(log_path, f"{key_line};{objectType};{len(atts)}")


if __name__ == '__main__':
    debug = 1

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
