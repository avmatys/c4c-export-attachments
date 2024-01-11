import base64
from datetime import datetime, date

import requests
import c4c
import file_utils
from main import ObjectType, FolderType


def mapping_line(account, attachment, att_path):
    if attachment is None or account is None or "Name" not in attachment:
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
    uuid = account.get("ObjectID", "")
    id = account.get("AccountID", "")
    name = account.get("Name", "")
    line = f"{att_path};{att_name};Account;{uuid};{id};{name};{att_uuid};{att_size};{att_creator};{att_creation_date_form};{att_link};{att_mime};{att_type};{att_type_text}"
    return line


def download_attachments(keys_path="/", file_folder="/", mapping_path="/", error_path="/", log_path="/", package=10):
    keys = []
    key_lines = []
    key_data_map = {}
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
            key_data_map[id] = {"line": line, "ID": id, "Name": name, "ObjectID": uuid}
            key_lines.append(line)
            # Check if package should be processed
            if len(keys) == package or counter == 0:
                # Read data from C4C
                data = c4c.get_data(keys, ObjectType.account, c4c_client)
                # Some error during read - store into the error area
                if data is None:
                    for key_line in key_lines:
                        file_utils.write_to_file(error_path, f"{key_line}, Check logs in c4c/api/logging.log")
                    continue
                # Iterate through data
                for item in data:
                    # Get line data
                    item_id = item.get("AccountID", "")
                    key_data = key_data_map.get(item_id, {})
                    key_line = key_data.get("line", "")
                    # Check if attachments exists
                    if "CorporateAccountAttachmentFolder" not in item or len(
                            item["CorporateAccountAttachmentFolder"]) == 0:
                        file_utils.write_to_file(log_path, f"{key_line}; 0")
                        continue
                    # Save data into the file
                    atts = item["CorporateAccountAttachmentFolder"]
                    for att in atts:
                        file_content = att.get('Binary', None)
                        filename = att.get('Name', None)
                        mime_code = att.get('MimeType', None)
                        if file_content is None or filename is None:
                            file_utils.write_to_file(error_path, f"{key_line}, Binary is not available")
                            continue
                        att_name = f"{item_id}_{filename}"
                        att_path = f"{file_folder}/{att_name}"
                        binary = base64.b64decode(file_content)
                        # Skip links (urls)
                        if mime_code is not None and len(mime_code) > 0:
                            with open(att_path, 'wb') as f:
                                f.write(binary)
                        # Prepare mapping
                        map_line = mapping_line(item, att, att_name)
                        file_utils.write_to_file(mapping_path, f"{map_line}")
                    # Save number of atts for customer
                    file_utils.write_to_file(log_path, f"{key_line}; {len(atts)}")
                # Clear keys
                keys.clear()
                key_data_map.clear()
                key_lines.clear()


if __name__ == '__main__':
    debug = 1

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
