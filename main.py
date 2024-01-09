from datetime import datetime, date
import enum
import threading

class ObjectType(enum.Enum):
    account = 1
    activity = 2
    oppty = 3
    techtask = 4
    pricereq = 5


class Mode(enum.Enum):
    test = 0
    prod = 1


class FolderType(enum.Enum):
    input_raw = 1
    input_struct = 2
    output_error = 3
    output_log = 4
    output_mapping = 5
    output_file = 6


# Enable test mode
MODE = Mode.test

import file_utils
import account
import activity


# Split big files in small pieces
def preprocess_input(object_type, package=10000):
    file_utils.clear_folder(object_type, FolderType.input_struct)
    file_utils.split_files_in_folder(object_type, FolderType.input_raw, FolderType.input_struct, package)


# Download
def download_attachments(object_type, module):
    # Prepare threads
    threads = []
    # Get all files from dir
    input_path = file_utils.get_path(object_type, FolderType.input_struct)
    files = file_utils.get_files(input_path)
    # Get all output folders
    error_path = file_utils.get_path(object_type, FolderType.output_error)
    log_path = file_utils.get_path(object_type, FolderType.output_log)
    mapping_path = file_utils.get_path(object_type, FolderType.output_mapping)
    file_path = file_utils.get_path(object_type, FolderType.output_file)
    for f in files:
        # creating threads
        start_time = datetime.now()
        print(f"{start_time} Start thread for {object_type.name} - {f}")

        thread = threading.Thread(target=module.download_attachments, name=f,
                                  args=(f"{input_path}/{f}", file_path, f"{mapping_path}/{f}", f"{error_path}/{f}",
                                        f"{log_path}/{f}", 10))
        threads.append(thread)
        thread.start()

        module.download_attachments(f"{input_path}/{f}", file_path, f"{mapping_path}/{f}", f"{error_path}/{f}",
                                    f"{log_path}/{f}", 10)
    # Attach threads to curent
    for thread in threads:
        thread.join()

    # Set table headers for each types
    if object_type is ObjectType.account:
        first_line = "AttachmentPath;AttachmentName;ObjectType;AccountUUID;AccountID;AccountName;AttachmentUUID;SizeInkB;AttachmentCreator;AttachmentCreationDate(UTC+0);DocumentLink;MimeType;TypeCode;TypeCodeText"
    if object_type is ObjectType.activity:
        first_line = "AttachmentPath;AttachmentName;ActivityUUID;ActivityID;ActivityName;ActivityTypeCode;AttachmentUUID;AttachmentCreator;AttachmentCreationDate(UTC+0);DocumentLink;MimeType;TypeCode"
    file_utils.merging_files_in_folder(object_type, FolderType.output_mapping, FolderType.output_file,first_line)

def print_menu():
    print('1.  Preprocess Accounts')
    print('2.  Preprocess Activities')
    print('3.  Preprocess Opportunities')
    print('4.  Preprocess Tech Tasks')
    print('5.  Preprocess Price Requests')
    print('6.  Download Accounts')
    print('7.  Download Activities')
    print('8.  Download Opportunities')
    print('9.  Download Tech Tasks')
    print('10. Download Price Requests')
    print('11. Stop')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    while 1:
        # Show options
        print_menu()
        # Read input
        try:
            mode = int(input('Input:'))
        except ValueError:
            print("Not a number")
        # Execute logic
        if mode == 1:
            preprocess_input(ObjectType.account, 10)
        if mode == 2:
            preprocess_input(ObjectType.activity, 10)
        if mode == 3:
            preprocess_input(ObjectType.oppty, 10)
        if mode == 4:
            preprocess_input(ObjectType.techtask, 10)
        if mode == 5:
            preprocess_input(ObjectType.pricereq, 10)
        if mode == 6:
            download_attachments(ObjectType.account, account)
        if mode == 7:
            download_attachments(ObjectType.activity, activity)
        if mode == 11:
            break

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
