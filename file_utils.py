import pathlib
import os
import math
from main import MODE, Mode, ObjectType, FolderType
from logger import *

# first file logger
logging = setup_logger('c4cfile', 'c4c/api/file.log')

#logging.basicConfig(level=logging.ERROR, filename="c4c/api/file.log", filemode="a",
#                    format="%(asctime)s;%(levelname)s;%(message)s;")

PATHS = {
    Mode.test.name: {
        ObjectType.account.name: {
            FolderType.input_raw.name: "account/input/raw",
            FolderType.input_struct.name: "account/input/struct",
            FolderType.output_error.name: "account/output/error",
            FolderType.output_log.name: "account/output/log",
            FolderType.output_mapping.name: "account/output/mapping",
            FolderType.output_file.name: "account/output/file",
        },
        ObjectType.activity.name: {
            FolderType.input_raw.name: "activity/input/raw",
            FolderType.input_struct.name: "activity/input/struct",
            FolderType.output_error.name: "activity/output/error",
            FolderType.output_log.name: "activity/output/log",
            FolderType.output_mapping.name: "activity/output/mapping",
            FolderType.output_file.name: "activity/output/file",
        }
    },
    Mode.prod.name: {
        ObjectType.account.name: {
            FolderType.input_raw.name: "account/input/raw",
            FolderType.input_struct.name: "account/input/struct",
            FolderType.output_error.name: "account/output/error",
            FolderType.output_log.name: "account/output/log",
            FolderType.output_mapping.name: "account/output/mapping",
            FolderType.output_file.name: "account/output/file",
        },
        ObjectType.activity.name: {
            FolderType.input_raw.name: "activity/input/raw",
            FolderType.input_struct.name: "activity/input/struct",
            FolderType.output_error.name: "activity/output/error",
            FolderType.output_log.name: "activity/output/log",
            FolderType.output_mapping.name: "activity/output/mapping",
            FolderType.output_file.name: "activity/output/file",
        }
    }
}


def get_path(object_type, folder_type):
    object_type = object_type.name
    folder_type = folder_type.name
    if object_type is None :
        logging.error(f"GET_PATH;Object type is not set")
        return None
    if folder_type is None:
        logging.error(f"GET_PATH;Folder type is not set")
        return None
    paths = PATHS.get(MODE.name, None)
    if paths is None:
        logging.error(f"GET_PATH;Paths are not set for {object_type.name} {folder_type.name}")
        return None
    folders = paths.get(object_type, None)
    if folders is None:
        logging.error(f"GET_PATH;{object_type.name} is not in paths")
        return None
    path = folders.get(folder_type,None)
    if path is None:
        logging.error(f"GET_PATH;{folder_type.name} is not in paths for {object_type.name}")
        return None
    return path


def split_file(input_path, output_folder, output_prefix, lines_number=1000, header=True):
    if input_path is None or len(input_path) == 0:
        logging.error(f"SPLIT_FILE;Input path is not specified or not correct {input_path}")
        return
    with open(input_path, encoding="utf8") as fin:
        file_type = pathlib.Path(input_path).suffix
        filename = f"{output_folder}/{output_prefix}0{file_type}"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        fout = None
        try:
            fout = open(filename, "w", encoding="utf8")
        except IOError:
            logging.error(f"SPLIT_FILE;Can't generate output file {filename}. Can't process {input_path}")
        if fout is None:
            return
        header_line = ""
        for i, line in enumerate(fin):
            if i == 0:
                header_line = line
            fout.write(line)
            if (i + 1) % lines_number == 0:
                fout.close()
                index = math.ceil(i / lines_number + 1)
                filename = f"{output_folder}/{output_prefix}{index}{file_type}"
                try:
                    fout = open(filename, "w", encoding="utf8")
                except IOError:
                    logging.error(f"SPLIT_FILE;Can't generate output file {filename}. Can't process {input_path}")
                if fout is None:
                    return
                if header:
                    fout.write(header_line)
        fout.close()


def get_files(path):
    f = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        f.extend(filenames)
        break
    return f


def clear_folder(object_type, folder_type):
    path = get_path(object_type, folder_type)
    if path is None:
        return
    files = get_files(path)
    for f in files:
        delete_file(f"{path}/{f}")


def delete_file(path):
    try:
        if os.path.isfile(path) or os.path.islink(path):
            os.unlink(path)
    except Exception as e:
        logging.error(f"DELETE_FILE;Can't delete file {path}. Reason {e}")

def split_files_in_folder(object_type, folder_input, folder_output, package=1000):
    path = get_path(object_type, folder_input)
    files = get_files(path)
    output_path = get_path(object_type, folder_output)
    if path is None:
        print(f"Path {path} doesn't exist")
        return
    for f in files:
        input_path = f"{path}/{f}"
        file_prefix = pathlib.Path(input_path).stem
        split_file(input_path, output_path, file_prefix, package)


def merging_mapping_files(object_type, folder_input, folder_output):
    # Set table headers for each types
    first_line = None
    if object_type.name is ObjectType.account.name:
        first_line = "AttachmentPath;AttachmentName;ObjectType;AccountUUID;AccountID;AccountName;AttachmentUUID;SizeInkB;AttachmentCreator;AttachmentCreationDate(UTC+0);DocumentLink;MimeType;TypeCode;TypeCodeText"
    if object_type.name is ObjectType.activity.name:
        first_line = "AttachmentPath;AttachmentName;ActivityUUID;ActivityID;ActivityName;ActivityTypeCode;AttachmentUUID;AttachmentCreator;AttachmentCreationDate(UTC+0);DocumentLink;MimeType;TypeCode"

    path = get_path(object_type, folder_input)
    files = get_files(path)
    output_path = get_path(object_type, folder_output)
    output_filename = "resultMappingFile.csv"
    output_file= f"{output_path}/{output_filename}"
    # Set headers
    if first_line is not None and first_line != "":
        write_to_file(output_file, f"{first_line}")
    if path is None:
        print(f"Path {path} doesn't exist")
        return
    for f in files:
        input_path = f"{path}/{f}"
        with open(input_path, encoding="utf8") as fin:
            lines = fin.readlines()
            for i, line in enumerate(lines):
                write_to_file(output_file, f"{line}",False)

def write_to_file(file, data,escapeSequence = True):
    with open(file, 'a', newline='\n', encoding='utf-8') as file:
        if escapeSequence:
            file.write("\n")
        file.write(data)