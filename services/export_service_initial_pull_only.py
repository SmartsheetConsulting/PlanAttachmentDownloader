import helpers.smar_helper as smar_helper
import logging
import datetime
import os
import errno
import json
import requests
import time
import re
import config.config as config


class ExportServiceInitialPullOnly:

    def __init__(self):
        self.logger = self._setup_logging()
    
    @staticmethod
    def _setup_logging(log_folder='logs'):

        logging.getLogger('smartsheet').setLevel(logging.ERROR)  # limit smartsheet SDK logging to errors only

        # Create a 'log' folder is none exists
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # ex. 12-11-2023_11-49-11_info.log
        info_log_file_name = f"{datetime.datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}_info.log"
        info_log_file_path = os.path.join(log_folder, info_log_file_name)
        info_file_handler = logging.FileHandler(info_log_file_path)
        info_file_handler.setFormatter(formatter)
        info_file_handler.setLevel(logging.INFO)
        logger.addHandler(info_file_handler)

        # ex. 12-11-2023_11-49-11_errors.log
        error_log_file_name = f"{datetime.datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}_errors.log"
        error_log_file_path = os.path.join(log_folder, error_log_file_name)
        error_file_handler = logging.FileHandler(error_log_file_path)
        error_file_handler.setFormatter(formatter)
        error_file_handler.setLevel(logging.ERROR)
        logger.addHandler(error_file_handler)

        return logger

    def download_attachments(self, delete_attachments=False):
        self.logger.info("Beginning initial data pull")

        user_list = []
        sheets_list = []

        try:
            self.logger.info("Getting all users in org")
            print("Getting all users in org")

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {config.SMARTSHEET_ACCESS_TOKEN}"
            }

            try:
                page = 1
                total_pages = 1
                while page <= total_pages:
                    try:
                        url = f"https://api.smartsheet.com/2.0/users?page={page}&pageSize=10"
                        response = requests.request("GET", url, headers=headers, data="", verify=False)
                        json_response = response.json()
                        total_pages = json_response['totalPages']
                        total_pages = 1

                        user_list.extend(json_response['data'])
                        page = page + 1
                    except Exception as e:
                        print(f"{e}")
                        self.logger.error(f"{e}")
            except Exception as e:
                self.logger.error(f"There was a problem retrieving all users: {e}")
                print(f"There was a problem retrieving all users: {e}")
                return

            for idx, user in enumerate(user_list):
                try:
                    self.logger.info(f"Processing user {idx + 1} of {len(user_list)}")

                    headers['Assume-User'] = user['email']

                    page = 1
                    total_pages = 1
                    while page <= total_pages:
                        try:
                            url = f"https://api.smartsheet.com/2.0/sheets?page={page}&pageSize=1000"
                            response = requests.request("GET", url, headers=headers, data="", verify=False)
                            json_response = response.json()
                            if 'errorCode' in json_response and (json_response['errorCode'] == 5349 or json_response['errorCode'] == 1030):
                                self.logger.error(f"User {user['email']} cannot be assumed. Error Code: {json_response['errorCode']}")
                                break

                            total_pages = json_response['totalPages']

                            sheets = json_response['data']
                            for sheet in sheets:
                                if sheet['accessLevel'] == 'OWNER':
                                    sheet['owner_email'] = user['email']
                                    sheets_list.append(sheet)
                            page = page + 1
                        except Exception as e:
                            self.logger.error(f"{e}")
                except Exception as e:
                    self.logger.error(f"There was a problem retrieving all org sheets: {e}")
                    print(f"There was a problem retrieving all org sheets: {e}")
                    return

            self.logger.info(f"{len(sheets_list)} sheets to process")
            print(f"{len(sheets_list)} sheets to process")

            parent_path = os.path.join(os.path.curdir, 'smartsheet_attachments')
            if not os.path.exists(parent_path):
                os.mkdir(parent_path)


            attachment_manifest_path = os.path.join(parent_path, 'all_attachments.txt')
            if not os.path.exists(attachment_manifest_path):
                open(attachment_manifest_path, 'w')

            attachment_manifest_file_content = open(attachment_manifest_path, 'r').read()

            for idx, sheet in enumerate(sheets_list):
                self.logger.info(f"Processing sheet {idx + 1} of {len(sheets_list)}: sheetName={sheet['name']}, sheetId={sheet['id']}, ownerEmail={sheet['owner_email']}")
                print(f"Processing sheet {idx + 1} of {len(sheets_list)}: : sheetName={sheet['name']}, sheetId={sheet['id']}, ownerEmail={sheet['owner_email']}")

                sheet_folder_name = f"{sheet['id']} - {sheet['owner_email']} - {sheet['name']}"
                sheet_folder_name = self.replace_symbol(sheet_folder_name)
                sheet_folder_path = os.path.join(parent_path, sheet_folder_name)

                # Process any attachments
                try:
                    self.logger.info(f"Processing attachments for sheet '{sheet['name']}' (sheetId: {sheet['id']})")
                    attachments = smar_helper.list_attachments(config.SMARTSHEET_ACCESS_TOKEN, sheet['id'], sheet['owner_email'])
                    json_attachments = json.loads(attachments.to_json())
                    if 'data' in json_attachments and len(json_attachments['data']) > 0:
                        # Create a folder for the sheet being processed
                        try:
                            self.logger.info(f"Creating folder for sheet '{sheet['name']}' (sheetId: {sheet['id']})")
                            sheet_folder_exists = os.path.exists(sheet_folder_path)
                            if sheet_folder_exists:
                                self.logger.info(f"Folder for sheet '{sheet['name']}' (sheetId: {sheet['id']}) already exists")
                            else:
                                os.mkdir(sheet_folder_path)
                        except Exception as e:
                            print(f"There was a problem creating a sheet folder for sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}")
                            self.logger.error(
                                f"There was a problem creating a sheet folder for sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}")
                            continue

                        attachment_folder_path = os.path.join(sheet_folder_path, 'attachments')
                        if not os.path.exists(attachment_folder_path):
                            os.mkdir(attachment_folder_path)

                        if not os.path.exists(attachment_folder_path) or not self.test_filepath_validity(attachment_folder_path):
                            time.sleep(0.500)  # Is it a race condition? Let's wait a bit and check again
                            if not os.path.exists(attachment_folder_path):
                                os.mkdir(attachment_folder_path)

                        attachment_list_file_path = os.path.join(sheet_folder_path, 'attachments.txt')
                        if not os.path.exists(attachment_list_file_path):
                            open(attachment_list_file_path, 'w')

                        for file in json_attachments['data']:
                            if file['attachmentType'] == 'FILE':
                                print(f"Processing attachment {file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']})")
                                self.logger.info(f"Processing attachment {file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']})")
                                try:
                                    attachment_details = smar_helper.get_attachment(config.SMARTSHEET_ACCESS_TOKEN, sheet['id'], file['id'], sheet['owner_email'])
                                    if 'createdAt' in file:
                                        file_created_at = file['createdAt']
                                    else:
                                        file_created_at = None

                                    if 'createdBy' in file and 'name' in file['createdBy']:
                                        file_created_by_name = file['createdBy']['name']
                                    else:
                                        file_created_by_name = None

                                    if 'createdBy' in file and 'email' in file['createdBy']:
                                        file_created_by_email = file['createdBy']['email']
                                    else:
                                        file_created_by_email = None

                                    if 'id' in file:
                                        file_id = file['id']
                                    else:
                                        file_id = None

                                    if 'name' in file:
                                        file_name = file['name']
                                    else:
                                        file_name = None

                                    file_description_text = f"{file_name} - {file_created_by_name} - {file_created_by_email} - {file_created_at} - {file_id}\n"
                                    file_description_text = self.replace_symbol(file_description_text)
                                    attachment_file_update = open(attachment_list_file_path, "r+") # open with read/write access, starting at beginning
                                    content = attachment_file_update.read()
                                    if file_description_text not in content or file_description_text not in attachment_manifest_file_content: # check if attachment is already logged; if not, download it and log it
                                        adjusted_file_name = self.replace_symbol(file_name)
                                        attachment_download_filepath = os.path.join(attachment_folder_path, adjusted_file_name)
                                        if not self.test_filepath_validity(attachment_download_filepath):
                                            file_extension = os.path.splitext(adjusted_file_name)[1]
                                            adjusted_file_name = f"{file_id}{file_extension}"
                                            self.logger.warning(f"Adjusting filename '{file_name}' to '{adjusted_file_name}' due to invalid characters")

                                        smar_helper.download_attachment(config.SMARTSHEET_ACCESS_TOKEN, attachment_details, attachment_folder_path, sheet['owner_email'], adjusted_file_name)
                                        attachment_file_update.write(file_description_text)

                                        attachment_manifest_file_update = open(attachment_manifest_path, 'a')
                                        attachment_manifest_file_update.write(file_description_text)
                                        attachment_manifest_file_update.close()

                                    attachment_file_update.close()
                                    if delete_attachments:
                                        smar_helper.delete_attachment(config.SMARTSHEET_ACCESS_TOKEN,
                                                                      file_id, sheet['id'], sheet['owner_email'])
                                except Exception as e:
                                    print(f"There was a problem downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}")
                                    self.logger.error(f"There was a problem downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}")
                                    continue
                    else:
                        self.logger.info(f"There are no attachments for sheet '{sheet['name']}' (sheetId: {sheet['id']})")
                except Exception as e:
                    self.logger.error(f"There was a problem processing attachments from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}")
                    continue
        except Exception as e:
            self.logger.error(f"There was an error in the process: {e}")
            print(f"There was an error in the process: {e}")

    def replace_symbol(self, filepath):
        re.sub(r'[^\x00-\x7f]', r' ', filepath)
        filepath = re.sub(r'[^\x00-\x7f]', r' ', filepath)
        for symbol in ['<',
                       '>',
                       ':',
                       '"',
                       '/',
                       '\\',
                       '|',
                       '?',
                       '*',
                       '\u00a0',
                       '\u1680',
                       '\u180e',
                       '\u2000',
                       '\u2001',
                       '\u2002',
                       '\u2003',
                       '\u2004',
                       '\u2005',
                       '\u2006',
                       '\u2007',
                       '\u2008',
                       '\u2009',
                       '\u200a',
                       '\u200b',
                       '\u200c',
                       '\u200d',
                       '\u200e',
                       '\u200f',
                       '\u2028',
                       '\u2029',
                       '\u202a',
                       '\u202b',
                       '\u202c',
                       '\u202d',
                       '\u202e',
                       '\u202f',
                       '\u205f',
                       '\u2060',
                       '\u3000',
                       '\ufeff',
                       '\x00']:
            if symbol in filepath:
                filepath = filepath.replace(symbol, '_')
        return filepath

    def test_filepath_validity(self, filepath):
        try:
            os.lstat(filepath)
        except OSError as exc:
            if hasattr(exc, 'winerror'):
                if exc.winerror == 123:  # Windows-specific error code indicating an invalid pathname.
                    print(f"Invalid pathname: {filepath}: {exc}")
                    self.logger.warning(f"Invalid pathname: {filepath}: {exc}")
                    return False
            elif exc.errno in {errno.ENAMETOOLONG, errno.ERANGE}:
                print(f"Invalid pathname: {filepath}: {exc}")
                self.logger.warning(f"Invalid pathname: {filepath}: {exc}")
                return False

        return True
