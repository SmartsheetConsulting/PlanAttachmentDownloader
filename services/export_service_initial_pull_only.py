import helpers.smar_helper as smar_helper
import logging
import datetime
import os
import json
import requests
import config.config as config


class ExportServiceInitialPullOnly:

    def __init__(self):
        self.logger = self._setup_logging()
    
    @staticmethod
    def _setup_logging(log_folder='logs'):

        logging.getLogger('smartsheet').setLevel(logging.ERROR) # limit smartsheet SDK logging to errors only

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

    def download_attachments(self):
        self.logger.info("Beginning initial data pull")

        sheets_list = []
        # Make list org sheets call using sys admin token
        try:
            self.logger.info("Getting all sheets in org")
            print("Getting all sheets in org")

            try:
                headers = {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'Authorization': f"Bearer {config.SMARTSHEET_ACCESS_TOKEN}"
                }

                # Create map of sheet IDs and owners
                page = 1
                total_pages = 1
                while page <= total_pages:
                    url = f"https://api.smartsheet.com/2.0/users/sheets?page={page}&pageSize=1000"
                    response = requests.request("GET", url, headers=headers, data="", verify=False)
                    json_response = response.json()
                    total_pages = json_response['totalPages']

                    for sheet in json_response['data']:
                        if sheet['owner'] and sheet['ownerId']:
                            sheets_list.append(sheet)
                    page = page + 1
            except Exception as e:
                self.logger.error(f"There was a problem retrieving all org sheets: {e}")
                print(f"There was a problem retrieving all org sheets: {e}")
                return

            owners_to_ignore = []

            self.logger.info(f"{len(sheets_list)} sheets to process")
            print(f"{len(sheets_list)} sheets to process")

            parent_path = os.path.join(os.path.curdir, 'smartsheet_attachments')
            if not os.path.exists(parent_path):
                os.mkdir(parent_path)

            attachment_manifest_path = os.path.join(parent_path, 'all_attachments.txt')
            attachment_manifest_file = open(attachment_manifest_path, 'a+')
            attachment_manifest_file_content = open(attachment_manifest_path, 'r').read()

            for idx, sheet in enumerate(sheets_list):
                self.logger.info(f"Processing sheet {idx + 1} of {len(sheets_list)}")
                print(f"Processing sheet {idx + 1} of {len(sheets_list)}")

                # Ignore sheet owned by admins
                if sheet['owner'].lower() in owners_to_ignore:
                    continue

                sheet_folder_name = f"{sheet['id']} - {sheet['owner']} - {sheet['name']}"
                sheet_folder_name = self.replace_symbol(sheet_folder_name)
                sheet_folder_path = os.path.join(parent_path, sheet_folder_name)

                # Process any attachments
                try:
                    self.logger.info(f"Processing attachments for {sheet['name']} ({sheet['id']})")
                    attachments = smar_helper.list_attachments(config.SMARTSHEET_ACCESS_TOKEN, sheet['id'], sheet['owner'])
                    json_attachments = json.loads(attachments.to_json())
                    if 'data' in json_attachments and len(json_attachments['data']) > 0:
                        # Create a folder for the sheet being processed
                        try:
                            self.logger.info(f"Creating folder for {sheet['name']} ({sheet['id']})")
                            sheet_folder_exists = os.path.exists(sheet_folder_path)
                            if sheet_folder_exists:
                                self.logger.info(f"Folder for {sheet['name']} ({sheet['id']}) already exists")
                                continue
                            else:
                                os.mkdir(sheet_folder_path)
                        except Exception as e:
                            self.logger.error(
                                f"There was a problem creating a sheet folder for {sheet['name']} ({sheet['id']}): {e}")

                        attachment_folder_path = os.path.join(sheet_folder_path, 'attachments')
                        if not os.path.exists(attachment_folder_path):
                            os.mkdir(attachment_folder_path)

                        attachment_file_path = os.path.join(attachment_folder_path, 'attachments.txt')
                        if not os.path.exists(attachment_file_path):
                            open(attachment_file_path, 'w')

                        for file in json_attachments['data']:
                            try:
                                attachment_details = smar_helper.get_attachment(config.SMARTSHEET_ACCESS_TOKEN, sheet['id'], file['id'], sheet['owner'])
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
                                attachment_file_update = open(attachment_file_path, "r+") # open with read/write access, starting at beginning
                                content = attachment_file_update.read()
                                if file_description_text not in content or file_description_text not in attachment_manifest_file_content: # check if attachment is already logged; if not, download it and log it
                                    smar_helper.download_attachment(config.SMARTSHEET_ACCESS_TOKEN, attachment_details, attachment_folder_path, sheet['owner'])
                                    attachment_file_update.write(file_description_text)
                                    attachment_manifest_file.write(file_description_text)
                                attachment_file_update.close()
                            except Exception as e:
                                self.logger.error(f"There was a problem downloading {sheet['name']} ({sheet['id']}): {e}")
                                continue
                    else:
                        self.logger.info(f"There are no attachments for {sheet['name']} ({sheet['id']})")
                except Exception as e:
                    self.logger.error(f"There was a problem processing attachments from {sheet['name']} ({sheet['id']}): {e}")
                    continue
        except Exception as e:
            self.logger.error(f"There was an error in the process: {e}")
            print(f"There was an error in the process: {e}")

    def replace_symbol(self, filepath):
        for symbol in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']:
            if symbol in filepath:
                filepath = filepath.replace(symbol, '_')
        return filepath
