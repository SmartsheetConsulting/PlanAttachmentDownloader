import helpers.smar_helper as smar_helper
import logging
import datetime
import os
import errno
import json
import requests
import time
import re
import csv
import pathvalidate as pathvalidate
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
        info_file_handler = logging.FileHandler(info_log_file_path, encoding='iso8859-1')
        info_file_handler.setFormatter(formatter)
        info_file_handler.setLevel(logging.INFO)
        logger.addHandler(info_file_handler)

        # ex. 12-11-2023_11-49-11_errors.log
        error_log_file_name = f"{datetime.datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}_errors.log"
        error_log_file_path = os.path.join(log_folder, error_log_file_name)
        error_file_handler = logging.FileHandler(error_log_file_path, encoding='iso8859-1')
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
                                self.logger.exception(f"User {user['email']} cannot be assumed. Error Code: {json_response['errorCode']}", stack_info=True)
                                break

                            total_pages = json_response['totalPages']

                            sheets = json_response['data']
                            for sheet in sheets:
                                if sheet['accessLevel'] == 'OWNER':
                                    sheet['owner_email'] = user['email']
                                    sheets_list.append(sheet)
                            page = page + 1
                        except Exception as e:
                            self.logger.exception(f"{e}", stack_info=True)
                except Exception as e:
                    self.logger.exception(f"There was a problem retrieving all org sheets: {e}", stack_info=True)
                    print(f"There was a problem retrieving all org sheets: {e}")
                    return

            self.logger.info(f"{len(sheets_list)} sheets to process")
            print(f"{len(sheets_list)} sheets to process")

            parent_path = os.path.join(os.path.curdir, 'smartsheet_attachments')
            if not os.path.exists(parent_path):
                os.mkdir(parent_path)

            attachment_manifest_path = os.path.join(parent_path, 'all_attachments.csv')
            if not os.path.exists(attachment_manifest_path):
                with open(attachment_manifest_path, 'w', newline='', encoding='utf-8') as attachment_manifest_file:
                    manifest_csvwriter = csv.writer(attachment_manifest_file, delimiter=',')
                    manifest_csvwriter.writerow(['Attachment ID', 'Attachment Name', 'Created By', 'Created By Email', 'Created At', 'Sheet ID', 'Sheet Name', 'Owner Email', 'Folder Path'])

            with open(attachment_manifest_path, 'a', newline='', encoding='utf-8') as attachment_manifest_file:
                manifest_csvwriter = csv.writer(attachment_manifest_file, delimiter=',')
                for idx, sheet in enumerate(sheets_list):
                    self.logger.info(f"Processing sheet {idx + 1} of {len(sheets_list)}: sheetName={sheet['name']}, sheetId={sheet['id']}, ownerEmail={sheet['owner_email']}")
                    print(f"Processing sheet {idx + 1} of {len(sheets_list)}: : sheetName={sheet['name']}, sheetId={sheet['id']}, ownerEmail={sheet['owner_email']}")

                    owner = sheet['owner_email']
                    owner_folder_name = self.replace_symbol(owner)
                    owner_folder_path = os.path.join(parent_path, owner_folder_name)
                    if not os.path.exists(owner_folder_path):
                        os.mkdir(owner_folder_path)

                    sheet_folder_name = f"{sheet['id']} - {sheet['name']}"
                    sheet_folder_name = self.replace_symbol(sheet_folder_name)
                    sheet_folder_path = os.path.join(owner_folder_path, sheet_folder_name)
                    alternate_sheet_folder_name = f"{sheet['id']}"
                    alternate_sheet_folder_path = os.path.join(owner_folder_path, alternate_sheet_folder_name)
                    if not os.path.exists(sheet_folder_path):
                        try:
                            os.mkdir(sheet_folder_path)
                        except Exception as e1:
                            try:
                                if not os.path.exists(alternate_sheet_folder_path):
                                    os.mkdir(alternate_sheet_folder_path)
                                sheet_folder_path = alternate_sheet_folder_path
                            except Exception as e2:
                                print(
                                    f"There was a problem creating a sheet folder for sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e2}")
                                self.logger.error(
                                    f"There was a problem creating a sheet folder for sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e2}")
                                continue

                    sheet_folder_manifest_file_path = os.path.join(owner_folder_path, 'sheet_folder_manifest.csv')
                    if not os.path.exists(sheet_folder_manifest_file_path):
                        with open(sheet_folder_manifest_file_path, 'w', newline='', encoding='utf-8') as sheet_folder_manifest_file:
                            csvwriter = csv.writer(sheet_folder_manifest_file, delimiter=',')
                            csvwriter.writerow(['Sheet ID', 'Sheet Name', 'Owner Email', 'Folder Name', 'Alternate Folder Name'])

                    with open(sheet_folder_manifest_file_path, 'a', newline='', encoding='utf-8') as sheet_folder_manifest_file:
                        csvwriter = csv.writer(sheet_folder_manifest_file, delimiter=',')
                        csvwriter.writerow([f"{sheet['id']}", sheet['name'], sheet['owner_email'], sheet_folder_name, alternate_sheet_folder_name])

                    # Process any attachments
                    try:
                        self.logger.info(f"Processing attachments for sheet '{sheet['name']}' (sheetId: {sheet['id']})")
                        attachments = smar_helper.list_attachments(config.SMARTSHEET_ACCESS_TOKEN, sheet['id'], sheet['owner_email'])
                        json_attachments = json.loads(attachments.to_json())
                        if 'data' in json_attachments and len(json_attachments['data']) > 0:
                            attachment_folder_path = os.path.join(sheet_folder_path, 'attachments')
                            if not os.path.exists(attachment_folder_path):
                                os.mkdir(attachment_folder_path)

                            attachment_list_file_path = os.path.join(sheet_folder_path, 'attachments.csv')
                            if not os.path.exists(attachment_list_file_path):
                                with open(attachment_list_file_path, 'w', newline='', encoding='utf-8') as attachment_list_file:
                                    csvwriter = csv.writer(attachment_list_file, delimiter=',')
                                    csvwriter.writerow(['Attachment ID', 'Attachment Name', 'Created By', 'Created By Email', 'Created At'])

                            for file in json_attachments['data']:
                                if file['attachmentType'] == 'FILE':
                                    print(f"Processing attachment {file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}'")
                                    self.logger.info(f"Processing attachment {file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}'")
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
                                            file_name = self.replace_symbol(file['name'])
                                        else:
                                            file_name = None

                                        attachment_logged = False
                                        try:
                                            attachment_list_reader = csv.reader(open(attachment_list_file_path, 'r', encoding='utf-8'))
                                            for row in attachment_list_reader:
                                                if row[0] == f"{file_id}":
                                                    attachment_logged = True
                                                    break
                                        except:
                                            attachment_list_reader = csv.reader(open(attachment_list_file_path, 'r', encoding='iso8859-1'))
                                            for row in attachment_list_reader:
                                                if row[0] == f"{file_id}":
                                                    attachment_logged = True
                                                    break

                                        if attachment_logged:
                                            self.logger.info(
                                                f"Already downloaded attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}'")
                                            print(
                                                f"Already downloaded attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}")
                                            continue
                                        else:
                                            with open(attachment_list_file_path, "a", newline='', encoding='utf-8') as attachment_file_update:
                                                self.logger.info(f"Downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}'")
                                                file_extension = os.path.splitext(file_name)[1]
                                                alternate_file_name = f"{file_id}{file_extension}"
                                                attachment_csv_writer = csv.writer(attachment_file_update, delimiter=',')
                                                retry_allowance = 1
                                                success = False
                                                while retry_allowance >= 0 and not success:
                                                    try:
                                                        smar_helper.download_attachment(config.SMARTSHEET_ACCESS_TOKEN,
                                                                                        attachment_details,
                                                                                        attachment_folder_path,
                                                                                        sheet['owner_email'],
                                                                                        file_name,
                                                                                        alternate_file_name=alternate_file_name,
                                                                                        logger=self.logger)
                                                        attachment_csv_writer.writerow([f"{file_id}", file_name, file_created_by_name, file_created_by_email, file_created_at])
                                                        manifest_csvwriter.writerow([f"{file_id}", file_name, file_created_by_name, file_created_by_email, file_created_at, f"{sheet['id']}", sheet['name'], sheet['owner_email'], attachment_folder_path])
                                                        success = True
                                                    except FileNotFoundError as file_not_found_error:
                                                        print(f"There was a problem downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {file_not_found_error}. Trying again.")
                                                        self.logger.warning(f"There was a problem downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {file_not_found_error}. Trying again.")
                                                        time.sleep(1)

                                                        attachment_file_update.close()

                                                        if not os.path.exists(alternate_sheet_folder_path):
                                                            os.mkdir(alternate_sheet_folder_path)

                                                        alternate_attachment_folder_path = os.path.join(alternate_sheet_folder_path, 'attachments')
                                                        if not os.path.exists(alternate_attachment_folder_path):
                                                            os.mkdir(alternate_attachment_folder_path)

                                                        attachment_list_file_path = os.path.join(alternate_sheet_folder_path, 'attachments.csv')
                                                        if not os.path.exists(attachment_list_file_path):
                                                            with open(attachment_list_file_path, 'w', newline='', encoding='utf-8') as attachment_list_file:
                                                                csvwriter = csv.writer(attachment_list_file, delimiter=',')
                                                                csvwriter.writerow(['Attachment ID', 'Attachment Name', 'Created By','Created By Email', 'Created At'])

                                                        attachment_logged = False
                                                        try:
                                                            alternate_reader = csv.reader(open(attachment_list_file_path, 'r', encoding='iso8859-1'))
                                                            for row in alternate_reader:
                                                                if row[0] == f"{file_id}":
                                                                    attachment_logged = True
                                                                    break
                                                        except:
                                                            alternate_reader = csv.reader(
                                                                open(attachment_list_file_path, 'r', encoding='utf-8'))
                                                            for row in alternate_reader:
                                                                if row[0] == f"{file_id}":
                                                                    attachment_logged = True
                                                                    break

                                                        if attachment_logged:
                                                            self.logger.info(
                                                                f"Already downloaded attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}'")
                                                            print(
                                                                f"Already downloaded attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}")
                                                            continue
                                                        else:
                                                            with open(attachment_list_file_path, "a", newline='', encoding='utf-8') as alternate_attachment_file_update:
                                                                alternate_attachment_csv_writer = csv.writer(alternate_attachment_file_update, delimiter=',')# open with read/write access, starting at beginning
                                                                smar_helper.download_attachment(config.SMARTSHEET_ACCESS_TOKEN,
                                                                                                attachment_details,
                                                                                                alternate_attachment_folder_path,
                                                                                                sheet['owner_email'],
                                                                                                file_name,
                                                                                                alternate_file_name=alternate_file_name,
                                                                                                logger=self.logger)
                                                                alternate_attachment_csv_writer.writerow([f"{file_id}", file_name, file_created_by_name, file_created_by_email, file_created_at])
                                                                manifest_csvwriter.writerow([f"{file_id}", file_name, file_created_by_name, file_created_by_email, file_created_at, f"{sheet['id']}", sheet['name'], sheet['owner_email'], alternate_attachment_folder_path])
                                                                success = True
                                                    except Exception as e:
                                                        retry_allowance -= 1
                                                        if retry_allowance >= 0:
                                                            print(
                                                                f"There was a problem downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}. Trying again.")
                                                            self.logger.warning(
                                                                f"There was a problem downloading attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}): {e}. Trying again.")
                                                            time.sleep(1)
                                                        else:
                                                            raise e

                                        if delete_attachments:
                                            smar_helper.delete_attachment(config.SMARTSHEET_ACCESS_TOKEN, file_id, sheet['id'], sheet['owner_email'])
                                    except Exception as e:
                                        self.logger.exception(f"There was a problem processing attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}': {e}", stack_info=True)
                                        print(f"There was a problem processing attachment '{file['name']}' from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}': {e}")
                                        continue
                        else:
                            self.logger.info(f"There are no attachments for sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}'")
                    except Exception as e:
                        self.logger.exception(f"There was a problem processing attachments from sheet '{sheet['name']}' (sheetId: {sheet['id']}) for owner '{sheet['owner_email']}': {e}", stack_info=True)
                        continue
        except Exception as e:
            self.logger.exception(f"There was an error in the process: {e}", stack_info=True)
            print(f"There was an error in the process: {e}")

    def delete_attachments(self):
        self.logger.info("Beginning test cases - deletion")

        user_exclusion_list = [e.lower() for e in config.ATTACHMENT_DELETION_USER_EXCLUSION_LIST]

        parent_path = os.path.join(os.path.curdir, 'smartsheet_attachments')
        if not os.path.exists(parent_path):
            os.mkdir(parent_path)

        attachment_manifest_path = os.path.join(parent_path, 'all_attachments.csv')
        attachment_list_reader = csv.reader(open(attachment_manifest_path, 'r', encoding='iso8859-1'))

        next(attachment_list_reader, None)  # skip header row

        for row in attachment_list_reader:
            try:
                attachment_id = row[0]
                attachment_name = row[1]
                sheet_id = row[5]
                sheet_name = row[6]
                sheet_owner_email = row[7].lower()

                if sheet_owner_email in user_exclusion_list:
                    self.logger.info(
                        f"Skipping attachment '{attachment_name}' ({attachment_id}) from sheet '{sheet_name}' ({sheet_id}), owned by '{sheet_owner_email}'")
                    print(f"Skipping attachment '{attachment_name}' ({attachment_id}) from sheet '{sheet_name}' ({sheet_id}), owned by '{sheet_owner_email}'")
                    continue

                self.logger.info(f"Deleting attachment '{attachment_name}' ({attachment_id}) from sheet '{sheet_name}' ({sheet_id}), owned by '{sheet_owner_email}'")
                print(f"Deleting attachment '{attachment_name}' ({attachment_id}) from sheet '{sheet_name}' ({sheet_id}), owned by '{sheet_owner_email}'")
                del_resp = smar_helper.delete_attachment(config.SMARTSHEET_ACCESS_TOKEN, attachment_id, sheet_id, sheet_owner_email)
                if '_result_code' in del_resp.__dict__ and del_resp.result_code == 0:
                    self.logger.info(f"Attachment '{attachment_name}' ({attachment_id}) deleted.")
                    print(f"Attachment '{attachment_name}' ({attachment_id}) deleted.")
                elif del_resp.result.code == 1006:
                    self.logger.info(f"Attachment '{attachment_name}' ({attachment_id}) not found or already deleted.")
                    print(f"Attachment '{attachment_name}' ({attachment_id}) not found or already deleted.")
                else:
                    self.logger.warning(f"Failed to delete attachment '{attachment_name}' ({attachment_id}): ({del_resp.result.code}) {del_resp.result.message}")
                    print(f"Failed to delete attachment '{attachment_name}' ({attachment_id}): ({del_resp.result.code}) {del_resp.result.message}")
            except Exception as e:
                self.logger.exception(f"There was an error in the process: {e}", stack_info=True)
                print(f"There was an error in the process: {e}")

    def replace_symbol(self, filepath):
        filepath = re.sub(r'[^\x00-\x7f]', r'_', filepath)
        for symbol in ['<',
                       '>',
                       ':',
                       '"',
                       '/',
                       '\\',
                       '|',
                       '?',
                       '*',
                       '$',
                       ',',
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
            filepath = pathvalidate.sanitize_filename(filepath, platform='Windows')
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
