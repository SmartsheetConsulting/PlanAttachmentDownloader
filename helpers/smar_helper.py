import smartsheet
import requests
import os
import contextlib


def list_attachments(access_token, sheet, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.list_all_attachments(sheet, include_all=True)


def get_attachment(access_token, sheet, attachment, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.get_attachment(sheet, attachment)


def download_attachment(access_token, attachment, path, email, alternate_file_name=None, logger=None, root_path=None):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)

    if not os.path.isdir(path):
        raise ValueError("download_path must be a directory.")

    resp = requests.get(attachment.url, stream=True)

    if 200 <= resp.status_code <= 299:
        response = {
                    "result_code": 0,
                    "message": "SUCCESS",
                    "resp": resp,
                    "filename": attachment.name,
                    "download_directory": path,
                }

        if alternate_file_name is not None:
            response.filename = alternate_file_name

        download_path = os.path.join(path, response.filename)
        if not os.path.exists(path):
            os.makedirs(path)
        try:
            with open(download_path, "wb") as dlfile:
                logger.info(f"Successfully opened file for writing")
                with contextlib.closing(resp):
                    for chunk in resp.iter_content(2**16):
                        dlfile.write(chunk)
        except Exception as e:
            if root_path is not None:
                logger.exception(f"Error: {e}. Attempting to rename file and download to root path: {download_path}", stack_info=True)
                download_path = os.path.join(root_path, attachment.id)
                with open(download_path, "wb") as dlfile:
                    logger.info(f"Successfully opened file for writing")
                    with contextlib.closing(resp):
                        for chunk in resp.iter_content(2**16):
                            dlfile.write(chunk)
            else:
                raise
        return response
    else:
        return {
                "result": {"status_code": resp.status_code},
                "request_response": resp
        }


def _download_attachment(access_token, attachment, path, email, alternate_file_name=None):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.download_attachment(attachment, path, alternate_file_name)


def delete_attachment(access_token, attachment_id, sheet_id, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.delete_attachment(sheet_id, attachment_id)
