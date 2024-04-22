import smartsheet


def list_attachments(access_token, sheet, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.list_all_attachments(sheet, include_all=True)


def get_attachment(access_token, sheet, attachment, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.get_attachment(sheet, attachment)


def download_attachment(access_token, attachment, path, email, alternate_file_name=None):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.download_attachment(attachment, path, alternate_file_name)


def delete_attachment(access_token, attachment_id, sheet_id, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.delete_attachment(sheet_id, attachment_id)
