import smartsheet


def list_attachments(access_token, sheet, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.list_all_attachments(sheet, include_all=True)


def get_attachment(access_token, sheet, attachment, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.get_attachment(sheet, attachment)


def download_attachment(access_token, attachment, path, email):
    smart = smartsheet.Smartsheet(access_token)
    smart.assume_user(email)
    return smart.Attachments.download_attachment(attachment, path)