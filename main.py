from config import *
from services.export_service_initial_pull_only import *

def main():
    export_service = ExportServiceInitialPullOnly()
    export_service.download_attachments()

if __name__ == '__main__':
    main()
