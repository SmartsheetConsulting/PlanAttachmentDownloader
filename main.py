from config import *
import argparse
from services.export_service_initial_pull_only import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete_attachments", help="Delete attachments after download", action="store_true")
    args = parser.parse_args()
    export_service = ExportServiceInitialPullOnly()
    export_service.download_attachments(args.delete_attachments)


if __name__ == '__main__':
    main()
