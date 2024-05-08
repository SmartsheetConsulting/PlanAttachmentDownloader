from config import *
import argparse
from services.export_service_initial_pull_only import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download_attachments", help="Download and catalog attachments", action="store_true")
    parser.add_argument("--delete_attachments", help="Delete attachments", action="store_true")
    args = parser.parse_args()
    export_service = ExportServiceInitialPullOnly()
    if args.download_attachments:
        export_service.download_attachments(args.delete_attachments)
    elif args.delete_attachments:
        export_service.delete_attachments()


if __name__ == '__main__':
    main()
