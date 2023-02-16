# Demo: Get List of All Keys in UFS Land DA S3 Bucket.
import sys
sys.path.append( '../modules' )
from upload_data import UploadData
import argparse

# User Inputs.
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's key. Type: String. Options: 'srw', 'land-da', etc.")
args = argParser.parse_args()

# Instantiate Class Object.
uploader_wrapper = UploadData(file_relative_dirs=None, use_bucket=args.bucket)

# Get List of All Keys in UFS Land DA S3 Bucket.
uploader_wrapper.get_all_s3_keys()