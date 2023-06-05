# Delete Objects with Key Prefix.
import sys
sys.path.append( '../modules' )
from upload_data import UploadData
import argparse

# User Inputs.
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'srw', 'land-da', etc.")
argParser.add_argument("-k", "--key", help="Object's key. Type: String. Ex: k = '###/###/[filename].[file_format]'")
args = argParser.parse_args()

# Instantiate Class Object.
uploader_wrapper = UploadData(file_relative_dirs=None, use_bucket=args.bucket)

# Delete Object Based on Key.
uploader_wrapper.purge(args.key)

