# Delete Objects with Key Prefix.
import sys
sys.path.append( '../modules' )
from upload_data import UploadData
import argparse

# User Inputs.
"""
Example: python change_object_key.py -b land-da -k_current current_land_da_release_data/[CURRENT_OBJECT_NAME_IN_CLOUD].tar.gz -k_new current_land_da_release_data/[NEW_OBJECT_NAME_IN_CLOUD].tar.gz
"""

argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'land-da'")
argParser.add_argument("-k_current", "--current_key", help="Object's current key. Type: String. Ex: k = '###/###/[filename].[file_format]'")
argParser.add_argument("-k_new", "--new_key", help="Object's new key. Type: String. Ex: k = '###/###/[filename].[file_format]'")
args = argParser.parse_args()

# Instantiate Class Object.
uploader_wrapper = UploadData(file_relative_dirs=None, use_bucket=args.bucket)

# Delete Object Based on Key.
uploader_wrapper.rename_s3_keys(args.current_key, args.new_key)
