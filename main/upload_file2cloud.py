# Demo: Upload a Single Data File of Interest.
import sys
sys.path.append( '../modules' )
from progress_bar import ProgressPercentage
from upload_data import UploadData
import argparse

# User Inputs.
argParser = argparse.ArgumentParser()
argParser.add_argument("-b", "--bucket", help="Object's bucket label. Type: String. Options: 'srw', 'land-da', etc.")
argParser.add_argument("-k", "--key", help="Object's key. Type: String. Currently, the key should be set as the file of interest's relative directory path on your local system.  Ex: k = '###/###/[filename].[file_format]'")
args = argParser.parse_args()

# Instantiate Class Object.
uploader_wrapper = UploadData(file_relative_dirs=None, use_bucket=args.bucket)

# Upload Single Data File of Interest.
uploader_wrapper.upload_single_file(args.key)
