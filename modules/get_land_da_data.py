import os 
import pickle
from collections import defaultdict
import subprocess
import tarfile


class GetLandDaData():
    """
    Extract locality of the UFS datasets of interest & generate a dictionary which will
    map the UFS dataset files into the dataset types.
    
    """
    
    def __init__(self, avoid_ma_fldrs, avoid_fix_fldrs, avoid_ne_fldrs, avoid_fc_sample_fldrs, fix_data_dir, input_model_data_dir, ne_data_dir, fc_sample_data_dir):
        """
        Args: 
            avoid_ma_fldrs (str): Foldername to ignore within main input model data directory 
                                  of interest on-prem.
            avoid_fix_fldrs (str): Foldername to ignore within main fix data directory of interest 
                                  on-prem.
            avoid_ne_fldrs (str): Foldername to ignore within main natural earth data directory of interest 
                                  on-prem.
            avoid_fc_sample_fldrs (str): Foldername to ignore within main  forecast sample data directory of interest 
                                  on-prem.                                  
            fix_data_dir (str): Source directory of the fixed datasets.
            
            input_model_data_dir (str): Source directory of the input model datasets.
            
            ne_data_dir (str): Source directory of the natural earth datasets. 
            
            fc_sample_data_dir (str): Source directory of the sample forecast datasets. 

        """
        
        # Main directories of the various datasets on-premise.
        self.fix_data_dir = fix_data_dir
        self.input_model_data_dir = input_model_data_dir
        self.ne_data_dir = ne_data_dir
        self.fc_sample_data_dir = fc_sample_data_dir
        
        # Child directories of the datasets to ignore for cloud migration.        
        self.avoid_ma_fldrs = avoid_ma_fldrs
        self.avoid_fix_fldrs = avoid_fix_fldrs
        self.avoid_ne_fldrs = avoid_ne_fldrs
        self.avoid_fc_sample_fldrs = avoid_fc_sample_fldrs
        
        # Extract all data directories residing w/in datasets' main hpc directories.
        self.ma_file_dirs = self.get_data_dirs('input_model_data')
        self.fix_file_dirs = self.get_data_dirs('fix_data')
        self.ne_dirs = self.get_data_dirs('ne_data')
        self.fc_sample_dirs = self.get_data_dirs('fc_sample_data')
        
        # List of model analysis data files for Land DA's multi-preprocessor. ================================== Optional
        self.partition_ma_datasets = self.get_model_analysis_data()

        # List of grid fixed data files for Land DA's multi-preprocessor. ================================== Optional
        self.partition_fixed_datasets = self.get_fixed_data()
        
        # List of all natural earth data.
        self.partition_ne_datasets = self.get_ne_data()
        
        # List of sample forecast data.
        self.partition_fc_datasets = self.get_fc_data()
        
        # Requested by AUS to transfer Land DA fix, input & natural earth data as tar objects. ====== Modify
        # List all Land DA data directories from sources (filtered). 
        self.ma_data_list = self.get_tar_data_dirs('input_model_data')
        self.fix_data_list = self.get_tar_data_dirs('fix_data')
        self.ne_data_list = self.get_data_dirs('ne_data')         
        
        # Land DA forecast samples to support Land DA application (include: Observation, Model Forecast Output)
        self.fc_sample_data_list = self.get_tar_data_dirs('fc_sample_data')   
    
    def get_data_dirs(self, data_type):
        """
        Extract list of nested directories within given data's main directory.
        
        Args:
            data_type (str): Foldername of dataset category of interest. 
                             Options:'input_model_data', 'fix_data', 'ne_data', 'fc_sample_data'
            
        Return (list): Filtered list of nested directories within given data's main directory.
        
        """
        # Set dataset category & child directories of tar data files to ignore.
        if data_type == 'input_model_data':
            suffix_fldr = 'input_model_data'
            avoid_fldrs = self.avoid_ma_fldrs
        elif data_type == 'fix_data':
            suffix_fldr = 'fix'
            avoid_fldrs = self.avoid_fix_fldrs
        elif data_type == 'ne_data': # ==================================================== Modify. May have to remove. TBD.
            suffix_fldr = 'NaturalEarth'
            avoid_fldrs = self.avoid_ne_fldrs
        elif data_type == 'fc_sample_data':
            suffix_fldr = 'NaturalEarth' # ==================================================== Modify. TBD on Forecast Sample location.
            avoid_fldrs = self.avoid_fc_sample_fldrs
        else:
            print(f"{data_type} does not exist")
          
        # Generate list of all file directories residing w/in datasets' 
        # main directory of interest. 
        file_dirs = []
        file_size = []
        root_dirs = []
        
        # ** TODO: Grab the root of the folders of interests and set as an argument to class for non-tar ============= Modify
        # situations. If tar is being transferred, set to "./" + suffix_fldr**
        for root_dir, subfolders, filenames in os.walk("/home/schin/work/noaa/fv3-cam/UFS_SRW_App/develop/" + suffix_fldr, followlinks=True):
            root_dirs.append(root_dir)
            for file in filenames:
                file_dirs.append(os.path.join(root_dir, file))
        
        # List of all data folders/files in datasets' main directory of interest.
        
        # ** TODO: Grab the root of the folders of interests and set as an argument to class for non-tar 
        # situations. If tar is being transferred, set to "./" + suffix_fldr**
        root_list = os.listdir("/home/schin/work/noaa/fv3-cam/UFS_SRW_App/develop/" + suffix_fldr)
        print("\033[1m" +\
              "\nAll Primary Dataset Folders In Land DA's " +\
              f"{data_type} Data Directory:" +\
              f"\n\n\033[0m{root_dirs}\n")
        
        # Removal of personal names.
        if avoid_fldrs != None:
            file_dirs = [x for x in file_dirs if any(x for name in avoid_fldrs if name not in x)]
        
        return file_dirs
    
    def get_tar_data_dirs(self, dataset_type):
        """
        Extract list of nested directories within given data's tar folder.
        
        Args:
            dataset_type (str): Foldername of dataset category of interest. 
                                Options:'input_model_data', 'fix_data', 'ne_data', 'fc_sample_data'

            
        Return (list): Filtered list of nested directories within given data's tar folder.
        
        """
        # Set dataset category & child directories of tar data files to ignore.
        if dataset_type == 'fix_data':
            tar_data_dir = self.fix_data_dir
            avoid = ['fix/fix_am/co2dat_4a', 
                     'fix/fix_orog',
                     'fix', 
                     'fix/fix_am', 
                     'fix/fix_am/fix_co2_proj', 
                     'fix/fix_aer', 
                     'fix/fix_sfc_climo', 
                     'fix/fix_lut']
        elif dataset_type == 'input_model_data':
            tar_data_dir = self.input_model_data_dir
            avoid = [ 'input_model_data/FV3GFS',
                      'input_model_data/RAP',
                      'input_model_data/HRRR',
                      'input_model_data/NAM',
                      'input_model_data/GSMGFS']
        elif dataset_type == 'ne_data': # ==================================================== Modify. TBD on Forecast Sample location.
            tar_data_dir = self.ne_data_dir
            avoid = []
        elif dataset_type == 'fc_sample_data':
            tar_data_dir = self.fc_sample_data_dir
            avoid = []
        
        # Open tar file.
        file_obj = tarfile.open(tar_data_dir,"r")

        # Extract file directories within tar.
        tar_file_list = []
        for f_dir in file_obj.getmembers():
            tar_file_list.append(f_dir.name)
        tar_file_list.sort()
        print(f"\nObtained list of files from {dataset_type} source.")
        print(f"Total Files: {len(tar_file_list)}")
        
        # Filter file directories within tar.
        file_obj.extractall(members=[x for x in file_obj.getmembers() if x.name not in avoid])
        print(f"Filtered files from {dataset_type} source extracted to working dir.")
        
        return tar_file_list

    def get_model_analysis_data(self):  # =========================== Optional. If there is a desire to categorize the datasets
        """
        Extract list of external model analysis file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the file directories into the
        external model for which generated the model analysis data file.

        """
        
        # Extract list of all external model analysis file directories. 
        partition_ma_datasets = defaultdict(list) 
        for file_dir in self.ma_file_dirs:

            # FV3GFS data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['FV3GFS']):
                partition_ma_datasets['FV3GFS'].append(file_dir.replace("./", ""))

            # GSMGFS data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['GSMGFS']):
                partition_ma_datasets['GSMGFS'].append(file_dir.replace("./", ""))
                
            # HRRR data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['HRRR']):
                partition_ma_datasets['HRRR'].append(file_dir.replace("./", ""))
                
            # NAM data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['NAM']):
                partition_ma_datasets['NAM'].append(file_dir.replace("./", ""))
                
            # RAP data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['RAP']):
                partition_ma_datasets['RAP'].append(file_dir.replace("./", ""))

        return partition_ma_datasets    
    
    def get_fixed_data(self): # =========================== Optional. If there is a desire to categorize the fixed datasets
        """
        Extract list of all fixed file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the fixed file directories into the
        fixed data categories.
        
        """
        
        # Extract list of all grid fixed file directories.
        partition_fix_datasets = defaultdict(list) 
        for file_dir in self.fix_file_dirs:

            # Fixed aer data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_aer']):
                partition_fix_datasets['fix_aer'].append(file_dir.replace("./", ""))

            # fixed am data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_am']):
                partition_fix_datasets['fix_am'].append(file_dir.replace("./", ""))
                
            # Fixed lut data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_lut']):
                partition_fix_datasets['fix_lut'].append(file_dir.replace("./", ""))
                
            # Fixed orog data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_orog']):
                partition_fix_datasets['fix_orog'].append(file_dir.replace("./", ""))
                
            # Fixed orog data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['fix_sfc_climo']):
                partition_fix_datasets['fix_sfc_climo'].append(file_dir.replace("./", ""))

        return partition_fix_datasets      

    def get_ne_data(self): # =========================== Optional. If there is a desire to categorize the datasets
        """
        Extract list of all Natural Eartch file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the fixed file directories into the
        fixed data categories.
        
        """
        
        # Extract list of all grid fixed file directories.
        partition_ne_datasets = defaultdict(list) 
        for file_dir in self.ne_dirs:

            # Fixed raster data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['raster_files']):
                partition_ne_datasets['raster_files'].append(file_dir.replace("./", ""))

            # fixed shapefiles data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['shapefiles']):
                partition_ne_datasets['shapefiles'].append(file_dir.replace("./", ""))

        return partition_ne_datasets    

    def get_fc_data(self): # =========================== Optional. If there is a desire to categorize the fc datasets
        """
        Extract list of all forecast sample file directories.

        Args: 
            None
            
        Return (dict): Dictionary partitioning the fixed file directories into the
        fixed data categories.
        
        """
        
        # Extract list of all grid fixed file directories.
        partition_fc_datasets = defaultdict(list) 
        for file_dir in self.fc_sample_dirs:

            # Fixed raster data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['raster_files']):
                partition_fc_datasets['raster_files'].append(file_dir.replace("./", ""))

            # fixed shapefiles data files w/ root directory truncated.
            if any(subfolder in file_dir for subfolder in ['shapefiles']):
                partition_fc_datasets['shapefiles'].append(file_dir.replace("./", ""))

        return partition_fc_datasets   