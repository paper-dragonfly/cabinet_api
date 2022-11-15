![Cabinet Logo](rubix2.jpeg =200x)

# cabinet_api
Flask API for Cabinet blob-storage platfrom 

# Description 
Cabinet is a flexible blob-storage system that stores blobs and thier associated metadata. It allows users to easily save and search for blobs based on their metadata values rather than file paths. The cabinet_api interfaces between the cabinet-sdk client and Cabinet postgreSQL database. 


## Installation and Setup

Download from github: https://github.com/paper-dragonfly/cabinet_api

Designed to be used in connujunction with the cabinet-sdk package which can be found here: Download cabinet-sdk using: ```pip install -i https://test.pypi.org/simple/ cabinet-sdk``` 

Create an empty postgreSQL database called 'cabinet' and another called 'cabinet_test'. See Create Blob_types section below for instructions on populating these databases

If you want to work in an environment other than dev_local, such as production, set the working environment from the command line: ```export ENV='<environment>'```. This is used if you have more than one cabinet database, such as a local one and a remote one. 
Note: this ENV assignment will only last for the duration of your command line session. 
Note2: Regardless of the value of ENV, pytest will run the api app in testing mode. No need to set ENV='testing' to run pytest. 

Use the following CL command to run the API: ```python3 src/api.py```

#### Configuration
Create config.yaml file in the config folder which should be located in your root directory. This file will contain database connection strings and specify the host for each development environment. See config/config_template.yaml.


## Create Blob_types
Within Cabinet, entries are organized by blob_type. Each blob_type has it's own metadata schema. For example your Cabinet might have a 'cat_thumbnails' blob_type with metadata fields [entry_id, blob_type, cat_color, cat_breed, photo_size, photo_source'] or a 'student_essays' blob_type with metadata fields [entry_id, blob_type, student_name, student_DOB, subject, grade]

To add a new blob_type:
1. Navigate to db_setup.py 
2. In the 'DEFINE BLOB_TYPES HERE' section, create a dictionary where the key is the blob_type and the value is a sub-dict with metadata fields as keys and field_datatype as values. 
    * Note: all blob_types have fields entry_id, blob_type and blob_id. Do not include these in your blob_type definition, they will be added automatically 
3. Run db_setup.py 
4. Navigate to the Blob Types section of classes.py  
5. create a new class with attributes corresponding the the fields for yout new blob_type
6. In the Blob_types Reccord section of classes.py, add the blob_type to the blob_types dictionary with key: blob_type_name(str) and value: blob_type_class(class)  
    * e.g. blob_types = {'cat_thumbnails': CatPics, 'student_essays': StudentEssays}
7. Add the class to the blob_classes list - this is used for type hinting 



