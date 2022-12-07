![Cabinet Logo](rubix2.jpeg)

# cabinet_api
Flask API for Cabinet blob-storage platfrom 

# Description 
Cabinet is a flexible blob-storage system that stores blobs and their associated metadata. It allows users to easily save and search for blobs based on their metadata values rather than file paths. The cabinet_api interfaces between the cabinet-sdk client and Cabinet postgreSQL database. 


## Installation and Setup

1. Download API repo: https://github.com/paper-dragonfly/cabinet_api
2. Install dependencies with `pip install -r requirements.txt`
3. Run API:
    *  `python -m flask --app "src.api:create_app('<ENVIRONMENT>')" run` (development/testing) 
    *  `gunicorn --bind 0.0.0.0:5000 "src.api:create_app('<ENVIRONMENT>')"` (production) 
5. Download cabinet-sdk with: `pip install -i https://test.pypi.org/simple/ cabinet-sdk`
6. Create empty postgreSQL database called 'cabinet' and another called 'cabinet_test' 
    * See Create Blob_types section below for instructions on populating these databases

The ENVIRONMENT variable in the run command determins which database your API will connect to. This is used if you have more than one cabinet database, such as a local database, test database and remote database. Configure this in the config/config.yaml file. 
Note: pytest will allways run in 'testing' mode connecting to the cabinet_test database 


#### Configuration
Create config.yaml file in the config folder which should be located in your root directory. This file will contain database connection strings and specify the host for each environment. See config/config_template.yaml.


## Create Blob_types
Within Cabinet, entries are organized by blob_type. Each blob_type has its own metadata schema. For example your Cabinet might have a 'cat_thumbnails' blob_type with metadata fields [entry_id, blob_type, cat_color, cat_breed, photo_size, photo_source'] or a 'student_essays' blob_type with metadata fields [entry_id, blob_type, student_name, student_DOB, subject, grade]

To add a new blob_type:
1. Navigate to src/db_setup.py 
2. In the 'DEFINE BLOB_TYPES HERE' section, create a dictionary where the key is the blob_type and the value is a sub-dict with metadata fields as keys and field_datatype as values. 
    * Note: all blob_types have fields entry_id, blob_type and blob_hash. Do not include these in your blob_type definition, they will be added automatically 
3. Run db_setup.py 
4. Navigate to the Blob Types section of src/classes.py  
5. create a new class with attributes corresponding the the fields for the new blob_type
6. In the Blob_types Record section of classes.py, add the blob_type to the BLOB_TYPESs dictionary with key: blob_type_name(str) and value: blob_type_class(class)  
    * e.g. blob_types = {'cat_thumbnails': CatPics, 'student_essays': StudentEssays}
7. Add the class to the blob_classes list - this is used for type hinting 

## Testing
Run ```pytest``` in the command line to execute automated testing

