# cabinet_api
Flask API for Cabinet blob-storage platfrom 

# Description 
Cabinet is a flexible blob-storage system that stores blobs and thier associated metadata. It allows users to easily save and search for blobs based on their metadata values rather than file paths. The cabinet_api interfaces between the cabinet-sdk client and Cabinet postgreSQL database. 


## Installation 

Install using 

Designed to be used in connujunction with the cabinet-sdk package which can be found here: Download cabinet-sdk using: ```pip install -i https://test.pypi.org/simple/ cabinet-sdk``` 

Must create config file for api to work (see configuration below)
Create an empty postgreSQL database called 'cabinet' and another called 'cabinet_test' 

## Configuration 
Add config/config.yaml to your root directory. If it already exists, simply add the cabinet specific code to the config.yaml file. In this file, a connection string and host is provided for each development environment. 

Sample config.yaml file:
---
dev_local: #local db, local app
  conn_str: postgres://katcha@localhost:5432/cabinet
  host: localhost

testing: #local db, local app
  conn_str: postgres://katcha@localhost:5432/cabinet_test
  host: localhost

## Create blob_types
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



