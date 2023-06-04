# Import Dependencies
import yaml
import sys
import os

# Function to Read configuartion.yaml
def get_yaml():
    
    try:
        
        # get configuration.yaml path
        
        path = os.getcwd()
        path = path + "\src\data\configuration.yaml"
        print(path)
        
        
        # open configuration.yaml file and raed configurations
        
        with open(path, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            return cfg
    
    except Exception as e:
        
        print("Error reading configrations from .yaml",  str(e))


def read_source_configurations():
    
    # read source configrations from configurations.yaml
    
    try:
        cfg = get_yaml()
        source_config=cfg["source"]
        return source_config      
    
    except Exception as e:
        
        print("Error reading source configurations", str(e))


def read_local_configurations():
    
    # read local configrations from configurations.yaml
    
    try:
        cfg = get_yaml()
        local_directories=cfg["local"]
        return local_directories      
    
    except Exception as e:
        
        print("Error reading local configurations", str(e))


def read_database_configurations():
    
    # read database configrations from configurations.yaml
    
    try:
        cfg = get_yaml()
        database_directories=cfg["database"]
        return database_directories      
    
    except Exception as e:
        
        print("Error reading database configurations", str(e))