# PyMongo dependencies
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Handling YAML file
from bson.objectid import ObjectId
import yaml
from pathlib import Path

# Import Model class
from ODM.model import Model

def initApp(
        definitions_path: str = "./models.yml",
        mongodb_uri="mongodb://localhost:27017/",
        db_name="abd",
        scope=globals()
    ) -> None:
    """
    Initialize application models from YAML schema and connect to MongoDB.

    Dynamically creates classes that inherit from `Model` for each collection
    in the schema file. Sets up indexes, attributes, and DB collections.

    Parameters
    ----------
    definitions_path : str, optional
        Path to YAML model definitions file (default './models.yml').
    mongodb_uri : str, optional
        MongoDB connection URI (default 'mongodb://localhost:27017/').
    db_name : str, optional
        Name of MongoDB database (default 'abd').
    scope : dict, optional
        Dictionary (e.g., globals()) for registering model classes.

    Returns
    -------
    None
    """
    client = MongoClient(mongodb_uri, server_api=ServerApi('1'))
    print(f"Connected to database: {db_name}")

    try:
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    db = client[db_name]
    print(f"Loading schema from {definitions_path}")
    try:
        with open(definitions_path, 'r') as f:
            schema = yaml.safe_load(f)
        if not isinstance(schema, dict):
            raise ValueError("Schema file is not a valid dictionary.")
    except FileNotFoundError:
        print(f"Error: Schema file '{definitions_path}' not found.")
        return
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return
    except Exception as e:
        print(f"Unexpected error reading schema file: {e}")
        return

    for class_name, details in schema.items():
        print(f"Initializing model: {class_name}")
        unique_indexes = details.get('unique_indexes', [])
        regular_indexes = details.get('regular_indexes', [])
        location_index = details.get('location_index', [])

        indexes = {}
        for field in unique_indexes:
            indexes[field] = "unique"
        for field in regular_indexes:
            indexes[field] = "regular"
        if location_index:
            indexes[f"{location_index}_loc"] = "2dsphere"

        required_vars = set(details.get('required_vars', []))
        admissible_vars = set(details.get('admissible_vars', []))

        db_collection = db[class_name]
        cls = type(class_name, (Model,), {})
        scope[class_name] = cls
        cls.init_class(
            db_collection, indexes, required_vars, admissible_vars
        )
