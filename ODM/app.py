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

def initApp(definitions_path: str = "./models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="abd", scope=globals()) -> None:
    """
    Declares the classes that inherit from Model for each of the
    models of the collections defined in definitions_path.
    Initializes the model classes by providing the indexes and
    allowed and required attributes for each of them, and the connection to the
    database collection.

    Parameters
    ----------
    definitions_path : str
        Path to the model definitions file
    mongodb_uri : str
        URI for connecting to the database
    db_name : str
        Name of the database
    """
    # Initialize database
    client = MongoClient(mongodb_uri, server_api=ServerApi('1'))
    print(f"Connected to database: {db_name}")

    # Drop previous data
    client.drop_database(db_name)

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    db = client[db_name]

    # Open and read definitions file
    print(f"Loading schema from {definitions_path}")
    with open(definitions_path, 'r') as f:
        schema = yaml.safe_load(f)


    # For each item in the definitions file we create a class
    for class_name, details in schema.items():

        print(f"Initializing model: {class_name}")

        # Get required data from schema
        # Extract indexes from schema
        unique_indexes = details.get('unique_indexes', [])
        regular_indexes = details.get('regular_indexes', [])
        location_index = details.get('location_index', [])

        # Combine all index types into a list of dicts as required by init_class
        indexes = []
        for field in unique_indexes:
            indexes.append({field: "unique"})
        for field in regular_indexes:
            indexes.append({field: "regular"})

        indexes.append({location_index: "2dsphere"})

        # Get variables
        required_vars = set(details.get('required_vars', []))     # set
        admissible_vars = set(details.get('admissible_vars', [])) # set

        # Get or create the MongoDB collection
        db_collection = db[class_name]

        # Initialize the class (link it to the collection, set attributes)
        cls = type(class_name, (Model,), {})

        # Add classnames to globals so they are accesible from elsewhere
        scope[class_name] = cls

        # Initialize class per class and print class data
        cls.init_class(db_collection, indexes, required_vars, admissible_vars)
        print(f"Collection: {db_collection.name}")
        print(f"Required vars: {required_vars}")
        print(f"Admissible vars: {admissible_vars}")
        print(f"Indexes: {indexes}\n")

    # Ignore Pylance warning about MyModel, it cannot detect
    # that the class was declared in the previous line since it is done
    # at runtime.
