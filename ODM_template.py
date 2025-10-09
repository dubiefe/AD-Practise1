__author__ = "Emilie & Itziar"
__students__ = "Emilie Dubief & Itziar Morales Rodríguez"

# GeoJSON and geo locator
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any, Self
from geojson import Point

# PyMongo dependencies
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from bson.objectid import ObjectId
import yaml
from pathlib import Path

# Globals
DATABASE_NAME = "LinkedEs"
KEY_FILE_PATH = "./vockey.pem"

def getLocationPoint(address: str) -> Point:
    """
    Gets the coordinates of an address in geojson.Point format.
    Uses the geopy API to obtain the coordinates of the address.
    Be careful, the API is public and has a request limit, use sleeps.

    Parameters
    ----------
    address : str
        Full address from which to obtain coordinates

    Returns
    -------
    geojson.Point
        Coordinates of the address point
    """
    location = None
    while location is None:
        try:
            time.sleep(1)
            # TODO
            # A user_agent is required to use the API
            # Use a random name for the user_agent
            location = Nominatim(user_agent="My-Random-Name").geocode(address)
        except GeocoderTimedOut:
            # May throw an exception if timeout is exceeded
            # Try again
            continue
    # TODO

class Model:
    """
    Abstract model class.
    Create as many classes inheriting from this class as
    collections/models desired in the database.

    Attributes
    ----------
    required_vars : set[str]
        Set of attributes required by the model
    admissible_vars : set[str]
        Set of attributes allowed by the model
    db : pymongo.collection.Collection
        Connection to the database collection

    Methods
    -------
    __setattr__(name: str, value: str | dict) -> None
        Overrides the attribute assignment method to control
        which attributes are modified and when.
    __getattr__(name: str) -> Any
        Overrides the attribute access method
    save() -> None
        Saves the model to the database
    delete() -> None
        Deletes the model from the database
    find(filter: dict[str, str | dict]) -> ModelCursor
        Performs a read query in the DB.
        Returns a ModelCursor of models
    aggregate(pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor
        Returns the result of an aggregate query.
    find_by_id(id: str) -> dict | None
        Searches for a document by its ID using cache and returns it.
        If not found, returns None.
    init_class(db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None
        Initializes class variables during system initialization.
    """
    _required_vars: set[str]
    _admissible_vars: set[str]
    _location_var: None
    _db: pymongo.collection.Collection
    _data: dict[str, str | dict] = {}

    def __init__(self, **kwargs: dict[str, str | dict]):
        """
        Initializes the model with values provided in kwargs.
        Checks that the values provided in kwargs are allowed
        by the model and that required attributes are provided.

        Parameters
        ----------
        kwargs : dict[str, str | dict]
            Dictionary with the model's attribute values
        """
        # Perform necessary checks and handling
        # before assignment.
        # Assign all values in kwargs to attributes with
        # names matching the keys in kwargs
        # Use the data attribute to store variables
        # saved in the database in a single attribute
        # Encapsulating data in one variable simplifies
        # handling in methods like save.
        print(f"Creating class {self.__class__.__name__}")
        self._data = {}

        # Proper attribute check
        for key in kwargs:
            if key not in self._required_vars and key not in self._admissible_vars:
                raise ValueError(f"The attribute {key} doesn't exist")
        # Validate required variables
        missing = [var for var in self._required_vars if var not in kwargs]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        valid_vars = self._required_vars.union(self._admissible_vars)
        for k, v in kwargs.items():
            if k in valid_vars or k == "_id":
                self._data[k] = v

    def __setattr__(self, name: str, value: str | dict) -> None:
        """
        Overrides the attribute assignment method to control
        which attributes are modified and when.
        """
        # TODO
        # Perform necessary checks and handling
        # before assignment.
        # Assign the value to the variable name
        self._data[name] = value

        # Use super for class attributes, _data for model fields
        if name in {"_required_vars", "_admissible_vars", "_db", "_data", "_location_var"}:
            super().__setattr__(name, value)
        else:
            # Check if the attribute is in the required or in the admissible variables
            if (name not in self._required_vars) and (name not in self._admissible_vars) :
                raise AttributeError("The attribute " + name + " is not accepted for this document")
            else :
                self._data[name] = value

    def __getattr__(self, name: str) -> Any:
        """
        Overrides the attribute access method.
        __getattr__ is only called when the attribute
        is not found in the object.
        """
        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_data', '_location_var'}:
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError

    def save(self) -> None:
        """
        Saves the model to the database.
        If the model does not exist in the database, a new
        document is created with the model's values. Otherwise,
        the existing document is updated with the new values.
        """
        # TODO
        # Ensure required fields are present
        missing = [var for var in self._required_vars if var not in self._data]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # Only save admissible (and required) fields
        valid_fields = self._required_vars.union(self._admissible_vars)
        data_to_save = {k: v for k, v in self._data.items() if k in valid_fields or k == "_id"}

        # If _id present, update, else insert
        if "_id" in self._data:
            print("Updating", self._data)
            self._db.update_one({"_id": self._data["_id"]}, {"$set": data_to_save})
        else:
            print("Inserting", data_to_save)
            res = self._db.insert_one(data_to_save)
            self._data["_id"] = res.inserted_id

    def delete(self) -> None:
        """
        Deletes the model from the database.
        """
        # TODO
        pass

    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        """
        Uses pymongo's find method to perform a read query
        in the database.
        find should return a ModelCursor of models.

        Parameters
        ----------
        filter : dict[str, str | dict]
            Dictionary with the search criteria

        Returns
        -------
        ModelCursor
            Cursor of models
        """
        # TODO
        # cls is the pointer to the class
        pass  # Don't forget to remove this line once implemented

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        """
        Returns the result of an aggregate query.
        Nothing needs to be done in this function.
        It will be used for queries requested
        in the second project of the practice.

        Parameters
        ----------
        pipeline : list[dict]
            List of stages in the aggregate query

        Returns
        -------
        pymongo.command_cursor.CommandCursor
            pymongo cursor with the query result
        """
        return cls.db.aggregate(pipeline)

    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        """
        DO NOT IMPLEMENT UNTIL THE THIRD PROJECT
        Searches for a document by its ID using cache and returns it.
        If not found, returns None.

        Parameters
        ----------
        id : str
            ID of the document to search

        Returns
        -------
        Self | None
            Model of the found document or None if not found
        """
        # TODO
        pass

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, indexes: dict[str, str], required_vars: set[str], admissible_vars: set[str]) -> None:
        """
        Initializes class attributes during system initialization.
        Here, indexes should be initialized or ensured. Additional
        initialization/checks or changes may also be made
        as deemed necessary by the student.

        Parameters
        ----------
        db_collection : pymongo.collection.Collection
            Connection to the database collection
        indexes: Dict[str, str]
            Set of indexes and index types for the collection
        required_vars : set[str]
            Set of attributes required by the model
        admissible_vars : set[str]
            Set of attributes allowed by the model
        """
        cls._db = db_collection
        cls._required_vars = required_vars
        cls._admissible_vars = admissible_vars
        cls._location_var = None

        # Initialize indexes from the indexes dictionary, ascending by default
        for index in indexes:
            for field, idx_type in index.items():
                try:
                    if idx_type == "unique":
                        print(f"Creating UNIQUE index on field '{field}'")
                        cls._db.create_index([(field, pymongo.ASCENDING)], unique=True)
                    elif idx_type == "regular":
                        print(f"Creating REGULAR index on field '{field}'")
                        cls._db.create_index([(field, pymongo.ASCENDING)])
                    elif idx_type == "2dsphere":
                        print(f"Creating GEOSPHERE (2dsphere) index on field '{field}'")
                        cls._db.create_index([(field, pymongo.GEOSPHERE)])
                        cls._location_var = field
                    else:
                        raise ValueError(f"Unknown index type for field '{field}': {idx_type}")
                except Exception as e:
                    print(f"Error creating index on field '{field}': {e}")

        # Check if the location variable is set (mandatory for all)
        if cls._location_var == None:
            raise ValueError(f"_location_var not set")

        # Get tihs to work
        print(f"Creating class: {Self.__class__.__name__}")

        print(f"Required_vars: {cls._required_vars}")
        # TODO
        
class ModelCursor:
    """
    Cursor to iterate over the documents resulting from a query.
    Documents must be returned as model objects.

    Attributes
    ----------
    model_class : Model
        Class used to create models from the documents being iterated.
    cursor : pymongo.cursor.Cursor
        Pymongo cursor to iterate

    Methods
    -------
    alive()
        Return the value of alive
    
    __iter__() -> Generator
        Returns an iterator that goes through the cursor elements
        and returns the documents as model objects.
    """
    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        """
        Initializes the cursor with the model class and pymongo cursor.

        Parameters
        ----------
        model_class : Model
            Class used to create models from the documents being iterated.
        cursor: pymongo.cursor.Cursor
            Pymongo cursor to iterate
        """
        self.model = model_class
        self.cursor = cursor
        self._alive = True

    def alive(self):
        return self._alive

    def __iter__(self) -> Generator:
        """
        Returns an iterator that goes through the cursor elements
        and returns the documents as model objects.
        Use yield to generate the iterator.
        Use next to get the next document from the cursor.
        Use alive to check if more documents exist.
        """
        # TODO
        # creating the generator
        def generator():
            while self.alive(): 
                try :
                    document = next(self)       # get the next document
                    yield document              # send the document
                except StopIteration:           # handle exception raised by next if there are no documents left
                    self._alive = False         # set the iterator to not be alive

        return generator()
    
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
    print(f"Loading schema from {definitions_path}")
    client = None
    if mongodb_uri != "mongodb://localhost:27017/":
        client = MongoClient(
            mongodb_uri,
            tls=True,
            tlsCertificateKeyFile='./vockey.pem',
            tlsAllowInvalidCertificates=True,  # Remove in production!
            server_api=ServerApi('1')
        )
    else:
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

    # TODO
    # Declare as many model classes as there are collections in the database
    # Read the model definitions file to get the collections,
    # indexes, and the allowed and required attributes for each of them.

    # Example of model declaration for a collection called MyModel
    #scope["MyModel"] = type("MyModel", (Model,), {})

    _location_var: None

    yml_path = "./models.yml"
    with open(yml_path, 'r') as f:
        schema = yaml.safe_load(f)

    for class_name, details in schema.items():
        # Get required data from schema
        print(f"Initializing model: {class_name}")


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
        scope[class_name] = cls
        cls.init_class(db_collection, indexes, required_vars, admissible_vars)
        print(f"Collection: {db_collection.name}")
        print(f"Required vars: {required_vars}")
        print(f"Admissible vars: {admissible_vars}")
        print(f"Indexes: {indexes}\n")

    # Ignore Pylance warning about MyModel, it cannot detect
    # that the class was declared in the previous line since it is done
    # at runtime.
    
if __name__ == '__main__':
    # Initialize database and models with initApp
    # TODO

    # if vockey.pem found
    #atlas_uri = "mongodb+srv://itziar:2hVxGqn7&w3Q5nRdDGVy@ad1.fnx6k6d.mongodb.net/?retryWrites=true&w=majority&appName=AD1"
    atlas_uri = "mongodb+srv://ad1.fnx6k6d.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority&appName=AD1"

    # Otherwise
    #uri = ""

    initApp(mongodb_uri = atlas_uri) if Path("./vockey.pem").exists() else initApp()


    # Example
    #m = User(name="Pablo", email="pedro@gmail.com")
    #m.save()
    #m.name = "Pedro"
    #print(m.name)

    # Run tests to verify the model works correctly
    # TODO
    # Create model
    # Assign new value to allowed variable of the object
    # Assign new value to disallowed variable of the object
    # Save
    # Assign new value to allowed variable of the object
    # Save
    # Search for new document with find
    # Get first document
    # Modify value of allowed variable
    # Save
