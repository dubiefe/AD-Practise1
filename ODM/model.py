# PyMongo dependencies
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# For type hints
from typing import Any, Generator, Self

# Import getLocationPoint function
from ODM.geo import getLocationPoint

class Model:
    """
    Abstract model class.
    Create as many classes inheriting from this class as
    collections/models desired in the database.

    Attributes
    ----------
    _required_vars : set[str]
        Set of attributes required by the model
    _admissible_vars : set[str]
        Set of attributes allowed by the model
    _location_var : set[str]
        Attribute used to store location by the model
    _db : pymongo.collection.Collection
        Connection to the database collection
    _modified_vars : set[str]
        Set of attributes changed in the instantiated object
    _data : pymongo.collection.Collection
        Data saved by instantiated object

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
    _modified_vars: set[str] = set()
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
        # Encapsulating data in one variable simplifies
        # handling in methods like save.
        self._data = {}

        # Track modified variables
        self._modified_vars = set()

        # Check if key (attribute) is valid
        valid_vars = self._required_vars.union(self._admissible_vars)
        for key in kwargs:
            if key not in valid_vars and key != "_id" and key != self._location_var:
                raise ValueError(f"The attribute {key} doesn't exist")

        # Check if all required attributes are provided
        missing_key = [var for var in self._required_vars if var not in kwargs]
        if missing_key:
            raise ValueError(f"Missing required fields: {missing_key}")

        # Assign all values in kwargs to attributes with names matching the 
        # keys in kwargs
        for key, value in kwargs.items():
            if key in valid_vars or key == "_id":
                # Use the data attribute to store variables saved in the 
                # database in a single attribute
                self._data[key] = value

        # Automatically generate the GeoJSON point for the location attribute if 
        # base field is present
        if self._location_var and self._location_var.endswith("_loc"):
            base_field = self._location_var[:-4]
            if base_field in kwargs:
                self._data[self._location_var] = getLocationPoint(kwargs[base_field])

    def __setattr__(self, name: str, value: str | dict) -> None:
        """
        Overrides the attribute assignment method to control
        which attributes are modified and when.
        """

        # Define class attributes and check if __setattr__ is used for them
        internal_attributes = { "_required_vars", "_admissible_vars", "_db", 
                            "_data", "_location_var", "_modified_vars" }
        if name in internal_attributes:
            super().__setattr__(name, value)
        else:
            # Check if the attribute is valid
            valid_vars = self._required_vars.union(self._admissible_vars)
            if name not in valid_vars:
                raise AttributeError(f"[__setattr__] Invalid attribute: {name}")

            # Only add to modified_var if value is actually changing
            old_value = self._data.get(name, None)
            if old_value != value:
                self._modified_vars.add(name)
            # Assign if data checks are passed
            self._data[name] = value
            # If setting the base location field, also set the _loc field
            if self._location_var and self._location_var.endswith("_loc"):
                base_field = self._location_var[:-4]
                if name == base_field:
                    self._data[self._location_var] = getLocationPoint(value)
                    self._modified_vars.add(self._location_var)

    def __getattr__(self, name: str) -> Any:
        """
        Overrides the attribute access method.
        __getattr__ is only called when the attribute
        is not found in the object.
        """

        # Define class attributes and check if __getattr__ is used for them
        internal_attributes = { "_required_vars", "_admissible_vars", "_db", 
                            "_data", "_location_var", "_modified_vars" }
        if name in internal_attributes:
            # return super().__getattr__(name)
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            # Raise error if not
            raise AttributeError(f"[__getattr__] Invalid attribute: {name}")

    def save(self) -> None:
        """
        Saves the model to the database.
        If the model does not exist in the database, a new
        document is created with the model's values. Otherwise,
        the existing document is updated with the new values.
        Only variables that have been modified (tracked in _modified_vars)
        are saved in updates.
        """

        # Ensure required fields are present
        missing = [var for var in self._required_vars if var not in self._data]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # Save required, admissible, _id, and location _loc field
        valid_fields = self._required_vars.union(self._admissible_vars)
        to_save_fields = valid_fields.union({self._location_var}) if self._location_var else valid_fields

        # If _id present, update only modified fields, else insert all
        if "_id" in self._data:
            # Only update fields that have changed
            if self._modified_vars:
                data_to_update = {key: self._data[key]
                    for key in self._modified_vars
                    if key in to_save_fields or key == "_id"
                }
                if data_to_update:
                    print("Updating", data_to_update)
                    self._db.update_one({"_id": self._data["_id"]}, {"$set": data_to_update})
                self._modified_vars.clear()
            else:
                print("No changes to update.")
        else:
            # Insert all valid fields for new document
            data_to_save = {key: value
                for key, value in self._data.items()
                if key in to_save_fields or key == "_id"
            }
            print("Inserting", data_to_save)
            res = self._db.insert_one(data_to_save)
            self._data["_id"] = res.inserted_id
            self._modified_vars.clear()

    def delete(self) -> None:
        """
        Deletes the model from the database.
        """
        # Deletes itself
        self._db.delete_one({"_id": self._data["_id"]})

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
        # cls is the pointer to the class, we find it in the collection
        # Filter is in the right format
        result_find = cls._db.find(filter)

        # We return the model cursor
        return ModelCursor(model_class=cls, cursor=result_find)

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
                        cls._db.create_index([(field, pymongo.ASCENDING)], unique=True)
                    elif idx_type == "regular":
                        cls._db.create_index([(field, pymongo.ASCENDING)])
                    elif idx_type == "2dsphere":
                        cls._db.create_index([(field, pymongo.GEOSPHERE)])
                        cls._location_var = field
                    else:
                        raise ValueError(f"Unknown '{field}': {idx_type}")
                except Exception as e:
                    raise ValueError(f"Error index on field '{field}': {e}")

        if cls._location_var is None:
            raise ValueError(f"_location_var not set")

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
        # Assign model and class (iterator) and set to alive (active)
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
        # Creating the generator
        def generator():
            # While there are more elements
            while self.alive(): 
                try :
                    document = next(self.cursor) # Get next document
                    yield self.model(**document) # Create model and return
                except StopIteration:            # Nothing after, stop
                    self._alive = False          

        return generator()
