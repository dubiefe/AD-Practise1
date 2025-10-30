# PyMongo dependencies
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import DuplicateKeyError

# For type hints
from typing import Any, Generator, Self

# Import getLocationPoint function
from ODM.geo import getLocationPoint, _address_cache

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
        self._data = {}
        self._modified_vars = set()

        valid_vars = self._required_vars.union(self._admissible_vars)
        for key in kwargs:
            if key not in valid_vars and key != "_id" and key != self._location_var:
                raise ValueError(f"The attribute {key} doesn't exist")

        missing_key = [var for var in self._required_vars if var not in kwargs]
        if missing_key:
            raise ValueError(f"Missing required fields: {missing_key}")

        for key, value in kwargs.items():
            if key in valid_vars or key == "_id":
                self._data[key] = value

    def __setattr__(self, name: str, value: str | dict) -> None:
        internal_attributes = { "_required_vars", "_admissible_vars", "_db", 
                            "_data", "_location_var", "_modified_vars" }
        if name in internal_attributes:
            super().__setattr__(name, value)
        else:
            valid_vars = self._required_vars.union(self._admissible_vars)
            if name not in valid_vars:
                raise AttributeError(f"[__setattr__] Invalid attribute: {name}")

            old_value = self._data.get(name, None)
            if old_value != value:
                self._modified_vars.add(name)
            self._data[name] = value

    def __getattr__(self, name: str) -> Any:
        internal_attributes = { "_required_vars", "_admissible_vars", "_db", 
                            "_data", "_location_var", "_modified_vars" }
        if name in internal_attributes:
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"[__getattr__] Invalid attribute: {name}")

    def save(self) -> None:
        """
        Saves the model to the database.
        Calculates location only if record is new, address changed, or location is missing/invalid.
        """
        # Check required fields
        missing = [var for var in self._required_vars if var not in self._data]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        valid_fields = self._required_vars.union(self._admissible_vars)
        to_save_fields = valid_fields.union({self._location_var}) if self._location_var else valid_fields

        # Helper: find existing document
        def find_existing():
            if "_id" in self._data:
                return self._db.find_one({"_id": self._data["_id"]})
            elif "name" in self._data:
                return self._db.find_one({"name": self._data["name"]})
            return None

        # Helper: check if location is valid
        def is_valid_location(loc):
            if not loc:
                return False
            if isinstance(loc, dict):
                coords = loc.get('coordinates')
                return coords and coords != [0, 0]
            if isinstance(loc, list):
                return loc != [0, 0]
            return False


        # Location calculation
        if self._location_var and self._location_var.endswith("_loc"):
            base_field = self._location_var[:-4]
            address_value = self._data.get(base_field)
            existing_doc = find_existing()
            existing_address = existing_doc.get(base_field) if existing_doc else None
            existing_location = existing_doc.get(self._location_var) if existing_doc else None

            # Only update location if new, address changed, or location missing/invalid
            if address_value and (not existing_doc or address_value != existing_address or not is_valid_location(existing_location)):
                self._data[self._location_var] = _address_cache.get(address_value) or getLocationPoint(address_value)
                self._modified_vars.add(self._location_var)
            elif existing_doc and is_valid_location(existing_location):
                self._data[self._location_var] = existing_location

        # Upsert/update/insert logic
        if "_id" in self._data:
            if self._modified_vars:
                data_to_update = {k: self._data[k] for k in self._modified_vars if k in to_save_fields or k == "_id"}
                if data_to_update:
                    self._db.update_one({"_id": self._data["_id"]}, {"$set": data_to_update})
                self._modified_vars.clear()
        elif self._db.find_one({"name": self._data["name"]}):
            data_to_update = {k: self._data[k] for k in to_save_fields if k in self._data}
            res = self._db.update_one({"name": self._data["name"]}, {"$set": data_to_update})
            self._data["_id"] = res.upserted_id
            self._modified_vars.clear()
        else:
            data_to_save = {k: v for k, v in self._data.items() if k in to_save_fields or k == "_id"}
            res = self._db.insert_one(data_to_save)
            self._data["_id"] = res.inserted_id
            self._modified_vars.clear()

    def delete(self) -> None:
        self._db.delete_one({"_id": self._data["_id"]})

    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        result_find = cls._db.find(filter)
        return ModelCursor(model_class=cls, cursor=result_find)

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        return cls._db.aggregate(pipeline)

    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        pass

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, indexes: dict[str, str], required_vars: set[str], admissible_vars: set[str]) -> None:
        cls._db = db_collection
        cls._required_vars = required_vars
        cls._admissible_vars = admissible_vars
        cls._location_var = None

        for field, idx_type in indexes.items():
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
    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        self.model = model_class
        self.cursor = cursor
        self._alive = True

    def alive(self):
        return self._alive

    def __iter__(self) -> Generator:
        def generator():
            while self.alive(): 
                try:
                    document = next(self.cursor)
                    yield self.model(**document)
                except StopIteration:
                    self._alive = False
        return generator()
