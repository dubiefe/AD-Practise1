import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import DuplicateKeyError

import redis
import json
import hashlib
from bson import ObjectId

from typing import Any, Generator, Self
from ODM.geo import getLocationPoint, _address_cache

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

class Model:
    """
    Base class for ODM models (database collections).

    Attributes
    ----------
    _required_vars : set of str
        Required attribute names.
    _admissible_vars : set of str
        Allowed attribute names.
    _location_var : str or None
        Attribute name for location data.
    _db : pymongo.collection.Collection
        MongoDB collection reference.
    _modified_vars : set of str
        Changed attribute names.
    _data : dict
        Internal data dictionary.

    Methods
    -------
    __setattr__(name, value)
        Control assignment, track changes.
    __getattr__(name)
        Control attribute access.
    save()
        Save the model instance to the database and to the cache (write-through).
    delete()
        Delete the instance from the database.
    find(filter)
        Find documents matching filter (Redis cache).
    aggregate(pipeline)
        MongoDB aggregate query (Redis cache).
    find_by_id(id)
        Find record by its '_id' (Redis cache).
    init_class(...)
        Setup for DB, fields, indexes.
    """

    _required_vars: set[str]
    _admissible_vars: set[str]
    _location_var: None
    _db: pymongo.collection.Collection
    _redis_cache: redis.client.Redis
    _modified_vars: set[str] = set()
    _data: dict[str, str | dict] = {}

    def __init__(self, **kwargs: dict[str, str | dict]):
        """
        Initialize a Model instance. Validates required attributes.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments for attribute values.

        Raises
        ------
        ValueError
            Missing required or unknown attributes.
        """
        self._data = {}
        self._modified_vars = set()

        valid_vars = self._required_vars.union(self._admissible_vars)
        for key in kwargs:
            if key not in valid_vars and key != "_id" \
                and key != self._location_var:
                raise ValueError(f"The attribute {key} doesn't exist")

        missing_key = [
            var for var in self._required_vars if var not in kwargs
        ]
        if missing_key:
            raise ValueError(f"Missing required fields: {missing_key}")

        for key, value in kwargs.items():
            if key in valid_vars or key == "_id":
                self._data[key] = value

    def __setattr__(self, name: str, value: str | dict) -> None:
        """
        Override for setting attributes (tracks changes, restricts assignment).

        Parameters
        ----------
        name : str
            Attribute name.
        value : any
            Value to assign.

        Raises
        ------
        AttributeError
            If invalid attribute given.
        """
        internal = {
            "_required_vars", "_admissible_vars", "_db",
            "_data", "_location_var", "_modified_vars"
        }
        if name in internal:
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
        """
        Attribute access override (handles data fields).

        Parameters
        ----------
        name : str
            Attribute name.

        Returns
        -------
        Any
            Attribute value.

        Raises
        ------
        AttributeError
            If attribute not found.
        """
        internal = {
            "_required_vars", "_admissible_vars", "_db",
            "_data", "_location_var", "_modified_vars"
        }
        if name in internal:
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"[__getattr__] Invalid attribute: {name}")

    def save(self) -> None:
        """
        Save the model instance to the database.

        Location calculation is performed only if:
        - the record is new,
        - address field changed,
        - location field missing.

        Upserts by unique key.

        After writing to the database,
        update the single-document cache in Redis (write-through).
        """
        valid_fields = self._required_vars.union(self._admissible_vars)
        to_save_fields = valid_fields.union(
            {self._location_var}
        ) if self._location_var else valid_fields

        filter_key = (
            "_id" if "_id" in self._data else
            "name" if "name" in self._data else
            None
        )
        filter_val = self._data.get(filter_key) if filter_key else None

        existing_doc = (
            self._db.find_one({filter_key: filter_val})
            if filter_key else None
        )

        # Location calculation
        if self._location_var and self._location_var.endswith("_loc"):
            base_field = self._location_var[:-4]
            address_value = self._data.get(base_field)
            existing_location = (
                existing_doc.get(self._location_var) if existing_doc else None
            )

            if address_value and (
                not existing_doc
                or base_field in self._modified_vars
                or existing_location is None
            ):
                self._data[self._location_var] = \
                    _address_cache.get(address_value) or \
                    getLocationPoint(address_value)
                self._modified_vars.add(self._location_var)
            elif existing_doc and existing_location is not None:
                self._data[self._location_var] = existing_location

        data_to_save = {
            k: v for k, v in self._data.items()
            if k in to_save_fields or k == "_id"
        }

        if existing_doc:
            data_to_update = {
                k: self._data[k] for k in self._modified_vars
                if k in to_save_fields or k == "_id"
            }
            if data_to_update:
                self._db.update_one(
                    {filter_key: filter_val}, {"$set": data_to_update}
                )
            self._data["_id"] = existing_doc["_id"]
        else:
            res = self._db.insert_one(data_to_save)
            self._data["_id"] = res.inserted_id

        self._modified_vars.clear()

        # Save/refresh single-document cache by _id key after DB write
        if hasattr(self.__class__, "_redis_client") and self._redis_client is not None:
            cache_key = f"{self.__class__.__name__}:byid:{str(self._data['_id'])}"
            self._redis_client.set(
                cache_key,
                json.dumps(self._data, cls=MongoJSONEncoder),
                ex=86400
            )

    def delete(self) -> None:
        """
        Delete the model instance from the database.

        Removes the record with the given unique key.
        """
        filter_key = (
            "_id" if "_id" in self._data else
            "name" if "name" in self._data else
            None
        )
        filter_val = self._data.get(filter_key) if filter_key else None
        if filter_key and filter_val:
            self._db.delete_one({filter_key: filter_val})

    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        """
        Find and return models matching a filter.

        Parameters
        ----------
        filter : dict
            Query conditions.

        Returns
        -------
        ModelCursor
            Cursor over matching Model records.
        Uses Redis caching for repeated queries.
        """
        filter_str = json.dumps(filter, sort_keys=True)
        cache_key = f"{cls.__name__}:find:{hashlib.md5(filter_str.encode()).hexdigest()}"

        if cls._redis_client is not None:
            cached = cls._redis_client.get(cache_key)
            if cached:
                results = json.loads(cached)
                return ModelCursor(model_class=cls, cursor=iter(results))

        db_results = list(cls._db.find(filter))
        # Serialize using custom encoder to handle ObjectId
        if cls._redis_client is not None:
            cls._redis_client.set(
                cache_key, 
                json.dumps(db_results, cls=MongoJSONEncoder), 
                ex=86400
            )
        return ModelCursor(model_class=cls, cursor=iter(db_results))

    @classmethod
    def aggregate(
        cls, pipeline: list[dict]
    ) -> list[dict]:
        """
        Aggregate query with a pipeline.

        Parameters
        ----------
        pipeline : list of dict
            Aggregation pipeline.

        Returns
        -------
        list of dict
            Aggregation results, possibly from cache.
        Uses Redis cache for repeat queries.
        """
        pipe_str = json.dumps(pipeline, sort_keys=True)
        cache_key = f"{cls.__name__}:aggregate:{hashlib.md5(pipe_str.encode()).hexdigest()}"

        # Check Redis cache
        if cls._redis_client is not None:
            cached = cls._redis_client.get(cache_key)
            if cached:
                return json.loads(cached)

        # Perform the aggregation query
        result = list(cls._db.aggregate(pipeline))
        if cls._redis_client is not None:
            cls._redis_client.set(
                cache_key, 
                json.dumps(result, cls=MongoJSONEncoder), 
                ex=86400
            )
        return result

    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        """
        Find record by MongoDB object ID.

        Parameters
        ----------
        id : str
            Object ID as string.

        Returns
        -------
        Model or None
            Instance if found, else None.
        Uses Redis cache for repeat lookups.
        """
        cache_key = f"{cls.__name__}:byid:{str(id)}"

        if cls._redis_client is not None:
            cached = cls._redis_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                return cls(**data)

        # Convert string id to ObjectId if possible
        try:
            object_id = ObjectId(id)
        except Exception:
            object_id = id

        doc = cls._db.find_one({"_id": object_id})
        if doc:
            if cls._redis_client is not None:
                cls._redis_client.set(
                    cache_key,
                    json.dumps(doc, cls=MongoJSONEncoder),
                    ex=86400
                )
            return cls(**doc)
        return None

    @classmethod
    def init_class(
        cls,
        db_collection: pymongo.collection.Collection,
        indexes: dict[str, str],
        required_vars: set[str],
        admissible_vars: set[str]
    ) -> None:
        """
        Initialize class with DB collection and indexes.

        Parameters
        ----------
        db_collection : pymongo.collection.Collection
            MongoDB collection.
        indexes : dict
            Field names to index type.
        required_vars : set of str
            Required attributes.
        admissible_vars : set of str
            Additional allowed attributes.

        Raises
        ------
        ValueError
            On index creation errors or missing location.
        """
        # Initialize class attributes
        cls._db = db_collection
        cls._required_vars = required_vars
        cls._admissible_vars = admissible_vars
        cls._location_var = None
        cls._redis_client = redis.Redis(host='localhost', port=6379, db=0)

        # Set redis configuration
        cls._redis_client.config_set('maxmemory', 150 * 1024 * 1024)  
        cls._redis_client.config_set('maxmemory-policy', 'volatile-ttl')

        for field, idx_type in indexes.items():
            try:
                if idx_type == "unique":
                    cls._db.create_index(
                        [(field, pymongo.ASCENDING)],
                        unique=True
                    )
                elif idx_type == "regular":
                    cls._db.create_index([(field, pymongo.ASCENDING)])
                elif idx_type == "2dsphere":
                    cls._db.create_index([(field, pymongo.GEOSPHERE)])
                    cls._location_var = field
                else:
                    raise ValueError(
                        f"Unknown '{field}': {idx_type}"
                    )
            except Exception as e:
                raise ValueError(
                    f"Error index on field '{field}': {e}"
                )

        if cls._location_var is None:
            raise ValueError(f"_location_var not set")

class ModelCursor:
    """
    Iterator for Model query results.

    Parameters
    ----------
    model_class : Model
        Model class for instantiation.
    cursor : pymongo.cursor.Cursor
        PyMongo cursor.

    Methods
    -------
    alive()
        Is there more results?
    __iter__()
        Iterator over Model instances.
    """

    def __init__(
        self, model_class: Model, cursor: pymongo.cursor.Cursor
    ):
        """
        Initialize ModelCursor.

        Parameters
        ----------
        model_class : Model
            Model type for instantiation.
        cursor : pymongo.cursor.Cursor
            PyMongo cursor.
        """
        self.model = model_class
        self.cursor = cursor
        self._alive = True

    def alive(self) -> bool:
        """
        Returns whether this cursor has more results.

        Returns
        -------
        bool
            True if more results, False otherwise.
        """
        return self._alive

    def __iter__(self) -> Generator:
        """
        Yields Model instances from cursor until exhausted.

        Returns
        -------
        Generator
            Iterator over Model instances.
        """
        def generator():
            while self.alive():
                try:
                    document = next(self.cursor)
                    yield self.model(**document)
                except StopIteration:
                    self._alive = False
        return generator()
