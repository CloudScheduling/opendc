from bson.objectid import ObjectId
from werkzeug.exceptions import NotFound

from opendc.exts import db


class Model:
    """Base class for all models."""

    collection_name = '<specified in subclasses>'

    @classmethod
    def from_id(cls, _id):
        """Fetches the document with given ID from the collection."""
        if isinstance(_id, str) and len(_id) == 24:
            _id = ObjectId(_id)

        return cls(db.fetch_one({'_id': _id}, cls.collection_name))

    @classmethod
    def get_all(cls):
        """Fetches all documents from the collection."""
        return cls(db.fetch_all({}, cls.collection_name))

    def __init__(self, obj):
        self.obj = obj

    def get_id(self):
        """Returns the ID of the enclosed object."""
        return self.obj['_id']

    def check_exists(self):
        """Raises an error if the enclosed object does not exist."""
        if self.obj is None:
            raise NotFound('Entity not found.')

    def set_property(self, key, value):
        """Sets the given property on the enclosed object, with support for simple nested access."""
        if '.' in key:
            keys = key.split('.')
            self.obj[keys[0]][keys[1]] = value
        else:
            self.obj[key] = value

    def insert(self):
        """Inserts the enclosed object and generates a UUID for it."""
        self.obj['_id'] = ObjectId()
        db.insert(self.obj, self.collection_name)

    def update(self):
        """Updates the enclosed object and updates the internal reference to the newly inserted object."""
        db.update(self.get_id(), self.obj, self.collection_name)

    def delete(self):
        """Deletes the enclosed object in the database, if it existed."""
        if self.obj is None:
            return None

        old_object = self.obj.copy()
        db.delete_one({'_id': self.get_id()}, self.collection_name)
        return old_object
