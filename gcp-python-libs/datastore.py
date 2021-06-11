from google.cloud import datastore


class DatastoreClient:
    """Datastore client for common datastore operations"""

    def __init__(self, project_id, namespace):
        self.client = datastore.Client(project=project_id, namespace=namespace)

    def query(self, kind, filter=None):
        query = self.client.query(kind=kind)
        if filter:
            query.add_filter(*filter)
        result = list(query.fetch())
        return result

    def create_or_update_entity_from_dict(self, kind, entity_dict, key_value=None):
        #replaces an existing entity is key_value of it is passed, else creates a new one
        if key_value:
            key = self.client.key(kind, key_value)
        else:
            key = self.client.key(kind)
        entity = datastore.Entity(key=key)
        for key, value in entity_dict.items():
            entity[key] = value
            self.client.put(entity)

    def delete(self, entity):
        self.client.delete(entity.key)

    def update(self, entity):
        self.client.put(entity)
