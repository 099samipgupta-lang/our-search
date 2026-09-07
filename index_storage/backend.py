from abc import ABC, abstractmethod


class IndexStorageBackend(ABC):

    @abstractmethod
    def put(self, key, data):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError

    @abstractmethod
    def exists(self, key):
        raise NotImplementedError

    @abstractmethod
    def delete(self, key):
        raise NotImplementedError

    @abstractmethod
    def list_keys(self, prefix=""):
        raise NotImplementedError
