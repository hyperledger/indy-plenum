from abc import ABC, abstractmethod


class GraphStore(ABC):
    def __init__(self, store):
        self.store = store
        self.client = store.client
        self.bootstrap()

    @abstractmethod
    def bootstrap(self):
        pass

    @abstractmethod
    def addEdgeConstraint(self, edgeClass, iN=None, out=None):
        pass

    @abstractmethod
    def createEntity(self, createCmd, **kwargs):
        pass

    @abstractmethod
    def createVertex(self, name, **kwargs):
        pass

    @abstractmethod
    def createEdge(self, name, frm, to, **kwargs):
        pass
