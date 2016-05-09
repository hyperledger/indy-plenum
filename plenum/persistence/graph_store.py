from abc import ABC, abstractmethod


class GraphStore(ABC):
    """
    Interface for graph databases to be used with the Plenum system.
    """
    def __init__(self, store):
        self.store = store
        self.client = store.client
        self.bootstrap()

    @abstractmethod
    def bootstrap(self):
        """
        Setup the database, create the schema etc.
        """
        pass

    @abstractmethod
    def addEdgeConstraint(self, edgeClass, iN=None, out=None):
        """
        Create a constraint on an edge in the graph database

        :param edgeClass: name of the edge type
        """
        pass

    @abstractmethod
    def createVertex(self, name, **kwargs):
        """
        Create a vertex/node in the graph

        :param name: name of the vertex
        """
        pass

    @abstractmethod
    def createEdge(self, name, frm, to, **kwargs):
        """
        Create an edge in the graph

        :param name: name of the edge
        :param frm: from vertex
        :param to: to vertex
        """
        pass
