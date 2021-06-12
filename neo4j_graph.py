from py2neo import Graph, NodeMatcher


class Neo4jGraph:
    uri = "neo4j+s://a297e92a.databases.neo4j.io"
    user = "neo4j"
    password = "X5POvChKdgbbIHkCf6U0PubneCFF6dQM4Q3hh_c14zs"

    def __init__(self):
        self.graph = Graph(self.uri, auth=(self.user, self.password))
        self.matcher = NodeMatcher(self.graph)

    def clear_graph(self):
        self.graph.delete_all()
