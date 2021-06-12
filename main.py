from neo4j_graph import Neo4jGraph
from hardware import load_hardware
from virtual_machine import load_virtual_server


if __name__ == "__main__":

    neo4j = Neo4jGraph()

    neo4j.clear_graph()

    load_hardware(neo4j)

    load_virtual_server(neo4j)
