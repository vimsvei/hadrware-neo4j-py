from neo4j_graph import Neo4jGraph
from numpy import NaN
from py2neo import Node
from tqdm import tqdm


def split_name(str):
    split = str.rsplit("-n")
    if split[0] == str:
        return NaN
    return split[0]


def load_directory(source, title, label, neo4j: Neo4jGraph) -> object:
    with tqdm(total=source.shape[0], desc=title) as progress:
        for index, item in source.items():
            old_node = neo4j.matcher.match(label, name=item).first()
            if old_node is None:
                node = Node(label, name=item)
                neo4j.graph.create(node)
            progress.update(1)
