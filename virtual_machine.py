from pandas import ExcelFile, read_excel
from py2neo import Node, Relationship
from neo4j_graph import Neo4jGraph
from common import load_directory
from tqdm import tqdm


def load_virtual_server(neo4j: Neo4jGraph):
    LOCATED = Relationship.type("LOCATED")
    HOST_OS = Relationship.type("HOST_OS")
    AGGREGATE = Relationship.type("AGGREGATE")

    xlsx = ExcelFile("asset/virtual_server.xlsx")
    df = read_excel(xlsx, sheet_name="Виртуальные машины Банк")

    locations = df["Офис"].drop_duplicates()
    load_directory(locations, "Загрузка площадок", "Location", neo4j)

    hostOSs = df["Операционная система"].drop_duplicates()
    load_directory(hostOSs, "Загрузка операционных систем", "SystemSoftware", neo4j)

    with tqdm(total=df.shape[0], desc="Загрузка виртуальных машин") as progress:
        for index, item in df.iterrows():
            node = Node("Node",
                        name=item["Name"],
                        ip=item["IP"],
                        RAM=item["RAM"],
                        description=item["Назначение сервера"]
                        )
            neo4j.graph.create(node)

            node_location = neo4j.matcher.match("Location", name=item["Офис"]).first()
            neo4j.graph.merge(LOCATED(node, node_location))

            node_sys_soft = neo4j.matcher.match("SystemSoftware", name=item["Операционная система"]).first()
            neo4j.graph.merge(HOST_OS(node, node_sys_soft))

            node_cluster = neo4j.matcher.match("TechCollaboration", name=item["Платформа"]).first()
            if node_cluster is None:
                node_hardware = neo4j.matcher.match("Hardware", name=item["Платформа"]).first()
                if not (node_hardware is None):
                    neo4j.graph.merge(AGGREGATE(node, node_hardware))
            else:
                neo4j.graph.merge(AGGREGATE(node, node_cluster))

            progress.update(1)