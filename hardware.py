from py2neo import Graph, Node, NodeMatcher, Relationship
from pandas import ExcelFile, read_excel, ExcelWriter
from common import split_name, load_directory
from neo4j_graph import Neo4jGraph
from numpy import NaN, isnan
from tqdm import tqdm


def load_hardware(neo4j: Neo4jGraph):
    LOCATED = Relationship.type("LOCATED")
    HOST_OS = Relationship.type("HOST_OS")
    ASSIGNMENT = Relationship.type("ASSIGNMENT")
    AGGREGATE = Relationship.type("AGGREGATE")
    CPU_MODEL = Relationship.type("CPU_MODEL")

    xlsx = ExcelFile("asset/hardware.xlsx")
    df = read_excel(xlsx, sheet_name="Servers")
    df["ClusterName"] = df["Name"].apply(split_name)

    locations = df["Офис, ЦОД"].drop_duplicates()
    load_directory(locations, "Загрузка площадок", "Location", neo4j)

    hostOSs = df["Host OS"].drop_duplicates()
    load_directory(hostOSs,"Загрузка операционных систем", "SystemSoftware", neo4j)

    external_companies = df["Производитель"].drop_duplicates()
    load_directory(external_companies, "Загрузка производителей оборудования", "ExternalCompany", neo4j)

    groups = df["Описание"].drop_duplicates()
    load_directory(groups, "Загрузка групп оборудования", "Group", neo4j)

    clusters = df["ClusterName"].drop_duplicates().dropna(how='all')
    load_directory(clusters, "Загрузка кластеров", "TechCollaboration", neo4j)

    cpus = df["CPU"].drop_duplicates()
    load_directory(cpus, "Загрузка моделей CPU", "CPU", neo4j)

    group_clusters = df[["ClusterName", "Описание"]].drop_duplicates().dropna()
    with tqdm(total=group_clusters.shape[0], desc="Установка связей кластеров и групп оборудования") as progress:
        for index, item in group_clusters.iterrows():
            node_cluster = neo4j.matcher.match("TechCollaboration", name=item["ClusterName"]).first()
            node_group = neo4j.matcher.match("Group", name=item["Описание"]).first()
            neo4j.graph.merge(ASSIGNMENT(node_cluster, node_group))
            progress.update(1)

    with tqdm(total=df.shape[0], desc="Загрузка железных серверов") as progress:
        for index, item in df.iterrows():
            node = Node("Hardware",
                        name=item["Name"],
                        modelCPU=item["CPU"],
                        countCPU=item["CPU.1"],
                        RAM=item["RAM"],
                        ip=item["Server IP"])
            neo4j.graph.create(node)

            node_location = neo4j.matcher.match("Location", name=item["Офис, ЦОД"]).first()
            neo4j.graph.merge(LOCATED(node, node_location))

            node_sys_soft = neo4j.matcher.match("SystemSoftware", name=item["Host OS"]).first()
            neo4j.graph.merge(HOST_OS(node, node_sys_soft))

            node_group = neo4j.matcher.match("Group", name=item["Описание"]).first()
            neo4j.graph.merge(ASSIGNMENT(node, node_group))

            node_company = neo4j.matcher.match("ExternalCompany", name=item["Производитель"]).first()
            neo4j.graph.merge(ASSIGNMENT(node, node_company))

            node_cluster = neo4j.matcher.match("TechCollaboration", name=item["ClusterName"]).first()
            if not (node_cluster is None):
                neo4j.graph.merge(AGGREGATE(node, node_cluster))

            node_cpu_model = neo4j.matcher.match("CPU", name=item["CPU"]).first()
            neo4j.graph.merge(CPU_MODEL(node, node_cpu_model,))

            progress.update(1)
