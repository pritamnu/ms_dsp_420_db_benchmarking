"""
Author: pritamchatterjee2023@u.northwestern.edu
Database benchmarking tool
"""
import random
import time
import pandas as pd

from neo4j import GraphDatabase

from stats_collector import collect_stats as collect_stats


class AbstractDB:
    def test_create_db(self, *args, **kwargs):
        """
        Test Steps:
        1. Create empty database
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def test_create_tables(self, *args, **kwargs):
        """
        Test Steps:
        1. Create all required tables/schema/model as per requirement
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def test_load_data(self, *args, **kwargs):
        """
        Test Step:
        1. Load all the data from csv file to tables in DB
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def test_list_tactics(self, *args, **kwargs):
        """
        Test Steps:
        1. List all the tactics in sorted order of their IDs
        2. Display the description details of each tacticsd
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplemented

    def test_techniques_for_tactics(self, *args, **kwargs):
        """
        Test Steps:
        1. List all the techniques with their description for a given tactics
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplemented

    def test_find_mitigations_for_technique(self, *args, **kwargs):
        """
        Test Steps:
        1. List all mitigation steps for a
        :param args:
        :param kwargs:
        :return:
        """


class BaseDB(AbstractDB):
    def __init__(self):
        self.xls_data_files = {
            "techniques": "data/techniques.xlsx",  # Unique IDs
            "mitigations": "data/mitigations.xlsx", # Dup IDs
            "stages": "data/tactics.xlsx", # Unique IDs
            "groups": "data/groups.xlsx",  # Dup IDs
            "softwares": "data/softwares.xlsx",  # Dup IDs
            "techniques_mitigations": "data/techniques_mitigations.xlsx",
            "groups_techniques": "data/groups_techniques.xlsx"
        }

        self._csv_data_files = {
            "stages": "data/stages.csv",
            "techniques": "data/techniques.csv",
            "sub_techniques": "data/sub_techniques.csv",
            "mitigations": "data/mitigations.csv",
            "techniques_mitigations": "data/techniques_mitigations.csv",
            "platforms": "data/platforms.csv",
            "groups": "data/groups.csv",
            "softwares": "data/softwares.csv",
            "groups_techniques": "data/groups_techniques.csv"
        }

    def _process_data_files(self):
        self._process_stages()
        self._process_techniques()
        self._process_mitigations()
        self._process_techniques_mitigations()
        self._process_groups()
        self._process_software()
        self._process_groups_techniques()

    def _process_mitigations(self):
        df = pd.read_excel(self.xls_data_files["mitigations"])
        df.to_csv(self._csv_data_files['mitigations'], index=False)

    def _process_techniques_mitigations(self):
        df = pd.read_excel(self.xls_data_files["techniques_mitigations"])
        df.to_csv(self._csv_data_files['techniques_mitigations'], index=False)
    def _process_stages(self):
        df = pd.read_excel(self.xls_data_files["stages"])
        df.to_csv(self._csv_data_files['stages'], index=False)

    def _process_techniques(self):
        df = pd.read_excel(self.xls_data_files["techniques"])
        df[df.is_sub_technique==False].to_csv(self._csv_data_files['techniques'], index=False)
        df[df.is_sub_technique==True].to_csv(self._csv_data_files['sub_techniques'], index=False)

    def _process_groups(self):
        df = pd.read_excel(self.xls_data_files["groups"])
        df.to_csv(self._csv_data_files['groups'], index=False)

    def _process_software(self):
        df = pd.read_excel(self.xls_data_files["softwares"])
        df.to_csv(self._csv_data_files['softwares'], index=False)

    def _process_groups_techniques(self):
        df = pd.read_excel(self.xls_data_files["groups_techniques"])
        df.to_csv(self._csv_data_files['groups_techniques'], index=False)

class Neo4j(BaseDB):
    """
    class for benchmarking neo4j database
    """
    def __init__(self, cleanup=True):
        super(Neo4j, self).__init__()
        self.process_name = "neo4j"
        self.uri = "neo4j://localhost:7687"
        self.username = "neo4j"
        self.password = "beauty-textile-prime-organic-dragon-4081"
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        # self.driver = GraphDatabase.driver(self.uri, auth=None)
        if cleanup:
            self._clean_db()
        self._prepare_data()

    def _clean_db(self):
        query = """MATCH (n)
        DETACH DELETE n
        """
        # self.driver.verify_connectivity()
        print("Cleaning up data...")
        self.driver.execute_query(query)

    def _get_connection(self):
        print("Getting connection")
        self.driver.verify_connectivity()
        return self.driver

    def _close_connection(self):
        # Don't forget to close the driver connection when you are finished
        # with it
        print("Closing connection")
        self.driver.close()

    def _prepare_data(self):
        """
        Prepare the data for loading into the Neo4j DB
        :return:
        """
        print("Transforming and preparing data...")
        self._process_data_files()

    @collect_stats
    def test_dummy(self):
        time.sleep(10)

    # @collect_stats
    def test_create_db(self):
        """
        Method to create DB. Not benchmarked.
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    # @collect_stats
    def test_create_tables(self):
        """
        Method to create tables in the DB. Not benchmarked.
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_load_data(self):
        """
        Test for loading all the data into database
        technique-[:IS_ASSOCIATED_WITH]->tactics
        sub_technique-[:SUB_TECHNIQUE_OF]->technique
        mitigation-[:MITIGATES]->technique
        mitigation-[:MITIGATES]->sub_technique
        group-[:USES]->technique
        group-[:USES]->sub_technique
        group-[:USES]->software
        :return:
        """
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///stages.csv' AS row
        CREATE (tac:tactics {id: row.ID, name:row.name});
        """

        # self._get_connection()
        print("Loading Stages..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        LOAD CSV WITH HEADERS FROM 'file:///techniques.csv' AS row
        CREATE (tec:technique {id: row.ID, name: row.name, desc: row.description, url: row.url, tactics: row.tactis})
        WITH tec, row
        UNWIND split(row.tactics, ',') AS each_tactics
        MATCH (tac:tactics {name: each_tactics})
        MERGE (tec)-[:IS_ASSOCIATED_TO]->(tac);
        """
        print("Loading Techniques..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        LOAD CSV WITH HEADERS FROM 'file:///sub_techniques.csv' AS row
        CREATE (stec:sub_technique {id: row.ID, name: row.name, desc: row.description, url: row.url, tactics: row.tactis, parent: row.parent_technique})
        WITH stec, row
        MATCH (tec:technique {id: row.parent_technique})
        MERGE (stec)-[:SUB_TECHNIQUE_OF]->(tec);
        """
        print("Loading Sub Techniques..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        LOAD CSV WITH HEADERS FROM 'file:///mitigations.csv' AS row
        WITH row
        CREATE (mi:mitigation {id: row.ID, name: row.name, desc: row.description});
        """
        print("Loading Mitigations..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        // load mitigation relations with techniques
        LOAD CSV WITH HEADERS FROM 'file:///techniques_mitigations.csv' AS row
        WITH row
        MATCH (mi {id: row.ID})
        MATCH (tec {id: row.target_ID})
        MERGE (mi)-[:MITIGATES]->(tec)
        """
        print("Adding Techniques and Mitigations relationships..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        // Load groups
        LOAD CSV WITH HEADERS FROM 'file:///groups.csv' AS row
        WITH row
        CREATE (gr:group {id: row.ID, name: row.name, desc: row.description, associated_groups: row.associated_groups});
        """
        print("Loading Groups..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        // Load softwares
        LOAD CSV WITH HEADERS FROM 'file:///softwares.csv' AS row
        with row
        MERGE (sw:software {id: row.target_ID, name: row.target_name})
        MERGE (gr:group {id: row.ID, name: row.name})
        MERGE (gr)-[:USES]->(sw)
                """
        print("Loading Softwares..")
        print(self.driver.execute_query(query).summary.__dict__)

        query = """
        LOAD CSV WITH HEADERS FROM 'file:///groups_techniques.csv' AS row
        with row
        MATCH (gr:group {id: row.ID})
        MATCH (tec {id: row.target_ID})
        MERGE (gr)-[:USES]->(tec);
        """
        print("Loading Groups and Techniques relationships..")
        print(self.driver.execute_query(query).summary.__dict__)
        # self._close_connection()

    @collect_stats
    def test_random_read_level1(self, techniques_count=175):
        """
        Performs a query to find the groups that use the given technique and its sub-technique.
        :return:
        """
        df = pd.read_csv(self._csv_data_files['techniques'])

        techniques = sorted(df.ID.values)[:techniques_count]
        for technique in techniques:
            query = """
            MATCH (group)-[:USES]->(tec:technique {id: $id})
            OPTIONAL MATCH (stec:sub_technique)-[:SUB_TECHNIQUE_OF]->(tec)
            OPTIONAL MATCH (group)-[:USES]->(stec)
            RETURN group, tec, stec    
            """
            print(self.driver.execute_query(query, id=technique).summary.__dict__)


    @collect_stats
    def test_random_read_level2(self, techniques_count=175):
        """
        Performs a query to find the software that used by the groups to perform the given
        technique and its sub-technique.
        :return:
        """
        df = pd.read_csv(self._csv_data_files['techniques'])

        techniques = sorted(df.ID.values)[:techniques_count]
        for technique in techniques:
            query = """
            MATCH (group)-[:USES]->(tec:technique {id: $id})
            OPTIONAL MATCH (stec:sub_technique)-[:SUB_TECHNIQUE_OF]->(tec)
            OPTIONAL MATCH (group)-[:USES]->(stec)
            MATCH (group)-[:USES]->(sw:software)
            RETURN group, tec, stec, sw
                """
            print(self.driver.execute_query(query, id=technique).summary.__dict__)

    @collect_stats
    def test_complex_aggregate_query(self, techniques_count=175):
        """
        1. We first match groups using techniques and their sub-techniques.
        2. Then, we match software used by those groups and optionally match mitigations applied to techniques.
        3. Using the WITH clause, we aggregate the counts of groups, software, and mitigations for each technique and sub-technique.
        4. Finally, we return the technique name, sub-technique name, counts of groups and software, and the average count of mitigations applied to each technique and sub-technique.
        :param techniques_count:
        :return:
        """
        query = """
MATCH (group)-[:USES]->(technique)
OPTIONAL MATCH (stec:sub_technique)-[:SUB_TECHNIQUE_OF]->(technique)
OPTIONAL MATCH (group)-[:USES]->(stec)
OPTIONAL MATCH (group)-[:USES]->(sw:software)
OPTIONAL MATCH (mitigation)-[:MITIGATES]->(technique)
WITH technique, stec, sw, COUNT(DISTINCT group) AS groupCount, COUNT(DISTINCT sw) AS softwareCount, COUNT(DISTINCT mitigation) AS mitigationCount
RETURN technique.name AS technique, 
       stec.name AS sub_technique, 
       groupCount AS GroupCount, 
       softwareCount AS SoftwareCount, 
       AVG(mitigationCount) AS AvgMitigations
ORDER BY technique, sub_technique
        """
        print(self.driver.execute_query(query).summary.__dict__)


    @collect_stats
    def test_recursive_query(self):
        """
        1. traverse the hierarchical relationship between techniques and
           sub-techniques using variable-length relationships
        2. find the groups using each technique and sub-technique
        3. aggregate the counts of groups for each technique and sub-technique
        4. we return the technique name, sub-technique name,
           and the count of groups using each technique/sub-technique
        :return:
        """
        query = """
MATCH (technique)-[:SUB_TECHNIQUE_OF*0..]->(sub_technique)
OPTIONAL MATCH (group)-[:USES]->(technique)
OPTIONAL MATCH (group)-[:USES]->(sub_technique)
WITH technique, sub_technique, COUNT(DISTINCT group) AS groupCount
RETURN technique.name AS Technique, 
       sub_technique.name AS SubTechnique, 
       groupCount AS GroupCount
ORDER BY Technique, SubTechnique
        """
        print(self.driver.execute_query(query).summary.__dict__)


class Postgres(BaseDB):
    """
    Class for Postgres DB benchmarking
    """
    def __init__(self, cleanup=True):
        super(Postgres, self).__init__()
        self.process_name = "neo4j"
        self.uri = "neo4j://localhost:7687"
        self.username = "neo4j"
        self.password = "beauty-textile-prime-organic-dragon-4081"
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        # self.driver = GraphDatabase.driver(self.uri, auth=None)
        if cleanup:
            self._clean_db()
        self._prepare_data()

    def _clean_db(self):
        query = """MATCH (n)
        DETACH DELETE n
        """
        # self.driver.verify_connectivity()
        print("Cleaning up data...")
        self.driver.execute_query(query)

    def _get_connection(self):
        print("Getting connection")
        self.driver.verify_connectivity()
        return self.driver

    def _close_connection(self):
        # Don't forget to close the driver connection when you are finished
        # with it
        print("Closing connection")
        self.driver.close()

    def _prepare_data(self):
        """
        Prepare the data for loading into the Neo4j DB
        :return:
        """
        print("Transforming and preparing data...")
        self._process_data_files()

    @collect_stats
    def test_dummy(self):
        time.sleep(10)

    # @collect_stats
    def test_create_db(self):
        """
        Method to create DB. Not benchmarked.
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    # @collect_stats
    def test_create_tables(self):
        """
        Method to create tables in the DB. Not benchmarked.
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_load_data(self):
        """
        Test for loading all the data into database
        :return:
        """
        time.sleep(random.choice(range(1, 20)))


    @collect_stats
    def test_list_tactics(self):
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_techniques_for_tactics(self):
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_find_mitigations_for_technique(self):
        time.sleep(random.choice(range(1, 20)))


class MongoDB(BaseDB):
    """
    Class for Postgres DB benchmarking
    """
    def __init__(self, cleanup=True):
        super(MongoDB, self).__init__()
        self.process_name = "neo4j"
        self.uri = "neo4j://localhost:7687"
        self.username = "neo4j"
        self.password = "beauty-textile-prime-organic-dragon-4081"
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        # self.driver = GraphDatabase.driver(self.uri, auth=None)
        if cleanup:
            self._clean_db()
        self._prepare_data()

    def _clean_db(self):
        query = """MATCH (n)
        DETACH DELETE n
        """
        # self.driver.verify_connectivity()
        print("Cleaning up data...")
        self.driver.execute_query(query)

    def _get_connection(self):
        print("Getting connection")
        self.driver.verify_connectivity()
        return self.driver

    def _close_connection(self):
        # Don't forget to close the driver connection when you are finished
        # with it
        print("Closing connection")
        self.driver.close()

    def _prepare_data(self):
        """
        Prepare the data for loading into the Neo4j DB
        :return:
        """
        print("Transforming and preparing data...")
        self._process_data_files()

    @collect_stats
    def test_dummy(self):
        time.sleep(10)

    # @collect_stats
    def test_create_db(self):
        """
        Method to create DB. Not benchmarked.
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    # @collect_stats
    def test_create_tables(self):
        """
        Method to create tables in the DB. Not benchmarked.
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_load_data(self):
        """
        Test for loading all the data into database
        :return:
        """
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_list_tactics(self):
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_techniques_for_tactics(self):
        time.sleep(random.choice(range(1, 20)))

    @collect_stats
    def test_find_mitigations_for_technique(self):
        time.sleep(random.choice(range(1, 20)))


# unit tests
def main():
    # loading databases
    # NOTE: select the DB class you are working on
    databases = [Neo4j]  #Postgres, Neo4j, MongoDB]

    # loading tests
    # NOTE: Add the tests in the list that you want to be executed
    tests = ["test_dummy", "test_load_data", "test_random_read_level1",
             "test_random_read_level2", "test_recursive_query", "test_complex_aggregate_query"]
    for db in databases:
        db_obj = db()
        for test in tests:
            print("-" * 50)
            print("Running test: {}".format(test))
            print("-" * 50)
            getattr(db_obj, test)()


    # NOTE: Final report will be provided per DB per scenario in the reports directory

if __name__ == '__main__':
    main()
