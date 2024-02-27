How to use
-----------------------
1. Create a virtual environment in python.
2. Run pip install requirements.txt inside the virtual env.
3. Run main.py

Use the below command to create a docker container
for Neo4j
-----------------------
docker run --restart always --publish=7474:7474 --publish=7687:7687 -v C:\Users\Pritam\PycharmProjects\db_benchmarking\data:/import --env NEO4J_AUTH=neo4j/beauty-textile-prime-organic-dragon-4081 neo4j:5.17.0