How to use
-----------------------
1. Create a virtual environment in python.
2. Activate the virtual env
3. Run pip install requirements.txt inside the virtual env.
4. Run main.py

Pre-requisites
-----------------------
1. Databases need to be benchmarked one by one.
2. Databases need to be running in containers.
3. While one database benchmarking is being done, other database containers should be stopped.

Configuring Neo4j container
-----------------------
docker run --restart always --publish=7474:7474 --publish=7687:7687 -v C:\Users\Pritam\PycharmProjects\db_benchmarking\data:/import --env NEO4J_AUTH=neo4j/beauty-textile-prime-organic-dragon-4081 neo4j:5.17.0