from py2neo import neo4j
from py2neo import node, rel
from py2neo import cypher
import csv
import json
import re

graph_db = neo4j.GraphDatabaseService()
session = cypher.Session("http://localhost:7474")

try: 
	print(graph_db.neo4j_version)
except:
	print "Error, could not connect to database, I think."

# csvfile = open("ig_test.csv", 'r')
# jsonfile = open("ig_test.json", "w")

# try:
# 	fieldnames = ("name","abbrev","type","addresses")
# 	reader = csv.DictReader(csvfile, fieldnames)
# 	for row in reader:
# 		json.dump(row, jsonfile)
# 		jsonfile.write('\n')
# except Exception, e:
# 	print "Could not open file or parse into json" + str(e)

#initialize index, types for labels
index = 0
types= []
#GET NODES
try:
    nodes = ()
    with open("nodes.csv", mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[0] != 'name':
                nodes = nodes + ({'name':row[0], 'abbrev':row[3], 'type':row[1], "description":row[2], "node_id":index },)
                types.append(row[1])
                index = index + 1
    types = list(set(types))
except Exception, e:
    print "Could not convert to dictionary: " + str(e)
#GET LIST OF ISSUES
try: 
    issues = ()
    with open("issues.csv", mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[0] != 'name':
                issues = issues + ({'name':row[0], "type":row[1], "description":row[2], "node_id":index},)
                index = index + 1
except Exception, e:
    print "Could not get issues: " + str(e)
#GET RELATIONSHIPS
try:
    relationships = ()
    with open("relationships.csv", mode='r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if row[0]!='name1':
                relationships = relationships + ({'node1':row[0], 'relationship':row[1], 'node2':row[2]}, )
except Exception, e:
    print "Could not get relationships: " + str(e)
#clear DB
graph_db.clear()

#MAKE A LIST OF INDECES
indeces = {}
indeces['Issue'] = graph_db.get_or_create_index(neo4j.Node, 'Issue')
for type_ in types:
    indeces[type_] = graph_db.get_or_create_index(neo4j.Node, type_)

batch = neo4j.WriteBatch(graph_db)

#CREATE ISSUE NODES
for i in issues:
    n = batch.create(node(node_id=i['node_id'], name=i['name'], description=i['description'], type="Issue"))
    batch.add_labels(n, 'Issue')
    batch.add_indexed_node(indeces['Issue'], 'node_id', index, n)

#CREATE NODES
for nod in nodes:
    n = batch.create(node(node_id=nod['node_id'], name=nod['name'].replace('"', ""), abbrev=nod['abbrev'], description=nod['description'], type=nod['type']))
    batch.add_labels(n, nod['type'])
    batch.add_indexed_node(indeces[nod['type']],'node_id', nod['node_id'], n)

nodes = batch.submit()

#NEED TO CREATE QUERY TO ADD LINKS TO ISSUES
for r in relationships:
    query = "START n=node(*), m=node(*) "
    query += "WHERE n.name=\"{0}\" AND m.name=\"{1}\" ".format(r['node1'].replace('"', ""), r['node2'].replace('"', ""))
    query += "CREATE (n)-[:{0}]->(m)" .format(r['relationship'])
    neo4j.CypherQuery(graph_db, query).execute()







