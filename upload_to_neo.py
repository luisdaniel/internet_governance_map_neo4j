from py2neo import neo4j
from py2neo import node, rel
from py2neo import cypher
import csv
import json
import re

graph = neo4j.GraphDatabaseService()
session = cypher.Session("http://localhost:7474")

try: 
	print(graph.neo4j_version)
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
				nodes = nodes + ({'name':row[0], 'abbrev':row[1], 'type':row[2], "node_id":index },)
				types.append(row[2])
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
				issues = issues + ({'name':row[0], "type":row[1], "node_id":index},)
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

print relationships

#clear DB
graph.clear()

#MAKE A LIST OF INDECES
indeces = {}
indeces['Issue'] = graph.get_or_create_index(neo4j.Node, 'Issue')
for type_ in types:
	indeces[type_] = graph.get_or_create_index(neo4j.Node, type_)

batch = neo4j.WriteBatch(graph)

#CREATE ISSUE NODES
for i in issues:
	n = batch.create(node(node_id=i['node_id'], name=i['name'], type="Issue"))
	batch.add_labels(n, 'Issue')
	batch.add_indexed_node(indeces['Issue'], 'node_id', index, n)

#CREATE NODES
for nod in nodes:
	n = batch.create(node(node_id=nod['node_id'], name=nod['name'].replace('"', ""), abbrev=nod['abbrev'], type=nod['type']))
	batch.add_labels(n, nod['type'])
	batch.add_indexed_node(indeces[nod['type']],'node_id', nod['node_id'], n)

nodes = batch.submit()

#NEED TO CREATE QUERY TO ADD LINKS TO ISSUES
for r in relationships:
	query = "START n=node(*), m=node(*) "
	query += "WHERE n.name=\"{0}\" AND m.name=\"{1}\" ".format(r['node1'].replace('"', ""), r['node2'].replace('"', ""))
	query += "CREATE (n)-[:{0}]->(m)" .format(r['relationship'])
	neo4j.CypherQuery(graph, query).execute()


# indeces = {'Child Pornography': Index(Node, u'http://localhost:7474/db/data/index/node/Child%20Pornography'), 
			# 'Tools & Resources': Index(Node, u'http://localhost:7474/db/data/index/node/Tools%20%26%20Resources'), 
			# 'Research & Advocacy': Index(Node, u'http://localhost:7474/db/data/index/node/Research%20%26%20Advocacy'), 
			# 'Law & Policy': Index(Node, u'http://localhost:7474/db/data/index/node/Law%20%26%20Policy'), 
			# 'Actor': Index(Node, u'http://localhost:7474/db/data/index/node/Actor'), 
			# 'Standards': Index(Node, u'http://localhost:7474/db/data/index/node/Standards'), 
			# 'Initiative & Events': Index(Node, u'http://localhost:7474/db/data/index/node/Initiative%20%26%20Events'), 
			# 'Person': Index(Node, u'http://localhost:7474/db/data/index/node/Person'), 
			# 'Question': Index(Node, u'http://localhost:7474/db/data/index/node/Question'), 
			# 'IPv6': Index(Node, u'http://localhost:7474/db/data/index/node/IPv6'), 
			# 'Laws & Policies': Index(Node, u'http://localhost:7474/db/data/index/node/Laws%20%26%20Policies'), 
			# 'Initiatives and Events': Index(Node, u'http://localhost:7474/db/data/index/node/Initiatives%20and%20Events'),
			# 'Issue': Index(Node, u'http://localhost:7474/db/data/index/node/Issue')}

#TEST QUERY
tx = session.create_transaction()
tx.append("MATCH (n)-[:ADDRESSES]->(m) WHERE m.name=\"Child Pornography\" RETURN n")
results = tx.execute()
for r in results[0]:
	print r.values[0]['name'], r.values[0]['type'], r.values[0]['node_id']






