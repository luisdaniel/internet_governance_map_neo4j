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

csvfile = open("ig_test.csv", 'r')
jsonfile = open("ig_test.json", "w")

try:
	fieldnames = ("name","abbrev","type","addresses")
	reader = csv.DictReader(csvfile, fieldnames)
	for row in reader:
		json.dump(row, jsonfile)
		jsonfile.write('\n')
except Exception, e:
	print "Could not open file or parse into json" + str(e)

index = 0
try:
	ig = ()
	with open("ig_test.csv", mode='r') as infile:
		reader = csv.reader(infile)
		
		issues = []
		types= []
		for row in reader:
			if row[0] != 'name':
				ig = ig + ({'name':row[0], 'abbrev':row[1], 'type':row[2], 'addresses':row[3], "node_id":index },)
				issues.append(row[3])
				types.append(row[2])
				index = index + 1
	issues = list(set(issues))
	types = list(set(types))
	#print ig
	# print issues
	#print types
except Exception, e:
	print "Could not convert to dictionary: " + str(e)

#clear DB
graph.clear()

#Make a list of indeces
indeces = {}
indeces['Issue'] = graph.get_or_create_index(neo4j.Node, 'Issue')
for type_ in types:
	indeces[type_] = graph.get_or_create_index(neo4j.Node, type_)
# print indeces

#make a list of 

# issues = ['Child Pornography', 'IPv6']
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

#create nodes
batch = neo4j.WriteBatch(graph)
for r in ig:
	n = batch.create(node(node_id=r['node_id'], name=r['name'].replace('"', ""), abbrev=r['abbrev'], type=r['type']))
	batch.add_labels(n, r['type'])
	batch.add_indexed_node(indeces[r['type']],'node_id', r['node_id'], n)

#NEED TO ADD ISSUES AS NODES
for i in issues:
	index = index + 1
	n = batch.create(node(node_id=index, name=i))
	batch.add_labels(n, 'Issue')
	batch.add_indexed_node(indeces['Issue'], 'node_id', index, n)
nodes = batch.submit()

# #NEED TO CREATE QUERY TO ADD LINKS TO ISSUES
for i in ig:
	query = "START n=node(*), issue=node(*) "
	query += "WHERE n.name=\"{0}\" AND issue.name=\"{1}\" ".format(i['name'].replace('"', ""), i['addresses'].replace('"', ""))
	query += "CREATE (n)-[:ADDRESSES]->(issue)" 
	neo4j.CypherQuery(graph, query).execute()

# cp, = graph.create(node(name="Child Pornography"))
# cp.add_labels("Issue")
# for i in ig:
# 	node1, = graph.create(node(name=i['name']))
# 	node1.add_labels(i['type'])
# 	graph.create(rel(node1, "ADDRESSES", cp))
# 	# graph.create(rel(cp, "ADDRESSED_BY", node1))
