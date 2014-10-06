
from py2neo import neo4j
from py2neo import node, rel
from py2neo import cypher

graph = neo4j.GraphDatabaseService()
session = cypher.Session("http://localhost:7474")
tx = session.create_transaction()

try: 
	print(graph.neo4j_version)
except:
	print "Error, could not connect to database, I think."

graph.clear()

actor = graph.get_or_create_index(neo4j.Node, 'Actor')
character = graph.get_or_create_index(neo4j.Node, 'Character')
place = graph.get_or_create_index(neo4j.Node, 'Place')

records = [
		{"type":"Actor", "name":"Bruce Willis", "node_id":0},
		{"type":"Actor", "name":"Alan Rickman", "node_id":1},
		{"type":"Character", "name":"John McClane", "node_id":2},
		{"type":"Character", "name":"Hans Gruber", "node_id":3},
		{"type":"Place", "name":"Nakatomi", "node_id":4}
	]
links = [
		{"n1":"Bruce Willis", "relationship":"PLAYS", "n2":"John McClane"},
		{"n1":"Alan Rickman", "relationship":"PLAYS", "n2":"Hans Gruber"},
		{"n1":"John McClane", "relationship":"VISITS", "n2":"Nakatomi"},
		{"n1":"Hans Gruber", "relationship":"STEALS_FROM", "n2":"Nakatomi"},
		{"n1":"John McClane", "relationship":"KILLS", "n2":"Hans Gruber"}
	]

batch = neo4j.WriteBatch(graph)
for r in records:
	n = batch.create(node(node_id=r['node_id'], name=r['name']))
	batch.add_indexed_node(r['type'],'node_id', r['node_id'], n)
nodes = batch.submit()

for l in links:
	query = "START n=node(*), m=node(*) "
	query += "WHERE n.name='" + l['n1'] + "' AND m.name='" + l['n2'] + "' "
	query += "CREATE (n)-[:"+l['relationship']+"]->(m)" 
	neo4j.CypherQuery(graph, query).execute()


# START n=node(*), m=node(*)  
# where has(n.name) and has(m.name) and n.name='Neo'
# create (n)-[:FRIENDSHIP {status:2}]->(m)

# records = [
# 	("Actor", "Bruce Willis", 0), 
# 	("Character", "John McClane", 1), 
# 	("Actor", "Alan Rickman", 2),
# 	("Character", "Hans Gruber", 3),
# 	("Place", "Nakatomi Plaza", 4)]

# graph.create(rel(bruce, "PLAYS", john))
# graph.create(rel(alan, "PLAYS", hans))
# graph.create(rel(john, "VISITS", nakatomi))
# graph.create(rel(hans, "STEALS_FROM", nakatomi))
# graph.create(rel(john, "KILLS", hans))
# relationships = [
# 		("Bruce Willis", "PLAYS", "John McClane"),
# 		("Alan Rickman", "PLAYS", "Hans Gruber"),
# 		("John McClane", "VISITS", "Nakatomi"),
# 		("Hans Gruber", "STEALS_FROM", "Nakatomi"),
# 		("John McClane", "KILLS", "Hans Gruber")
# 		]



# read_batch = neo4j.ReadBatch(graph)
# write_batch = neo4j.WriteBatch(graph_db)
# for name1, relationship, name2 in relationships:
# 	n1 = read_batch.get_indexed_nodes('city_index','city_id',city_id)



# city_node_id = batch.create(node(city_id=city_id, name=city_name))
#      batch.add_indexed_node('city_index','city_id',city_id, city_node_id)

# bruce, = actor.get_or_create("name", "Bruce Willis", {"name":"Bruce Willis"})
# john, = character.get_or_create("name", "John McClane", {"name":"John McClane"})
# alan, = actor.get_or_create("name", "Alan Rickman", {"name":"Alan Rickman"})
# hans, = character.get_or_create("name", "Hans Gruber", {"name":"Hans Gruber"})
# nakatomi, = place.get_or_create("name", "Nakatomi Plaza", {"name":"Nakatomi Plaza"})




# tx.append("MATCH (a:Actor), (b:Character) "
# 		"WHERE a.name = 'Bruce Willis' AND b.name = 'John McClane' "
# 		"CREATE UNIQUE (a)-[ab:PLAYS]->(b) "
# 		"RETURN ab")
# tx.execute()





# ("MATCH (a:Person), (b:Person) "
#           "WHERE a.name = 'Alice' AND b.name = 'Bob' "
#           "CREATE UNIQUE (a)-[ab:KNOWS]->(b) "
#           "RETURN ab")