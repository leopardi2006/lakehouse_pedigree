match (n) detach delete n

//http://localhost:11001/project-4ddbb599-2e94-4357-84a9-1b359f362d77/

//load table.csv to graph db
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///table.csv' AS line
CREATE (:Table {name: line.name1, schema: line.schema, short_name: line.name, type: line.type, alias: line.name2, row_count: toInteger(line.row_count)})

MATCH (t:Table)
WHERE LEFT(t.schema,7) = 'ANALYSE'
SET t:Analyse
RETURN t


MATCH (t:Table)
WHERE LEFT(t.schema,7) = 'PREPARE'
SET t:Prepare
RETURN t

MATCH (t:Table)
WHERE LEFT(t.schema,7) = 'PUBLISH'
SET t:Publish
RETURN t

MATCH (t:Table)
WHERE t.type = 'VIEW'
SET t:View
RETURN t


//load usp.csv to graph db
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///usp.csv' AS line
CREATE (:Routine {name: line.routine_full_name, schema: line.schema_name, short_name: line.routine_name, type: line.type, last_exe_date: line.last_execution_time, create_date: line.create_date, modify_date: line.modify_date, exe_count: toInteger(line.execution_count)})

MATCH (r:Routine)
WHERE LEFT(r.schema,7) = 'ANALYSE'
SET r:Analyse
RETURN r

MATCH (r:Routine)
WHERE LEFT(r.schema,7) = 'PREPARE'
SET r:Prepare
RETURN r

MATCH (r:Routine)
WHERE LEFT(r.schema,7) = 'PUBLISH'
SET r:Publish
RETURN r

MATCH (r:Routine)
WHERE r.type = 'FUNCTION'
SET r:Function
RETURN r

//create generic names for nodes and links
MATCH (n)
SET n:Object
RETURN n.name


//create index to speed up match
//CREATE INDEX ON :Table(Name)
CREATE INDEX TableIndex
FOR (n:Table)
ON (n.name)
//CREATE INDEX ON :Routine(Name)
CREATE INDEX RoutineIndex
FOR (n:Routine)
ON (n.name)

//load usp2table.csv to graph db
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///usp2table.csv' AS line
MATCH (f:Routine { name: line.usp_full_name}),(t:Table { name: line.table_full_name})
CREATE (f)-[:LINK {name: 'ref_to'}]->(t)  


//load usp2usp.csv to graph db
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///usp2usp.csv' AS line
MATCH (f:Routine { name: line.usp_full_name}),(t:Routine { name: line.ref_usp_full_name})
CREATE (f)-[:LINK {name: 'call'}]->(t)  


//remove duplicates relationship between all nodes
match (s)-[r]->(n) 
with s,n,type(r) as t, tail(collect(r)) as coll 
foreach(x in coll | delete x)


//find a sample graph
match (p)-[:REF_TO]->(t)
where t.name <> 'prepare.audit_ingestion_summary'
return p, t

match (p)-[]-(t)
where t.name <> toLower('prepare.audit_ingestion_summary')
return p, t


//find the pedigree of a specific published table
match (n)
return n


MATCH (b:Table:Prepare), (g:Table:Publish{name:toLower('name: publish.anfield_loy_offer_summary')})
MATCH p=shortestPath((b)-[:REF_TO *..5]-(g))
RETURN p

//use apoc to anlyze the network

// Find all pivotal nodes in sub-network between 2 nodes
MATCH (a:Table{name:'prepare.anfield_alp_transaction_bonus_info_sync'}), (b:Table{name:"publish.anfield_loy_offer_summary"})
MATCH  p=allShortestPaths((a)-[*]-(b)) WITH collect(p) AS paths,a,b
MATCH (c:Table) WHERE all(x IN paths WHERE c IN nodes(x)) AND NOT c IN [a,b]
RETURN a.name, b.name, c.name AS PivotalNode LIMIT 25

//find out "important" node with highest degree (without direction)
MATCH (n:Table)-[r]-(m) 
RETURN n.name, count(r) as Degree
ORDER BY Degree desc

//--the station that has the biggest immediate impact
MATCH (n:Table{name:'prepare.audit_ingestion_summary'})-[*..1]-(m) RETURN n,m

//MATCH (n{name:'publish.anfield_to_dm_fin_product_coupon_status'})-[*..3]-(m) RETURN n,m



// !!! Find maximum diameter of network <<< very long time to run
// maximum shortest path between two nodes  
MATCH (a:Table), (b:Table) WHERE id(a) > id(b)
MATCH p=shortestPath((a)-[*]-(b))
RETURN length(p) AS len, extract(x IN nodes(p) | x.name) AS path
ORDER BY len DESC LIMIT 5


//find out "important" node with highest degrees
MATCH (n)-[r]-(m) 
RETURN n.name, count(r) as CT
ORDER BY CT desc



//change the node display restriction
:config initialNodeDisplay: 1000

//NEW//Closeness Centrality Procedure
CALL gds.alpha.closeness.stream({
  nodeProjection: 'Object',
  relationshipProjection: 'LINK'
})
YIELD nodeId, centrality
RETURN gds.util.asNode(nodeId).name AS node, centrality
ORDER BY centrality DESC

//show top node
MATCH (n{name:"1378"})-[*..3]-(m) RETURN n,m



//named graph projection
CALL gds.graph.create('myGraph', 'Object', 'LINK')
//NEW//Betweeness Centrality Procedure 
CALL gds.betweenness.stream('myGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY score DESC

//show top node
MATCH (n{Name:"769"})-[*..3]-(m) RETURN n,m

