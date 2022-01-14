match (n) detach delete n

//load table.csv to graph db

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
WHERE t.schema = 'dbo'
SET t:dbo
RETURN t


//load usp.csv to graph db

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
WHERE r.schema = 'dbo'
SET r:dbo
RETURN r

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
CREATE (f)-[:REF_TO]->(t)  


//load usp2usp.csv to graph db
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:///usp2usp.csv' AS line
MATCH (f:Routine { name: line.usp_full_name}),(t:Routine { name: line.ref_usp_full_name})
CREATE (f)-[:CALL]->(t)  


//remove duplicates relationship between all nodes
match (s)-[r]->(n) 
with s,n,type(r) as t, tail(collect(r)) as coll 
foreach(x in coll | delete x)


//find a sample graph
match (p)-[:REF_TO]->(t)
where t.name <> 'PREPARE.AUDIT_INGESTION_SUMMARY'
return p, t

match (p)-[]->(t)
where t.name <> 'PREPARE.AUDIT_INGESTION_SUMMARY'
return p, t


//find the pedigree of a specific published table
match (b)-[r1]->(s)-[r2]->(g)
return b,r1,


MATCH (b:Table:Prepare), (g:Table:Publish{name:'PUBLISH.ANFIELD_LOY_offer_SUMMARY'})
MATCH p=shortestPath((b)-[:REF_TO *..5]-(g))
RETURN p
