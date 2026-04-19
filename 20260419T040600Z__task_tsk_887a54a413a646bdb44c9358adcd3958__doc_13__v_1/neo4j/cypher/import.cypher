CREATE CONSTRAINT entity_entity_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.entity_id IS UNIQUE;
CREATE CONSTRAINT observation_observation_id IF NOT EXISTS FOR (n:Observation) REQUIRE n.observation_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/entities.csv' AS row
MERGE (n:Entity {entity_id: row.entity_id})
SET n.canonical_entity_id = row.canonical_entity_id,
    n.canonical_key = row.canonical_key,
    n.display_name = row.display_name,
    n.normalized_name = row.normalized_name,
    n.name = row.name,
    n.primary_type = row.primary_type,
    n.type = row.type,
    n.type_labels = CASE WHEN row.type_labels_pipe = '' THEN [] ELSE split(row.type_labels_pipe, '|') END,
    n.type_labels_json = row.type_labels_json,
    n.description = row.description,
    n.attributes_json = row.attributes_json,
    n.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    n.source_chunk_ids_json = row.source_chunk_ids_json,
    n.entity_occurrence_ids = CASE WHEN row.entity_occurrence_ids_pipe = '' THEN [] ELSE split(row.entity_occurrence_ids_pipe, '|') END,
    n.entity_occurrence_ids_json = row.entity_occurrence_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/observations.csv' AS row
MERGE (o:Observation {observation_id: row.observation_id})
SET o.text = row.text,
    o.kind = row.kind,
    o.source_kind = row.source_kind,
    o.attribute_key = row.attribute_key,
    o.attribute_value = row.attribute_value,
    o.relation_type = row.relation_type,
    o.peer_entity_id = row.peer_entity_id,
    o.peer_entity_name = row.peer_entity_name,
    o.direction = row.direction,
    o.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    o.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/entity_has_observation.csv' AS row
MATCH (e:Entity {entity_id: row.entity_id})
MATCH (o:Observation {observation_id: row.observation_id})
MERGE (e)-[:HAS_OBSERVATION]->(o);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_056B0FE714'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_056B0FE714]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_3F8A56C3E0'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_3F8A56C3E0]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_4E1545733F'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_4E1545733F]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_4F136DB218'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_4F136DB218]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_5355C49382'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_5355C49382]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_5BE321F307'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_5BE321F307]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_6ED9EA33F3'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_6ED9EA33F3]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_9263E46A01'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_9263E46A01]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_93D4786288'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_93D4786288]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_9899E5D4D0'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_9899E5D4D0]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_9A950C085E'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_9A950C085E]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_D984D98F05'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_D984D98F05]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_E7943F9461'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_E7943F9461]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_F200F29D83'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_F200F29D83]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_F896FB3776'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_F896FB3776]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T040600Z__task_tsk_887a54a413a646bdb44c9358adcd3958__doc_13__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.neo4j_rel_type = 'REL_FBAF4B891A'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:REL_FBAF4B891A]->(target)
SET r.relationship_id = row.relationship_id,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;
