CREATE CONSTRAINT entity_entity_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.entity_id IS UNIQUE;
CREATE CONSTRAINT observation_observation_id IF NOT EXISTS FOR (n:Observation) REQUIRE n.observation_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/entities.csv' AS row
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

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/observations.csv' AS row
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

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/entity_has_observation.csv' AS row
MATCH (e:Entity {entity_id: row.entity_id})
MATCH (o:Observation {observation_id: row.observation_id})
MERGE (e)-[r:HAS_OBSERVATION]->(o)
SET r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'APPLIES_TO'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:APPLIES_TO]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'BASED_ON'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:BASED_ON]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'CAUSES'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:CAUSES]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'CONFIRMS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:CONFIRMS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'CONTAINS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:CONTAINS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'DISCOVERS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:DISCOVERS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'EXPERIENCES_FAILURE'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:EXPERIENCES_FAILURE]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'HANDLES'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:HANDLES]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'INSPECTS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:INSPECTS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'MONITORS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:MONITORS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'PRECEDES'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:PRECEDES]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'PRODUCES'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:PRODUCES]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'RESPONSIBLE_FOR'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:RESPONSIBLE_FOR]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'TARGETS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:TARGETS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'UNKNOWN_RELATION'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:UNKNOWN_RELATION]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'USES_REPAIR_PROCESS'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:USES_REPAIR_PROCESS]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'USES_TEST_METHOD'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:USES_TEST_METHOD]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/cenaia/graph/main/20260419T083141Z__task_tsk_a28a776681594360bb02b2e7b5108572__doc_11__v_1/neo4j/cypher/relationships.csv' AS row
WITH row WHERE row.canonical_type_en = 'WITNESSES'
MATCH (source:Entity {entity_id: row.source_entity_id})
MATCH (target:Entity {entity_id: row.target_entity_id})
CREATE (source)-[r:WITNESSES]->(target)
SET r.relationship_id = row.relationship_id,
    r.neo4j_rel_type = row.neo4j_rel_type,
    r.source = row.source,
    r.target = row.target,
    r.source_role = row.source_role,
    r.target_role = row.target_role,
    r.relation_key = row.relation_key,
    r.canonical_type = row.canonical_type,
    r.canonical_type_en = row.canonical_type_en,
    r.display_type_zh = row.display_type_zh,
    r.display_type_en = row.display_type_en,
    r.display_type_bilingual = row.display_type_bilingual,
    r.type = row.type,
    r.raw_type = row.raw_type,
    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,
    r.raw_types_json = row.raw_types_json,
    r.description = row.description,
    r.normalization_status = row.normalization_status,
    r.matched_schema_json = row.matched_schema_json,
    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,
    r.source_chunk_ids_json = row.source_chunk_ids_json;
