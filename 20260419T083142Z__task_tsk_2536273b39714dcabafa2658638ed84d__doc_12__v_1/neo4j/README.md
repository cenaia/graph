# Neo4j Import Guide

## Default Imports
- Cloud engineering import: `neo4j/cypher/import.cypher`
- Local engineering import: `neo4j/cypher/import.local.cypher`
- Both use English `canonical_type_en` as the Neo4j relationship type.
- Chinese or bilingual teaching labels come from relationship properties: `display_type_zh` and `display_type_bilingual`.

## Why The Cloud Script Works Directly
- `import.cypher` uses `raw.githubusercontent.com` URLs, so Neo4j Aura can fetch CSV files without local `file:///` access.

## Engineering vs Teaching
- Engineering mode keeps stable English relation types for queries, statistics, and downstream programs.
- Teaching mode uses the same English relation types, but you display `display_type_zh` or `display_type_bilingual` in Browser/Bloom captions or result tables.

## Visualization Hint
- In Neo4j Browser or Bloom, set the relationship caption/text field to `display_type_zh` or `display_type_bilingual` when you want Chinese or bilingual labels.

## Example Queries
```cypher
MATCH ()-[r:CAUSES]->()
RETURN type(r) AS canonical_type_en, r.display_type_bilingual AS display_name, count(*) AS relation_count
ORDER BY relation_count DESC
LIMIT 10;
```

```cypher
MATCH ()-[r]->()
RETURN type(r) AS canonical_type_en, r.display_type_zh AS zh_label, r.display_type_bilingual AS bilingual_label
LIMIT 20;
```

- Selected relation view profile for this bundle: `both`

## Generated Scripts
- `neo4j/cypher/import.cypher` (cloud, profile=engineering)
- `neo4j/cypher/import.local.cypher` (local, profile=engineering)
