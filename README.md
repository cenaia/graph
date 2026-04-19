# Graph Export Workspace

该目录用于存放外置知识图谱备份脚本和每次导出的完整工作目录。

## 运行方式

```bash
cd /Users/ciaccy/Desktop/0all/obj/contest/Kiveiv-loukie7-0320
uv run /Users/ciaccy/Desktop/0all/obj/contest/graph/export_latest_graph_bundle.py
```

可选参数：

```bash
uv run /Users/ciaccy/Desktop/0all/obj/contest/graph/export_latest_graph_bundle.py \
  --task-id tsk_xxx \
  --mode both \
  --runtime-env dev
```

## 输出结构

每次运行都会在本目录下新建一个子目录：

```text
<timestamp>__task_<task_id>__doc_<doc_id>__v_<version_no>/
├── manifest.json
├── raw/
│   ├── graph.json
│   ├── graph.graphml
│   ├── entity_observations.json
│   └── viewer.html
└── neo4j/
    ├── type_mappings.json
    ├── cypher/
    │   ├── entities.csv
    │   ├── relationships.csv
    │   ├── observations.csv
    │   ├── entity_has_observation.csv
    │   └── import.cypher
    └── admin/
        ├── entities.csv
        ├── relationships.csv
        ├── observations.csv
        └── entity_has_observation.csv
```

## Neo4j 导入

### Cypher / cypher-shell

把 `neo4j/cypher/` 下文件放到 Neo4j `import` 目录后执行：

```bash
cypher-shell -u neo4j -p '<password>' -f import.cypher
```

### Neo4j 5.x `neo4j-admin database import full`

适合空库离线导入：

```bash
neo4j-admin database import full neo4j \
  --nodes=neo4j/admin/entities.csv \
  --nodes=neo4j/admin/observations.csv \
  --relationships=neo4j/admin/relationships.csv \
  --relationships=neo4j/admin/entity_has_observation.csv
```

### Neo4j 4.x 兼容示例

```bash
neo4j-admin import \
  --database=neo4j \
  --nodes=neo4j/admin/entities.csv \
  --nodes=neo4j/admin/observations.csv \
  --relationships=neo4j/admin/relationships.csv \
  --relationships=neo4j/admin/entity_has_observation.csv
```

## 安全保护

- 默认只允许连接 `kiveiv_dev`
- 如果当前环境不是 `kiveiv_dev`，脚本会直接拒绝执行
- 只有显式加 `--allow-nonstandard-db` 才会跳过该保护
