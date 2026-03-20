# Plan : IcebergPartitionedScan

## Contexte

`IcebergTableScan` contient un TODO explicite depuis sa création :

```rust
// TODO:
// This is more or less a placeholder, to be replaced
// once we support output-partitioning
Partitioning::UnknownPartitioning(1),
```

Ce noeud exécute l'intégralité du scan dans une seule partition DataFusion, quelle que soit
la taille de la table. Cela rend l'intégration Iceberg structurellement plus lente que Delta
Lake (issue upstream [#1864](https://github.com/apache/iceberg-rust/issues/1864)) et empêche
toute exécution distribuée efficace.

`IcebergTableScan` et `IcebergTableProvider` sont conservés tels quels pour ne pas casser
l'API publique existante. Le nouveau comportement est opt-in via un nouveau table provider.

---

## Nouveaux composants

| Composant | Fichier | Rôle |
|---|---|---|
| `IcebergPartitionedTableProvider` | `table/partitioned.rs` | Table provider qui appelle `plan_files()` au planning time |
| `IcebergPartitionedScan` | `physical_plan/partitioned_scan.rs` | Noeud d'exécution, 1 partition DataFusion par `FileScanTask` |

### Choix du nom `IcebergPartitionedScan`

| Nom candidat | Raison d'écarter |
|---|---|
| `IcebergFileScan` | Ambigu avec `FileScanTask` du core Iceberg |
| `IcebergParallelScan` | Décrit l'effet, pas la structure |
| `IcebergDistributedScan` | Trop spécifique à un cas d'usage |
| `IcebergPartitionedScan` | ✓ Décrit exactement ce qui change : N partitions DataFusion exposées |

---

## Architecture de fichiers

```
crates/integrations/datafusion/src/
├── physical_plan/
│   ├── mod.rs                    ← ajouter pub mod partitioned_scan
│   ├── scan.rs                   ← inchangé
│   ├── partitioned_scan.rs       ← nouveau
│   └── ...
├── table/
│   ├── mod.rs                    ← inchangé
│   ├── partitioned.rs            ← nouveau
│   └── ...
└── lib.rs                        ← exporter IcebergPartitionedTableProvider
```

---

## Vue d'ensemble : providers et noeuds

```
EXISTANT (inchangé)                NOUVEAU (opt-in)
────────────────────────────────   ──────────────────────────────────────────
IcebergTableProvider               IcebergPartitionedTableProvider
  scan()                             scan()
    └── IcebergTableScan               ├── load_table()
          1 partition                  ├── plan_files().await
          plan_files() au runtime      │     └── Vec<FileScanTask>
                                       └── IcebergPartitionedScan
                                             N partitions
                                             plan_files() au planning time

IcebergStaticTableProvider         (pas de pendant partitionné
  scan()                            immédiatement — peut venir plus tard)
    └── IcebergTableScan
```

---

## Plan de modifications

### Étape 1 — Créer `physical_plan/partitioned_scan.rs`

Partir de `scan.rs` comme base et appliquer les modifications suivantes.

#### 1a. Struct

Remplacer les champs `table`, `snapshot_id`, `projection`, `predicates` par `tasks` et
`file_io`. `file_io` est nécessaire pour que `ArrowReaderBuilder` puisse ouvrir les fichiers
sans avoir à stocker `Table` entier.

```
IcebergTableScan {                 IcebergPartitionedScan {
  table: Table,                      tasks: Vec<FileScanTask>,
  snapshot_id: Option<i64>,          file_io: FileIO,
  plan_properties: PlanProperties,   schema: ArrowSchemaRef,
  projection: Option<Vec<String>>,   plan_properties: PlanProperties,
  predicates: Option<Predicate>,     limit: Option<usize>,
  limit: Option<usize>,            }
}
```

#### 1b. Constructeur `new()`

```
new(                               new(
  table: Table,                      tasks: Vec<FileScanTask>,
  snapshot_id: Option<i64>,          file_io: FileIO,
  schema: ArrowSchemaRef,            schema: ArrowSchemaRef,
  projection: Option<&Vec<usize>>,   limit: Option<usize>,
  filters: &[Expr],                ) -> Self
  limit: Option<usize>,
) -> Self
```

La conversion `filters → Predicate` et `projection → Vec<String>` sont retirées du
constructeur — elles sont faites en amont dans `IcebergPartitionedTableProvider::scan()`.

#### 1c. `compute_properties()`

```
fn compute_properties(             fn compute_properties(
  schema: ArrowSchemaRef,            schema: ArrowSchemaRef,
) -> PlanProperties                  n_partitions: usize,
                                   ) -> PlanProperties

Partitioning::UnknownPartitioning  Partitioning::UnknownPartitioning(
  (1)                                n_partitions.max(1)
                                   )
```

`n_partitions` vaut `tasks.len()` passé depuis `new()`.

#### 1d. `execute()`

```
execute(                           execute(
  _partition: usize,   ← ignoré     partition: usize,    ← utilisé
  _context,                         _context,
)                                  )

  get_batch_stream(                  let task = self.tasks
    self.table, ...                    .get(partition)
  )                                    .cloned()
                                       .ok_or(DataFusionError::Internal(...))?;

                                     ArrowReaderBuilder::new(self.file_io.clone())
                                       .build()
                                       .read(stream::once(Ok(task)))
```

La logique de limit par `try_filter_map` est portée telle quelle depuis
`IcebergTableScan::execute()`.

La fonction privée `get_batch_stream()` n'est pas portée — remplacée par l'appel direct à
`ArrowReaderBuilder`.

#### 1e. `DisplayAs`

```
"IcebergTableScan                  "IcebergPartitionedScan
  projection:[...]                   partitions:[N]
  predicate:[...]                    limit:[...]"
  limit:[...]"
```

`N` = `self.tasks.len()`.

---

### Étape 2 — Modifier `physical_plan/mod.rs`

```rust
pub mod partitioned_scan;   // ajouter
```

---

### Étape 3 — Créer `table/partitioned.rs`

Nouveau fichier contenant `IcebergPartitionedTableProvider`.

#### 3a. Struct

```rust
pub struct IcebergPartitionedTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: ArrowSchemaRef,
}
```

Même structure qu'`IcebergTableProvider` — c'est intentionnel, la différence est uniquement
dans le comportement de `scan()`.

#### 3b. `impl TableProvider`

```
scan(projection, filters, limit)

  // 1. Charger les métadonnées fraîches
  table = catalog.load_table()

  // 2. Convertir projection → noms de colonnes
  col_names = get_column_names(schema, projection)

  // 3. Convertir filters → Predicate Iceberg
  predicate = convert_filters_to_predicate(filters)

  // 4. Résoudre les fichiers au planning time
  builder = table.scan()
  builder = if col_names { builder.select(col_names) }
              else       { builder.select_all()       }
  builder = if predicate { builder.with_filter(pred) }
  tasks: Vec<FileScanTask> = builder
    .build()?
    .plan_files().await?
    .try_collect().await?

  // 5. Projeter le schéma de sortie
  output_schema = schema.project(projection) ou schema.clone()

  // 6. Récupérer le FileIO depuis la table
  file_io = table.file_io().clone()

  IcebergPartitionedScan::new(tasks, file_io, output_schema, limit)
```

`supports_filters_pushdown()` : identique à `IcebergTableProvider` — retourne `Inexact`
pour tous les filtres.

`insert_into()` : non supporté — retourne `FeatureUnsupported` (les écritures restent sur
`IcebergTableProvider`).

---

### Étape 4 — Modifier `lib.rs`

Exporter le nouveau provider dans l'API publique de la crate :

```rust
pub use table::partitioned::IcebergPartitionedTableProvider;
```

---

## Récapitulatif visuel complet

```
table/partitioned.rs                    physical_plan/partitioned_scan.rs
────────────────────────────────────    ──────────────────────────────────────
IcebergPartitionedTableProvider         IcebergPartitionedScan
  impl TableProvider                      champs : tasks, file_io, schema, limit
  │
  scan()                                execute(partition)
    ├── load_table()                      tasks[partition]
    ├── convert_filters_to_predicate()    ArrowReaderBuilder::new(file_io)
    ├── plan_files().await                  .build()
    │     └── Vec<FileScanTask>             .read(stream::once(task))
    ├── file_io = table.file_io()
    └── IcebergPartitionedScan::new(    compute_properties(tasks.len())
          tasks,          ──────────►    UnknownPartitioning(N)
          file_io,
          output_schema,
          limit,
        )
```

---

## Tests

### Dans `table/partitioned.rs`

| Test | Ce qu'il vérifie |
|---|---|
| `test_partitioned_provider_creation` | Construction réussie depuis un catalog |
| `test_partitioned_provider_scan_schema` | Le schéma projeté est correct |
| `test_partitioned_provider_rejects_writes` | `insert_into()` retourne une erreur |

### Dans `physical_plan/partitioned_scan.rs`

| Test | Ce qu'il vérifie |
|---|---|
| `test_partitioned_scan_output_partitioning` | `output_partitioning()` retourne `N` pour N fichiers |
| `test_partitioned_scan_empty_table` | `output_partitioning()` retourne `1` si aucun fichier (pas de panique) |
| `test_limit_pushdown` | La logique de limit fonctionne correctement |

Les tests existants de `IcebergTableScan` et `IcebergTableProvider` restent inchangés.

---

## Diagramme du flux d'exécution

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                            PLANNING TIME                                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  SessionContext::sql("SELECT ... FROM my_table")                             ║
║       │                                                                      ║
║       ▼                                                                      ║
║  DataFusion Logical Planner                                                  ║
║       │  détecte my_table → cherche le TableProvider enregistré              ║
║       ▼                                                                      ║
║  IcebergPartitionedTableProvider::scan(projection, filters, limit)           ║
║       │                                                                      ║
║       ├─ 1. catalog.load_table()          → Table { metadata, file_io }     ║
║       │                                                                      ║
║       ├─ 2. convert_filters_to_predicate(filters)                            ║
║       │        DataFusion Expr  →  Iceberg BoundPredicate                    ║
║       │                                                                      ║
║       ├─ 3. table.scan()                                                     ║
║       │        .select(col_names)    ← projection pushdown                   ║
║       │        .with_filter(pred)    ← predicate pushdown                    ║
║       │        .build()                                                      ║
║       │        .plan_files().await                                           ║
║       │              │                                                       ║
║       │              ├── lit le manifest list                                ║
║       │              ├── filtre les manifests (partition pruning)            ║
║       │              ├── filtre les fichiers (stats pruning)                 ║
║       │              └── stream de FileScanTask                              ║
║       │                      { path, schema, field_ids,                      ║
║       │                        predicate, deletes, start/length }            ║
║       │                                                                      ║
║       ├─ 4. try_collect() → Vec<FileScanTask>  [ t0, t1, t2, ... tN ]       ║
║       │                                                                      ║
║       ├─ 5. file_io = table.file_io().clone()                                ║
║       │                                                                      ║
║       └─ 6. IcebergPartitionedScan::new(tasks, file_io, schema, limit)      ║
║                   │                                                          ║
║                   └── compute_properties(tasks.len())                        ║
║                             Partitioning::UnknownPartitioning(N)             ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                  PHYSICAL PLAN (visible par DataFusion)                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║   IcebergPartitionedScan                                                     ║
║     partitions: N                                                            ║
║     tasks:  [ t0      t1      t2      ...     tN ]                          ║
║              part.0  part.1  part.2          part.N                          ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                            EXECUTION TIME                                   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  DataFusion Scheduler  →  lance N threads, un par partition                  ║
║                                                                              ║
║  ┌─────────────────┐  ┌─────────────────┐       ┌─────────────────┐        ║
║  │   execute(0)    │  │   execute(1)    │  ...  │   execute(N)    │        ║
║  │                 │  │                 │       │                 │        ║
║  │  tasks[0] = t0  │  │  tasks[1] = t1  │       │  tasks[N] = tN  │        ║
║  │       │         │  │       │         │       │       │         │        ║
║  │       ▼         │  │       ▼         │       │       ▼         │        ║
║  │  ArrowReader    │  │  ArrowReader    │       │  ArrowReader    │        ║
║  │  Builder(       │  │  Builder(       │       │  Builder(       │        ║
║  │   file_io)      │  │   file_io)      │       │   file_io)      │        ║
║  │       │         │  │       │         │       │       │         │        ║
║  │  .read(         │  │  .read(         │       │  .read(         │        ║
║  │   once(t0))     │  │   once(t1))     │       │   once(tN))     │        ║
║  │       │         │  │       │         │       │       │         │        ║
║  │  ouvre fichier  │  │  ouvre fichier  │       │  ouvre fichier  │        ║
║  │  applique pred  │  │  applique pred  │       │  applique pred  │        ║
║  │  applique proj  │  │  applique proj  │       │  applique proj  │        ║
║  │  applique deletes│  │  applique deletes│      │  applique deletes│       ║
║  │       │         │  │       │         │       │       │         │        ║
║  │       ▼         │  │       ▼         │       │       ▼         │        ║
║  │ RecordBatch     │  │ RecordBatch     │       │ RecordBatch     │        ║
║  │ Stream          │  │ Stream          │       │ Stream          │        ║
║  └────────┬────────┘  └────────┬────────┘       └────────┬────────┘        ║
║           │                   │                          │                  ║
║           └───────────────────┴──────────────────────────┘                  ║
║                               │                                              ║
║                               ▼                                              ║
║                     noeud DataFusion parent                                  ║
║                  (CoalescePartitions, Aggregate, ...)                        ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## Ce qui n'est pas dans ce plan

- **Codec de sérialisation** (`IcebergPhysicalCodec`) : laissé à part, nécessaire pour
  DataFusion distribué mais indépendant du présent changement.
- **`IcebergStaticTableProvider` partitionné** : peut être ajouté plus tard avec
  `IcebergStaticPartitionedTableProvider` suivant le même pattern.
- **`IcebergTableScan` et `IcebergTableProvider`** : conservés sans aucune modification.
