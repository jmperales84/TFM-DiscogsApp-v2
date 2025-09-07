import os
import json
from pathlib import Path
from typing import List, Iterable, Iterator
from neo4j import GraphDatabase, Query
from pyspark.sql import SparkSession, DataFrame
from itertools import islice

NEO4J_URI  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "tfm-discogsapp")

def build_spark_from_config(spark_conf: dict) -> SparkSession:
    """
    Create a SparkSession using the provided config dictionary.

    Steps:
      - Starts a builder with app name "neo4j-loader".
      - Applies each entry in `spark_conf` via `builder.config(k, v)`.
      - Does NOT download Delta at runtime. Delta must be enabled by passing:
          * "spark.jars" with the pre-bundled Delta JARs paths, and
          * "spark.sql.extensions" = "io.delta.sql.DeltaSparkSessionExtension",
          * "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog".
      - Sets Spark log level to "WARN".

    Args:
        spark_conf: Mapping of Spark configs (e.g., jars, extensions, catalog).
                Example keys:
                    - "spark.jars": "/opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar"
                    - "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                    - "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    Returns:
        A configured SparkSession ready to read/write Delta tables (if the JARs are present).

    Notes:
        - We intentionally avoid `configure_spark_with_delta_pip(...)` to keep runtime offline.
        - Ensure the Delta JARs exist at the paths referenced by "spark.jars".
    """

    builder = SparkSession.builder.appName("neo4j-loader")

    # Apply all Spark configs (including Delta settings and JAR paths)
    for k, v in (spark_conf or {}).items():

        builder = builder.config(k, v)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def ensure_constraints(driver) -> None:
    """
        Create unique constraints for core nodes if they do not exist.

        Nodes and properties:
          - (a:Album)  -> a.album_id
          - (p:Artist) -> p.artist_id
          - (w:Work)   -> w.work_id
          - (l:Label)  -> l.label_id

        Args:
            driver: Neo4j driver instance.

        Returns:
            None.
    """

    stmts: List[str] = [
        "CREATE CONSTRAINT album_id IF NOT EXISTS FOR (a:Album) REQUIRE a.album_id IS UNIQUE",
        "CREATE CONSTRAINT artist_id IF NOT EXISTS FOR (p:Artist) REQUIRE p.artist_id IS UNIQUE",
        "CREATE CONSTRAINT work_id   IF NOT EXISTS FOR (w:Work)   REQUIRE w.work_id IS UNIQUE",
        "CREATE CONSTRAINT label_id   IF NOT EXISTS FOR (l:Label)   REQUIRE l.label_id IS UNIQUE"
    ]
    with driver.session() as s:

        for q in stmts:

            s.run(q)

def to_rows(df: DataFrame, cols: List[str]) -> List[dict]:
    """
        Convert a DataFrame to a list of dicts keeping only selected columns.

        Args:
            df: Input Spark DataFrame.
            cols: Column names to extract.

        Returns:
            List of dictionaries, one per row, with only the requested columns.
    """

    return [ {c: r[c] for c in cols} for r in df.select(*cols).collect() ]

def get_rows_or_skip(df: DataFrame, cols: List[str], table: str, entry_name: str) -> List[dict] | None:
    """
        Validate the required columns and extract rows.

        Behavior:
          - If any required column is missing, log a warning and return None.
          - If the resulting row list is empty, log an info message and return None.
          - Otherwise, return the list of row dicts.

        Args:
            df: Input Spark DataFrame.
            cols: Required columns for this entity/relation.
            table: Table name (e.g. "albums", "artists").
            entry_name: Config entry name (e.g. "Album", "LEADS").

        Returns:
            List of row dicts, or None if missing columns or no rows.
    """

    missing: List[str] = [c for c in cols if c not in df.columns]

    if missing:

        print(f"[WARN] {table}: Missing columns {missing}. Skip entry {entry_name}")
        return None

    rows: List[dict] = to_rows(df, cols)

    if not rows:

        print(f"[INFO] {table}: 0 rows for {entry_name}. Skip row")
        return None

    return rows

def batched(iterable: Iterable, n: int=1000) -> list:
    """
        Yield lists of size up to n from an iterable (simple batching).

        Args:
            iterable: Any iterable to chunk.
            n: Batch size (default: 1000).

        Yields:
            Lists containing up to n items from the iterable.
    """

    it: Iterator = iter(iterable)

    while True:

        batch: list = list(islice(it, n))

        if not batch:

            break

        yield batch

def main():
    """
        Load Delta tables from the Gold layer and write them into Neo4j.

        Steps:
          1) Open a Neo4j driver and ensure unique constraints exist.
          2) Read `config.json` for paths, entities, relations, and Spark config.
          3) Start Spark with the provided `spark_conf`.
          4) For each configured Gold table:
               - Skip if the Delta path does not exist.
               - Read the table and count rows.
               - For each entry (entity/relation) in the config:
                   * Validate columns and collect rows.
                   * Resolve the Cypher file path relative to this script.
                   * Read Cypher text and execute it in batches using a parameter
                     named `rows`.
          5) Close the Neo4j driver and stop Spark.

        Returns:
            None.
    """

    # 1) Neo4j connection
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    ensure_constraints(driver)

    with open("config.json") as f:
        config: dict = json.load(f)

    # 2) Spark
    spark_conf: dict = config.get("spark_conf", {})
    spark: SparkSession = build_spark_from_config(spark_conf)

    gold_dir: str = config["paths"]["gold"]
    data_config: dict = config["delta_entities_relations"]

    for table, entries in data_config.items():

        path: str = f"{gold_dir}/{table}"

        if not os.path.exists(path):

            print(f"[WARN] Not exists {path}. Skip {table}")

            continue

        # Read Delta Table
        df: DataFrame = spark.read.format("delta").load(path)
        count: int = df.count()
        print(f"[OK] Read gold.{table} -> {count} rows")

        for entry in entries:

            cypher_file: str = entry["cypher_file"]
            cols: List[str] = entry["relevant_cols"]
            entry_name: str = entry.get("name")

            table_rows: List[dict] = get_rows_or_skip(df, cols, table, entry_name)

            if table_rows is None:
                continue

            base_dir: Path = Path(__file__).parent
            cypher_path: Path = (base_dir / cypher_file).resolve()

            if not cypher_path.exists():

                print(f"[ERROR] Cypher file doesn't exist: {cypher_path}")
                continue

            cypher_statements: str = cypher_path.read_text(encoding="utf-8")

            with driver.session() as session:

                total: int = 0

                for chunk in batched(table_rows, 2000):

                    session.run(Query(cypher_statements), rows=chunk)
                    total += len(chunk)

                print(f".[OK] {table}::{entry_name} -> {total} rows applied")

    driver.close()
    spark.stop()

if __name__ == "__main__":
    main()