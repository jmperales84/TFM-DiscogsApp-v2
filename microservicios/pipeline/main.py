import json
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from data_archiver import DataArchiver
from raw_stream_reader import RawStreamReader
from bronze_stream_writer import BronzeStreamWriter
from gold_transformer import GoldTransformer
from gold_writer import GoldWriter


def build_spark_from_config(spark_conf: dict) -> SparkSession:
    """
        Create and configure a SparkSession from a config dictionary.

        Steps:
          - Starts a builder with app name "pipeline-albums".
          - Applies each key/value from `spark_conf` via `builder.config(k, v)`.
          - Tries to enable Delta Lake if `delta-spark` is installed:
              uses `configure_spark_with_delta_pip(builder).getOrCreate()`.
            If not available, falls back to `builder.getOrCreate()`.
          - Sets Spark log level to "WARN".

        Args:
            spark_conf: Mapping of Spark config keys to values
                        (e.g. extensions, catalog, shuffle partitions).
                        Keys and values are passed as-is to `builder.config`.

        Returns:
            An active `SparkSession` with the requested configuration.

        Notes:
            - Import `configure_spark_with_delta_pip` *inside* the function.
              If you import it at module level y no está instalado, fallará al arrancar.
            - PySpark no expone un tipo estable para el “builder”, por eso no lo
              anotamos; lo importante es que la función retorne un `SparkSession`.
    """

    builder = SparkSession.builder.appName("pipeline-albums")

    # (**kwargs) not valid. Apply spark configuration one by one
    for k, v in (spark_conf or {}).items():

        builder = builder.config(k, v)

    # enable delta
    try:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        # start spark anyway
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def run_bronze(spark: SparkSession, config: dict) -> None:
    """
        Run the Bronze ingestion phase.

        Steps:
            1) Move newly arrived files from `landing` to `raw` using DataArchiver.
            2) Start a one-shot Structured Streaming job that:
                - reads the raw dataset via `RawStreamReader.read(spark)`,
                - writes each micro-batch to the Bronze path with `BronzeStreamWriter.append_2_bronze`,
                - uses a checkpoint under the Bronze dataset path,
                - runs with `trigger(availableNow=True)` to process all available data and then stop.
            3) Wait for the streaming query to finish and print a completion message.

        Args:
            spark: Active SparkSession.
            config: Pipeline configuration dictionary.

        Side effects:
            - Writes/updates files under the Bronze path (e.g., Delta files and transaction log).
            - Creates/updates the Bronze table registration in the metastore (idempotent).
            - Creates/updates the streaming checkpoint directory.

        Returns:
            None.
    """

    # 1) Archive landing -> raw (if applicable)
    moved: int = DataArchiver(config).move_all()
    print(f"[Archiver] Number of moved files: {moved}")

    # 2) Stream to Bronze (one-shot: process everything available and stop)
    reader: RawStreamReader = RawStreamReader(config)
    writer: BronzeStreamWriter = BronzeStreamWriter(config)

    query: StreamingQuery = (
        reader.read(spark)
        .writeStream
        .foreachBatch(lambda df, bid: writer.append_2_bronze(spark, df, bid))
        .option("checkpointLocation", writer.dataset_checkpoint_location)
        .trigger(availableNow=True)  # process everything and then stop
        .queryName("bronze-albums")
        .start()
    )
    query.awaitTermination()

    print("[Bronze] Ingestion completed.")


def ensure_table_registered(spark, config) -> tuple[str, str, str]:
    """
        Ensure the Bronze table is present in the metastore if data already exists on disk.

        Why:
            With one-shot streaming (availableNow), there may be runs with no new batches.
            In that case the writer does not register the table, even though the path
            already contains data from previous runs. This helper makes the table
            queryable by name (`bronze.<subdir>`) anyway.

        Steps:
            - Builds the table name `bronze.<subdir>` and its dataset path.
            - If the table is NOT in the catalog, checks the path:
                * For Delta: looks for the `_delta_log` directory.
                * For non-Delta: checks that the directory has files.
            - If data is present, creates the database (if needed) and registers an
                external table pointing to that location:
                CREATE TABLE IF NOT EXISTS bronze.<subdir>
                USING <format> LOCATION '<path>'.

        Args:
            spark: Active SparkSession.
            config: Pipeline configuration dict (expects paths.bronze, datasets.albums.subdir,
                    and bronze_options.format).

        Returns:
            A tuple (table_name, path, using_fmt) for logging/diagnostics.

        Side effects:
            - May create the `bronze` database.
            - May register the external table in the metastore.

        Notes:
            - Idempotent: if the table already exists or the path has no data, it does nothing.
            - Keeps data and catalog in sync without requiring a new write batch.
    """

    subdir: str = config["datasets"]["albums"]["subdir"]
    table: str = f"bronze.{subdir}"
    bronze_base: Path = Path(config["paths"]["bronze"])
    path: str = (bronze_base / subdir).as_posix()

    fmt: str = config.get("bronze_options", {}).get("format", "delta").lower()
    using_fmt: str = "DELTA" if fmt == "delta" else fmt.upper()

    # Check if there's data in the path
    has_delta: bool = Path(path, "_delta_log").exists()
    has_files: bool = Path(path).exists() and any(Path(path).iterdir())

    if not spark.catalog.tableExists(table):

        if (fmt == "delta" and has_delta) or (fmt != "delta" and has_files):

            spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table} "
                f"USING {using_fmt} LOCATION '{path}'"
            )
    return table, path, using_fmt


def run_gold(spark: SparkSession, config: dict) -> None:
    """
        Build and write Gold tables from the Bronze dataset.

        Steps:
            1) Create a GoldTransformer and GoldWriter from the config.
            2) Load the Bronze dataset (batch) via `tr.bronze_reader(spark)`.
            3) Derive each Gold table with GoldTransformer and persist it with GoldWriter:
                - Dimensions: albums, artists, labels, works
                 - Link/fact tables: tracks, album_artist, album_work, album_label
            4) Print a completion message.

        Args:
            spark: Active SparkSession.
            config: Pipeline configuration dictionary.

        Side effects:
            - Writes/overwrites datasets under the Gold path (e.g., Delta files).
            - Registers/re-registers `gold.<table>` entries in the metastore.

        Returns:
            None.
    """

    tr: GoldTransformer = GoldTransformer(config)
    wr: GoldWriter = GoldWriter(config)

    bronze_df: DataFrame = tr.bronze_reader(spark)

    wr.write_df(spark, GoldTransformer.albums(bronze_df), "albums")
    wr.write_df(spark, GoldTransformer.artists(bronze_df), "artists")
    wr.write_df(spark, GoldTransformer.tracks(bronze_df), "tracks")
    wr.write_df(spark, GoldTransformer.album_artist(bronze_df), "album_artist")
    wr.write_df(spark, GoldTransformer.labels(bronze_df), "labels")
    wr.write_df(spark, GoldTransformer.works(bronze_df), "works")
    wr.write_df(spark, GoldTransformer.album_work(bronze_df), "album_work")
    wr.write_df(spark, GoldTransformer.album_label(bronze_df), "album_label")

    print("[Gold] Tables created.")


def main():
    """
        Manage the full pipeline run end-to-end.

        Steps:
            1) Load the pipeline configuration from `config.json`.
            2) Build a SparkSession using the provided Spark config.
            3) Run the Bronze ingestion (one-shot streaming).
            4) Ensure the Bronze table is registered in the metastore even if no batch ran.
            5) Preview a small sample from Bronze (via table or path as a fallback).
            6) Run Gold transformations and write all Gold tables.
            7) Print quick sanity checks (samples and row counts) for key Gold tables.

        Side effects:
            - Writes/overwrites datasets under Bronze and Gold paths.
            - Registers/re-registers tables in the metastore.

        Returns:
            None.
    """

    # 0) Load configuration
    with open("config.json") as f:
        config: dict = json.load(f)

    # 1) Build SparkSession
    spark_conf: dict = config.get("spark_conf", {})
    spark: SparkSession = build_spark_from_config(spark_conf)

    # 2) Bronze first (one-shot ingestion)
    run_bronze(spark, config)

    # 2.1) Ensure bronze.<subdir> is registered even if no batch was produced
    table, path, using_fmt = ensure_table_registered(spark, config)

    # 2.2) Show a sample from the table if it exists; otherwise read by path as a fallback
    if spark.catalog.tableExists(table):

        print("\n=== bronze.albums ===")
        spark.table(table).show(10, truncate=False)
        print("Number of rows in bronze.albums:",
        spark.sql("SELECT COUNT(*) AS cnt FROM bronze.albums").collect()[0]["cnt"])

    else:

        print(f"[Warn] Table {table} not registered yet. Reading from path ({using_fmt}): {path}")
        fmt: str = config.get("bronze_options", {}).get("format", "delta")
        spark.read.format(fmt).load(path).show(10, truncate=False)

    # 3) Gold transformations
    run_gold(spark, config)

    # Quick looks at Gold tables

    print("\n=== gold.albums ===")
    spark.sql("SELECT * FROM gold.albums LIMIT 10").show(truncate=False)
    print("Number of rows in gold.albums:", spark.sql("SELECT COUNT(*) AS cnt FROM gold.albums").collect()[0]["cnt"])

    print("\n=== gold.artists ===")
    spark.sql("SELECT * FROM gold.artists LIMIT 10").show(truncate=False)
    print("Number of rows in gold.artists:", spark.sql("SELECT COUNT(*) AS cnt FROM gold.artists").collect()[0]["cnt"])

    print("\n=== gold.tracks ===")
    spark.sql("SELECT * FROM gold.tracks LIMIT 10").show(truncate=False)
    print("Number of rows in gold.tracks:", spark.sql("SELECT COUNT(*) AS cnt FROM gold.tracks").collect()[0]["cnt"])

    print("\n=== gold.album_artist ===")
    spark.sql("SELECT * FROM gold.album_artist LIMIT 10").show(truncate=False)
    print("Number of rows in gold.album_artist:",
          spark.sql("SELECT COUNT(*) AS cnt FROM gold.album_artist").collect()[0]["cnt"])

    print("\n=== gold.labels ===")
    spark.sql("SELECT * FROM gold.labels LIMIT 10").show(truncate=False)
    print("Number of rows in gold.labels:", spark.sql("SELECT COUNT(*) AS cnt FROM gold.labels").collect()[0]["cnt"])

    print("\n=== gold.works ===")
    spark.sql("SELECT * FROM gold.works LIMIT 10").show(truncate=False)
    print("Number of rows in gold.works:", spark.sql("SELECT COUNT(*) AS cnt FROM gold.works").collect()[0]["cnt"])

    print("\n=== gold.album_work ===")
    spark.sql("SELECT * FROM gold.album_work LIMIT 10").show(truncate=False)
    print("Number of rows in gold.album_work:",
          spark.sql("SELECT COUNT(*) AS cnt FROM gold.album_work").collect()[0]["cnt"])

    spark.stop()

if __name__ == "__main__":
    main()
