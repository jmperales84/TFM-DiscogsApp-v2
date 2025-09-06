from pathlib import Path
from typing import List
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql import DataFrame, SparkSession

class BronzeStreamWriter:
    """
        Main class to write raw data into the Bronze layer.

        This class manages how data is saved into the Bronze Delta tables:
            - Detects if the dataset needs initialization (first write).
            - Applies partitioning rules when the table is created.
            - Writes data in Delta (or other configured format) using append mode
            for subsequent batches.
            - Registers the table in the Spark metastore (idempotent).

        Config dict should include:
            - paths: {"bronze": str}
            - datasets: {"albums": {"subdir": str}}
            - bronze_options: {
                    "format": str (default "delta"),
                    "mode": str (default "append"),
                    "partitionBy": str or [str], optional,
                    other options like mergeSchema
                }

        Notes:
            - If format is Delta, a mergeSchema="true" option is set by default.
            - Partitioning only takes effect on the first write (creation).
            - The class ensures the database "bronze" exists and the table is
                always registered after each batch.
    """

    def __init__(self, config: dict) -> None:
        """
            Initialize the BronzeStreamWriter from a configuration dictionary.

            Settings:
                - Paths:
                    * bronze base path from `paths.bronze`
                    * dataset subdirectory from `datasets.albums.subdir`
                    * full dataset path `<bronze>/<subdir>`
                    * checkpoint location under the dataset path (`_checkpoint`)
                - Table name in the metastore: `bronze.<subdir>`
                - Write options (from `bronze_options`):
                    * format  (default: "delta", lowercased)
                    * mode    (default: "append")
                    * partitionBy (optional: str or list of str)
                    * any other writer options (e.g., mergeSchema)
                    * ignores a `path` key if present in options
                If format is Delta, ensures `mergeSchema="true"` by default.

            It also ensures the target dataset directory exists on disk.

            Args:
                config: Configuration dictionary with:
                    - paths: {"bronze": str}
                    - datasets: {"albums": {"subdir": str}}
                    - bronze_options: dict with writer options (optional)

            Side effects:
                    /<subdir>` directory if missing.

            Returns:
                None.
        """

        self.config: dict = config
        bronze_base: Path = Path(config["paths"]["bronze"])
        ds_cfg: dict = config["datasets"]["albums"]
        subdir: str = ds_cfg["subdir"]

        self.dataset_bronze_path: str = (bronze_base / subdir).as_posix()
        self.dataset_checkpoint_location: str = f"{self.dataset_bronze_path}/_checkpoint"
        self.table: str = f"bronze.{subdir}"

        bronze_opts: dict = dict(config.get("bronze_options", {}))
        self.format: str = bronze_opts.pop("format", "delta").lower()
        self.mode: str = bronze_opts.pop("mode", "append")
        self.partition_by: List[str] | None = bronze_opts.pop("partitionBy", None)
        bronze_opts.pop("path", None)

        if self.format == "delta":

            bronze_opts.setdefault("mergeSchema", "true")

        self.bronze_options = bronze_opts

        Path(bronze_base / subdir).mkdir(parents=True, exist_ok=True)

    def __str__(self):

        return f"BronzeStreamWriter(table='{self.table}', path='{self.dataset_bronze_path}')"

    def _is_delta_table_path(self) -> bool:
        """
            Determine whether the target dataset path contains a Delta table.

            A Delta table is identified by the presence of the `_delta_log`
            directory at the root of the dataset path.

            Returns:
                True if `_delta_log` exists under the dataset path; False otherwise.
        """

        return Path(self.dataset_bronze_path, "_delta_log").exists()

    def _path_has_files(self) -> bool:
        """
            Check whether the Bronze dataset directory exists and is non-empty.

            This helper returns True if the target path exists and contains at least
            one entry (file or subdirectory). It is used for non-Delta formats to
            decide whether a first-time initialization is required.

            Returns:
                True if the path exists and has entries; False otherwise.
        """

        p: Path = Path(self.dataset_bronze_path)

        return p.exists() and any(p.iterdir())

    def _needs_initialization(self) -> bool:
        """
            Determine whether the Bronze dataset path requires first-time initialization.

            For Delta format:
                - Returns True if the `_delta_log` directory is missing, which indicates
                that no Delta table has been created at this path yet.

            For non-Delta formats:
                - Returns True if the target path does not exist or contains no files.

            This check guides the first-write behavior (e.g., applying partitioning,
            choosing overwrite/append strategy, and registering the table).

            Returns:
                True if the dataset needs an initial write; False otherwise.
        """

        if self.format == "delta":

            return not self._is_delta_table_path()

        else:

            return not self._path_has_files()

    def write_data(self, spark: SparkSession, df: DataFrame) -> None:
        """
            Write a batch of data into the Bronze dataset path and register the table.

            Steps:
                1) Ensure the `bronze` database exists.
                2) Detect first-time initialization via `_needs_initialization()`.
                    - If True: write in overwrite mode, set the target path explicitly,
                    and apply `partitionBy` if configured (only effective at creation).
                    - If False: write using the configured mode (typically "append");
                    for Delta, `partitionBy` is ignored in append.
                3) Idempotently register the table in the metastore with
                     CREATE TABLE IF NOT EXISTS ... USING <format> LOCATION <path>`.

            Args:
                spark: active SparkSession.
                df: Spark DataFrame to persist.

            Side effects:
                - Writes files under the Bronze path (Delta/Parquet, etc.).
                - Ensures the table `bronze.<subdir>` is present in the metastore.

            Returns:
                None.
        """

        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

        init: bool = self._needs_initialization()

        if init:

            # First write: I prefer overwrite to guarantee a clean path (removes any non-Delta leftovers).
            # If you can guarantee the directory is empty, append would also work here.
            writer: DataFrameWriter = (df.write
                        .format(self.format)
                        .mode("overwrite")
                        .options(**self.bronze_options)
                        .option("path", self.dataset_bronze_path))

            writer.save()

        else:

            writer = (df.write
                        .format(self.format)
                        .mode(self.mode)    # typically "append"
                        .options(**self.bronze_options))
            writer.save(self.dataset_bronze_path)

        using_fmt: str = "DELTA" if self.format == "delta" else self.format.upper()
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.table} "
            f"USING {using_fmt} LOCATION '{self.dataset_bronze_path}'"
        )

    def append_2_bronze(self, spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
        """
            Handle one micro-batch and persist it into the Bronze layer.

            Steps:
                - Compact the micro-batch to a small, fixed number of partitions
                    (coalesce(4)) to reduce small files in the data lake.
                - Delegate the actual write to `write_data`.

            Args:
                spark: active SparkSession.
                batch_df: micro-batch DataFrame provided by `foreachBatch`.
                batch_id: sequential identifier of the micro-batch (starts at 0).

            Returns:
                None.
        """

        batch_df: DataFrame = batch_df.coalesce(4)

        self.write_data(spark, batch_df)
