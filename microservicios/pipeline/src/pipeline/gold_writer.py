from pathlib import Path
from typing import Any, Dict
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter


class GoldWriter:
    """
        Write DataFrames to the Gold layer.

        It writes each DataFrame to a path `<gold>/<name>` and registers
        (or re-registers) the table as `gold.<name>` in the metastore.
        Options like format/mode/mergeSchema come from `gold_options`.
    """

    def __init__(self, config: dict):
        """
            Init writer from config.

            Expects:
                - paths.gold
                - gold_options: { format, mode, mergeSchema, ... }
        """

        self.config: Dict[str, Any] = config
        self.base: Path = Path(config["paths"]["gold"])

        opts: Dict[str, Any] = dict(config.get("gold_options", {}))
        self.format: str = str(opts.pop("format", "delta")).lower()
        self.mode: str = str(opts.pop("mode", "overwrite")).lower()
        opts.pop("path", None)  # avoid conflicts with our .option("path", ...)
        self.options: Dict[str, Any] = opts

    def write_df(self, spark: SparkSession, df: DataFrame, name: str) -> None:
        """
            Persist a DataFrame as a Gold table.

            Steps:
                1) Write to `<gold>/<name>` with the configured format/mode/options.
                2) Ensure database `gold` exists.
                3) Recreate table `gold.<name>` pointing to that path.

            Args:
                spark: active SparkSession.
                df: DataFrame to write.
                name: logical table name (e.g., "albums").
        """

        safe: str = name.strip().lower().replace(" ", "_")
        path: str = (self.base / safe).as_posix()
        table: str = f"gold.{safe}"
        Path(self.base / safe).mkdir(parents=True, exist_ok=True)

        writer: DataFrameWriter = (
            df.write
            .format(self.format)
            .mode(self.mode)  # usually "overwrite" per run
            .options(**self.options)  # e.g., mergeSchema="true"
            .option("path", path)
        )
        writer.save()  # creates/updates _delta_log

        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"CREATE TABLE {table} USING {self.format.upper()} LOCATION '{path}'")

