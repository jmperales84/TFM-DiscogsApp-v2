from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.functions import current_timestamp, input_file_name
from typing import Dict, List, Any, Optional


class RawStreamReader:

    def __init__(self, config: dict):
        """
            Initialize the RawStreamReader from a configuration dictionary.

            It resolves the main paths (landing, raw, bronze) and the dataset
            settings for `albums` (format, subdirectory, optional schema DDL,
            and reader options). It also computes the raw input path
            (`raw/<subdir>`) and ensures the Bronze base directory exists.

            Expected config keys:
                - paths: {"landing": str, "raw": str, "bronze": str}
                - datasets: {
                        "albums": {
                            "type": "json",
                            "subdir": str,
                            "schema": Optional[str],
                            "reader_options": Dict[str, str]
                        }
                    }

            Args:
                config: configuration dictionary with the keys listed above.

            Side effects:
                - Creates the Bronze base directory if it does not exist.

            Returns:
                None.
        """

        self.landing_path: Path = Path(config["paths"]["landing"])
        self.raw_path: Path = Path(config["paths"]["raw"])
        self.bronze_path: Path = Path(config["paths"]["bronze"])

        ds_cfg: Dict[str, Any] = config["datasets"]["albums"]
        self.subdir: str = ds_cfg["subdir"]
        self.format: str = ds_cfg["type"]
        self.schema_ddl: Optional[str] = ds_cfg.get("schema")
        self.reader_options: Dict[str, str] = ds_cfg.get("reader_options", {})

        self.raw_albums_path: Path = self.raw_path / self.subdir
        self.bronze_path.mkdir(parents=True, exist_ok=True)

    def __str__(self):

        return f"RawStreamReader(raw_albums_path='{self.raw_albums_path}', bronze_path='{self.bronze_path}')"

    @staticmethod
    def add_metadata_columns(df: DataFrame) -> DataFrame:
        """
            Add ingestion metadata columns to a DataFrame.

            This method adds two extra columns:
                - _ingested_at: current timestamp when the data is read.
                - _ingested_filename: name of the source file.

            It also reorders the columns so that metadata columns
            appear first, followed by the original data columns.

            Args:
                df: input Spark DataFrame.

            Returns:
                A new Spark DataFrame with the metadata columns included.
        """

        df: DataFrame = (df
              .withColumn("_ingested_at", current_timestamp())
              .withColumn("_ingested_filename", input_file_name()))
        meta: List[str] = ["_ingested_at", "_ingested_filename"]
        data_cols: List[str] = [c for c in df.columns if c not in meta]

        return df.select(*(meta + data_cols))

    def read_json_stream(self, spark: SparkSession) -> DataFrame:
        """
            Build a streaming DataFrame reader for album JSON files.

            The reader is configured with:
                - Format set to "json".
                - Optional schema defined in config (if present).
                - Extra reader options passed from config, such as
                columnNameOfCorruptRecord or maxFilesPerTrigger.

            Args:
                spark: active SparkSession.

            Returns:
                A streaming DataFrame created from the raw/albums path.
        """

        reader: DataStreamReader = spark.readStream.format("json")

        # apply data schema if exists
        if self.schema_ddl:

            reader = reader.schema(self.schema_ddl)

        # apply config options
        reader = reader.options(**self.reader_options)

        return reader.load(self.raw_albums_path.as_posix())

    def read(self, spark: SparkSession) -> DataFrame:
        """
            Read the raw dataset as a streaming DataFrame.

            Steps:
                1. Check that the configured format is supported (only "json" for now).
                2. Use `read_json_stream` to build the reader.
                3. Add ingestion metadata columns with `add_metadata_columns`.

            Args:
                spark: active SparkSession.

            Returns:
                A streaming DataFrame with metadata columns included.
        """

        if self.format != "json":

            raise Exception(f"Format {self.format} not supported")

        df: DataFrame = self.read_json_stream(spark)

        return df.transform(self.add_metadata_columns)

