import os
from pathlib import Path
import pytest
from pyspark.sql import Row, functions as F
from urllib.parse import urlparse, unquote
from pipeline.gold_writer import GoldWriter


# ---------- Fixtures ----------

def _resolve_jars_dir() -> dict:
    # Default: microservicios/deps/jars (up from tests/)
    microservicios_dir = Path(__file__).resolve().parents[2]
    jars_dir = microservicios_dir / "deps" / "jars"
    jars_dir.mkdir(parents=True, exist_ok=True)
    return {
        "delta-spark_2.12-3.2.0.jar": jars_dir / "delta-spark_2.12-3.2.0.jar",
        "delta-storage-3.2.0.jar": jars_dir / "delta-storage-3.2.0.jar",
    }


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    paths = _resolve_jars_dir()
    wh = tmp_path_factory.mktemp("wh")
    local = tmp_path_factory.mktemp("spark_local")

    jar1 = paths["delta-spark_2.12-3.2.0.jar"].resolve().as_uri()
    jar2 = paths["delta-storage-3.2.0.jar"].resolve().as_uri()

    spark = (
        SparkSession.builder
        .appName("gold-writer-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", wh.as_posix())
        .config("spark.local.dir", local.as_posix())
        .config("spark.jars", f"{jar1},{jar2}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def tmp_datalake(tmp_path):
    base = tmp_path / "datalake"
    paths = {}
    for layer in ("landing", "raw", "bronze", "gold"):
        p = base / layer
        p.mkdir(parents=True, exist_ok=True)
        paths[layer] = p.as_posix()
    return paths


@pytest.fixture
def base_config(tmp_datalake):

    return {
        "paths": tmp_datalake,
        "datasets": {
            "albums": {
                "type": "json",
                "subdir": "albums",
                "schema": "artists STRING, title STRING, id LONG, year STRING, label STRING, tracklist ARRAY<STRING>, musicians ARRAY<STRING>, leaders ARRAY<STRING>, style ARRAY<STRING>, cover_url STRING",
                "reader_options": {
                    "multiLine": "true",
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt",
                    "maxFilesPerTrigger": "100"
                }
            }
        },
        "bronze_options": {
            "format": "delta",
            "mode": "append",
            "mergeSchema": "true"
        },
        "gold_options": {
            "format": "delta",
            "mode": "overwrite",
            "mergeSchema": "true"
        },
        "spark_conf": {}
    }

# ----------- Helper ------------

def _to_fs_path(loc: str) -> Path:
    """
    Transform a location, which can be given as a Uri, in a Path objetc
    """
    if loc.startswith("file:"):
        p = urlparse(loc)
        raw = unquote(p.path)

        if os.name == "nt" and raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
            raw = raw[1:]
        return Path(raw).resolve()

    return Path(loc).resolve()

# ---------- Tests ----------

def test_write_df_creates_delta_and_registers_table(spark, base_config, tmp_datalake):
    """
    - Save a DF in gold/<name> as delta table.
    - Register the table in the metastore as 'gold.<name>'.
    """
    writer = GoldWriter(base_config)

    data = spark.createDataFrame([
        Row(album_id=1, title="A"),
        Row(album_id=2, title="B"),
    ])

    name = "Albums"  # check normalization
    writer.write_df(spark, data, name)

    # expected filesystem path (NO URI)
    safe = name.strip().lower().replace(" ", "_")
    expected_fs = (Path(base_config["paths"]["gold"]) / safe).resolve()

    # check folder and _delta_log existence
    assert expected_fs.exists()
    assert (expected_fs / "_delta_log").exists()

    # Table should be registered in metastore and point to the path
    detail = spark.sql(f"DESCRIBE DETAIL gold.{safe}").first().asDict()
    actual_fs = _to_fs_path(detail["location"])

    assert actual_fs == expected_fs
    assert detail["format"] == "delta"
    assert detail["numFiles"] >= 1
    assert detail["partitionColumns"] == []

def test_write_df_normalizes_table_name(spark, base_config, tmp_datalake):

    writer = GoldWriter(base_config)
    name = "  Album Works  "  # extra spaces

    data = spark.createDataFrame([Row(album_id=1, work_id="x")])
    writer.write_df(spark, data, name)

    safe = name.strip().lower().replace(" ", "_")
    expected_fs = (Path(base_config["paths"]["gold"]) / safe).resolve()

    # check table exists
    assert spark.catalog.tableExists(f"gold.{safe}")

    # correct location
    detail = spark.sql(f"DESCRIBE DETAIL gold.{safe}").first().asDict()
    actual_fs = _to_fs_path(detail["location"])
    assert actual_fs == expected_fs