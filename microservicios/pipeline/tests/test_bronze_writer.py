from pathlib import Path
import pytest
from pyspark.sql import SparkSession, Row
from pipeline.bronze_stream_writer import BronzeStreamWriter

# ---------- Fixtures ----------

def _resolve_jars_dir() -> dict:
    # Default: microservicios/deps/jars (up from tests/)
    # microservicios/<servicio>/tests/conftest.py -> parents[2] == microservicios
    microservicios_dir = Path(__file__).resolve().parents[2]
    jars_dir = microservicios_dir / "deps" / "jars"
    jars_dir.mkdir(parents=True, exist_ok=True)
    return {
        "delta-spark_2.12-3.2.0.jar": jars_dir / "delta-spark_2.12-3.2.0.jar",
        "delta-storage-3.2.0.jar": jars_dir / "delta-storage-3.2.0.jar",
    }


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    paths = _resolve_jars_dir()
    wh = tmp_path_factory.mktemp("wh")

    jar1 = paths['delta-spark_2.12-3.2.0.jar'].resolve().as_uri()
    jar2 = paths['delta-storage-3.2.0.jar'].resolve().as_uri()

    spark = (
        SparkSession.builder
        .appName("bronze-writer-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", wh.as_posix())
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

        },
        "spark_conf": {}
    }


def _df(spark):
    data = [
        Row(artists="A", title="T1", id=1, year="1959", label="L", tracklist=["x"], musicians=["m"], leaders=["A"], style=["hard-bop"], cover_url="u"),
        Row(artists="B", title="T2", id=2, year="1960", label="L", tracklist=["y"], musicians=["n"], leaders=["B"], style=["bop"],      cover_url="v"),
    ]
    return spark.createDataFrame(data)


# ---------- Tests ----------

def test_first_write_creates_delta_table_and_registers_in_metastore(spark, base_config):
    writer = BronzeStreamWriter(base_config)
    path = Path(writer.dataset_bronze_path)

    df = _df(spark)
    writer.write_data(spark, df)

    # files were saved and table was created
    assert path.exists()
    assert (path / "_delta_log").exists()

    # table is registered in metastore
    assert spark.catalog.tableExists(writer.table)

    # Read from delta table gets the same results as df
    out = spark.read.format("delta").load(path.as_posix())
    assert out.count() == df.count()


def test_second_write_appends_without_recreate(spark, base_config):
    writer = BronzeStreamWriter(base_config)
    path = Path(writer.dataset_bronze_path)

    df1 = _df(spark)
    writer.write_data(spark, df1)

    # Second batch
    df2 = _df(spark).limit(1)  # take only one row
    writer.write_data(spark, df2)

    out = spark.read.format("delta").load(path.as_posix())
    assert out.count() == df1.count() + df2.count()  # append mode

    # table exists and was not created again
    assert spark.catalog.tableExists(writer.table)


def test_str_contains_table_and_path(base_config):
    w = BronzeStreamWriter(base_config)
    s = str(w)
    assert "BronzeStreamWriter(" in s
    assert f"table='{w.table}'" in s
    assert f"path='{w.dataset_bronze_path}'" in s