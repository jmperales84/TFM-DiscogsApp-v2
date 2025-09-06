import os
import pytest
from pathlib import Path
from pyspark.sql import Row, SparkSession

from pipeline.raw_stream_reader import RawStreamReader
from pipeline.main import build_spark_from_config

import os
import pytest
from pathlib import Path
from pyspark.sql import Row, SparkSession

from pipeline.raw_stream_reader import RawStreamReader


@pytest.fixture(scope="session")
def spark():

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = (
        SparkSession.builder
        .appName("pipeline-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    spark.sparkContext.setLogLevel("WARN")
    return spark


@pytest.fixture
def config(tmp_path):

    landing = tmp_path / "landing"
    raw = tmp_path / "raw"
    bronze = tmp_path / "bronze"

    # >>> CLAVE: crear el directorio que va a leer el stream
    (raw / "albums").mkdir(parents=True, exist_ok=True)
    landing.mkdir(parents=True, exist_ok=True)
    bronze.mkdir(parents=True, exist_ok=True)

    return {
        "paths": {
            "landing": str(landing),
            "raw": str(raw),
            "bronze": str(bronze),
        },
        "datasets": {
            "albums": {
                "type": "json",
                "subdir": "albums",
                "schema": "id LONG, title STRING",
                "reader_options": {
                    "multiLine": "true",
                    "mode": "PERMISSIVE",
                },
            }
        },
    }


def test_init_creates_bronze(config):
    reader = RawStreamReader(config)
    assert Path(config["paths"]["bronze"]).exists()
    assert Path(reader.raw_albums_path).as_posix().endswith("raw/albums")


def test_str_repr(config):
    reader = RawStreamReader(config)
    text = str(reader)
    assert "raw_albums_path" in text
    assert "bronze_path" in text


def test_add_metadata_columns(spark, config):
    reader = RawStreamReader(config)
    df = spark.createDataFrame([Row(id=1, title="test")])

    df2 = reader.add_metadata_columns(df)

    cols = df2.schema.names
    # metadata first
    assert cols[0] == "_ingested_at"
    assert cols[1] == "_ingested_filename"
    # keep data cols
    assert "id" in cols
    assert "title" in cols


def test_read_json_stream_returns_streaming(spark, config, capsys):

    reader = RawStreamReader(config)

    Path(config["paths"]["raw"], "albums").mkdir(parents=True, exist_ok=True)
    sdf = reader.read_json_stream(spark)
    assert sdf.isStreaming is True

    sdf.explain(True)
    out = capsys.readouterr().out.replace("\\", "/")
    assert reader.raw_albums_path.as_posix() in out


def test_read_adds_metadata(spark, config):
    reader = RawStreamReader(config)

    raw_dir = Path(config["paths"]["raw"]) / "albums"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "one.json").write_text('{"id": 1, "title": "test"}\n', encoding="utf-8")

    sdf = reader.read(spark)

    cols = sdf.schema.names
    assert "_ingested_at" in cols
    assert "_ingested_filename" in cols
    assert "id" in cols and "title" in cols