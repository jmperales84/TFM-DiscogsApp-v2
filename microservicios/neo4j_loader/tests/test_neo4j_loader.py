from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from neo4j_loader.main import ensure_constraints, to_rows, get_rows_or_skip, batched

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
        .appName("neo4j-loader-tests")
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

# --------------------------
# Mocks básicos de Neo4j
# --------------------------

class FakeSession:
    def __init__(self):
        self.queries = []
    def run(self, q, *args, **kwargs):
        # Guardamos la query tal cual (string o Query)
        self.queries.append((q, args, kwargs))
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

class FakeDriver:
    def __init__(self):
        self.sessions = []
    def session(self):
        s = FakeSession()
        self.sessions.append(s)
        return s
    def close(self):
        pass

# --------------------------
# Tests
# --------------------------

def test_ensure_constraints_calls_expected_queries():
    driver = FakeDriver()
    ensure_constraints(driver)

    # Se ha abierto una sesión
    assert len(driver.sessions) == 1
    sess = driver.sessions[0]

    # 4 sentencias en orden
    expected = [
        "CREATE CONSTRAINT album_id IF NOT EXISTS FOR (a:Album) REQUIRE a.album_id IS UNIQUE",
        "CREATE CONSTRAINT artist_id IF NOT EXISTS FOR (p:Artist) REQUIRE p.artist_id IS UNIQUE",
        "CREATE CONSTRAINT work_id   IF NOT EXISTS FOR (w:Work)   REQUIRE w.work_id IS UNIQUE",
        "CREATE CONSTRAINT label_id   IF NOT EXISTS FOR (l:Label)   REQUIRE l.label_id IS UNIQUE",
    ]
    got = []
    for (q, _, _) in sess.queries:
        if hasattr(q, "query"):
            got.append(q.query)
        else:
            got.append(q)
    assert got == expected


def test_batched_chunks_sizes():
    data = list(range(7))
    chunks = list(batched(data, 3))
    assert chunks == [[0,1,2], [3,4,5], [6]]

def test_batched_empty_iterable():
    assert list(batched([], 2)) == []

def test_batched_n_1():
    data = [10,11,12]
    assert list(batched(data, 1)) == [[10],[11],[12]]


def test_to_rows_extracts_selected_columns(spark):
    df = spark.createDataFrame([
        Row(a=1, b="x", c=True),
        Row(a=2, b="y", c=False),
    ])
    rows = to_rows(df, ["a","b"])
    assert rows == [{"a":1,"b":"x"}, {"a":2,"b":"y"}]


def test_get_rows_or_skip_missing_columns_returns_none(capsys, spark):
    df = spark.createDataFrame([Row(a=1, b="x")])
    res = get_rows_or_skip(df, ["a","c"], table="albums", entry_name="Album")
    assert res is None
    out = capsys.readouterr().out
    assert "[WARN] albums: Missing columns ['c']" in out

def test_get_rows_or_skip_empty_returns_none(capsys, spark):
    df = spark.createDataFrame([], schema="a INT, b STRING")
    res = get_rows_or_skip(df, ["a","b"], table="albums", entry_name="Album")
    assert res is None
    out = capsys.readouterr().out
    assert "[INFO] albums: 0 rows for Album" in out

def test_get_rows_or_skip_ok_returns_rows(spark):
    df = spark.createDataFrame([Row(album_id=1, artist_id="X"), Row(album_id=2, artist_id="Y")])
    res = get_rows_or_skip(df, ["album_id","artist_id"], table="album_artist", entry_name="PLAYS_IN")
    assert res == [{"album_id":1, "artist_id":"X"}, {"album_id":2, "artist_id":"Y"}]
