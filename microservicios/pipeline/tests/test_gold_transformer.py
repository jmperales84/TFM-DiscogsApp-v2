import hashlib
import pytest
from pathlib import Path
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pipeline.gold_transformer import GoldTransformer, _norm_title, _strip_take_parens

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



# --------------------------------------------------------------------------------------
# Helpers: build a small bronze_df that covers most of the cases
# --------------------------------------------------------------------------------------

def _bronze_rows():
    # Album 1 con variantes de takes y nombres
    a1 = Row(
        id=1,
        artists="Hank Mobley",
        title="Workout",
        year="1961",
        label="Blue Note",
        style=["Hard Bop"],
        cover_url="http://example/cover1.jpg",
        # examples with (take ...)
        tracklist=["Uh, Huh (Take 1)", "Uh, Huh (take 2)", "The Best Things In Life Are Free"],
        # include some elements with wrong characters
        musicians=["Hank Mobley", "Grant Green", "Wynton Kelly", "Paul Chambers", "Philly Joe Jones", "HÁNK   MÓBLEY"],
        leaders=["Hank Mobley"],
    )

    # Album #2 with extra space at the endo to try trim and hashing
    a2 = Row(
        id=2,
        artists="Lee Morgan",
        title="Cornbread",
        year="1965",
        label="Blue Note ",
        style=["Hard Bop"],
        cover_url="http://example/cover2.jpg",
        tracklist=["Ceora", "Ill Wind (Take One)"],
        musicians=["Lee Morgan", "Hank Mobley", "Herbie Hancock", "Larry Ridley", "Billy Higgins"],
        leaders=["Lee Morgan"],
    )
    return [a1, a2]


@pytest.fixture
def bronze_df(spark):
    return spark.createDataFrame(_bronze_rows())


# --------------------------------------------------------------------------------------
# 1) Tests auxiliary functions
# --------------------------------------------------------------------------------------

def test_norm_title_basic(spark):
    df = spark.createDataFrame(
        [("  Quién      vive?  ",), ("Café-con-leche",)],
        ["txt"]
    ).select(_norm_title(F.col("txt")).alias("norm"))
    assert set(r["norm"] for r in df.collect()) == {"quien vive", "cafe con leche"}


@pytest.mark.parametrize("raw,clean", [
    ("Song (take 2)", "Song"),
    ("SONG   (Alt Take)   ", "SONG"),
    ("(master take) Song", "Song"),
    ("Song (Take One) extra", "Song extra"),
    ("No parens here", "No parens here"),
])
def test_strip_take_parens_variants(spark, raw, clean):
    df = spark.createDataFrame([(raw,)], ["t"]).select(_strip_take_parens(F.col("t")).alias("x"))
    assert df.first()["x"] == clean


# --------------------------------------------------------------------------------------
# 2) Dimension tables
# --------------------------------------------------------------------------------------

def test_albums_dimension(bronze_df):
    df = GoldTransformer.albums(bronze_df)
    rows = {r["album_id"]: r.asDict() for r in df.collect()}
    assert set(rows.keys()) == {1, 2}
    assert rows[1]["year"] == 1961 and isinstance(rows[1]["year"], int)
    assert rows[2]["title"] == "Cornbread"
    # trims
    assert rows[2]["label"] == "Blue Note"


def test_labels_dimension(bronze_df):
    df = GoldTransformer.labels(bronze_df).orderBy("name")
    data = df.collect()
    # Result has to be "Blue Note" after trim and dedupe
    assert len(data) == 1
    name = data[0]["name"]
    label_id = data[0]["label_id"]
    expected = hashlib.sha256(name.lower().encode("utf-8")).hexdigest()
    assert name == "Blue Note"
    assert label_id == expected


def test_artists_dimension(bronze_df, spark):
    df = GoldTransformer.artists(bronze_df)
    # Result: Hank Mobley, Grant Green, Wynton Kelly, Paul Chambers, Philly Joe Jones,
    # Lee Morgan, Herbie Hancock, Larry Ridley, Billy Higgins
    # "HÁNK   MÓBLEY" should not create any additional artist.
    names = set(r["name"] for r in df.collect())
    expected_core = {
        "Hank Mobley", "Grant Green", "Wynton Kelly", "Paul Chambers", "Philly Joe Jones",
        "Lee Morgan", "Herbie Hancock", "Larry Ridley", "Billy Higgins"
    }

    assert expected_core.issubset(names)
    assert len(names) == len(expected_core)


def test_works_dimension(bronze_df):
    df = GoldTransformer.works(bronze_df)
    works = {r["work_title"] for r in df.collect()}
    # Tracks with suffixes such as "Uh, Huh (take ...)" should be kept as "Uh, Huh"
    assert "Uh, Huh" in works
    assert "Ill Wind" in works
    assert "Ceora" in works
    assert "The Best Things In Life Are Free" in works
    assert all("(take" not in w.lower() for w in works)


# --------------------------------------------------------------------------------------
# 3) Fact tables
# --------------------------------------------------------------------------------------

def test_tracks_facts(bronze_df):
    df = GoldTransformer.tracks(bronze_df)
    t = df.filter(F.col("album_id") == 1).select("name").collect()
    names = {r["name"] for r in t}
    # In album #1, the two tracks with "(take ...)" should be saved as "Uh, Huh"
    assert "Uh, Huh" in names
    # Try the second track
    assert "The Best Things In Life Are Free" in names
    # 2 tracks for the first album
    assert len(names) == 2


def test_album_artist_facts(bronze_df):
    df = GoldTransformer.album_artist(bronze_df)
    # Check different roles (album_id, artist_id, role)
    # Example: in album #1 Hank Mobley should be as leader as well as a musician
    roles_per_album = (
        df.groupBy("album_id", "role").agg(F.countDistinct("artist_id").alias("n"))
        .orderBy("album_id", "role")
        .collect()
    )
    # Both albums must contain both roles
    combo = {(r["album_id"], r["role"]) for r in roles_per_album}
    assert (1, "leader") in combo and (1, "musician") in combo
    assert (2, "leader") in combo and (2, "musician") in combo


def test_album_work_facts(bronze_df):
    works_df = GoldTransformer.works(bronze_df).select("work_id").distinct()
    aw_df = GoldTransformer.album_work(bronze_df).select("work_id").distinct()
    # Every work_id from album_work must appear in works table
    missing = aw_df.join(works_df, "work_id", "left_anti")
    assert missing.count() == 0


def test_album_label_facts(bronze_df):
    df = GoldTransformer.album_label(bronze_df)
    # Both album should contain the same label_id for the same label
    ids = df.collect()
    assert len(ids) == 2
    assert ids[0]["label_id"] == ids[1]["label_id"]


# --------------------------------------------------------------------------------------
# 4) Bronze reader
# --------------------------------------------------------------------------------------

def test_bronze_reader_reads_delta_dataset(spark, base_config, tmp_datalake):
    #
    subdir = base_config["datasets"]["albums"]["subdir"]
    bronze_path = Path(tmp_datalake["bronze"]) / subdir
    bronze_path.mkdir(parents=True, exist_ok=True)

    data = spark.createDataFrame(_bronze_rows())
    data.write.format("delta").mode("overwrite").save(bronze_path.as_posix())

    gt = GoldTransformer(base_config)
    read_df = gt.bronze_reader(spark)


    assert {"id", "artists", "title", "year", "label", "tracklist", "musicians", "leaders"}.issubset(set(read_df.columns))
    assert read_df.count() == 2