from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col, explode_outer, trim, lower, sha2, lit, regexp_replace, concat_ws, length
)
from pathlib import Path

# ------------------------
# Normalization helpers
# ------------------------

def _norm_title(c: Column) -> Column:
    """
        Normalize a text column for title matching and deduplication.

        Steps:
            - Trim leading/trailing spaces and convert to lowercase.
            - Replace Spanish accented vowels with their ASCII equivalents.
            - Replace 'ñ' with 'n'.
            - Remove punctuation (keep word characters and spaces).
            - Collapse multiple spaces into a single space.
            - Trim again to clean residual borders.

        Args:
            c: Spark Column expression containing the title text.

        Returns:
            A Spark Column with the normalized title.

    """

    c: Column = lower(trim(c))
    c = regexp_replace(c, "[áàäâ]", "a")
    c = regexp_replace(c, "[éèëê]", "e")
    c = regexp_replace(c, "[íìïî]", "i")
    c = regexp_replace(c, "[óòöô]", "o")
    c = regexp_replace(c, "[úùüû]", "u")
    c = regexp_replace(c, "ñ", "n")
    c = regexp_replace(c, r"[^\w\s]", " ")
    c = regexp_replace(c, r"\s+", " ")

    return trim(c)

def _strip_take_parens(c: Column) -> Column:
    """
        Remove parenthetical notes that mention the word 'take' from a text column.

        This helper drops any substring like:
            - "(take 2)", "(alt take)", "(master take)", "(Take One)"
        The match is case-insensitive and only applies to text inside parentheses.
        After removal, multiple spaces are collapsed and leading/trailing
        spaces are trimmed.

        Args:
            c: Spark Column containing the text.

        Returns:
            A Spark Column with 'take' parenthetical notes removed.

        Notes:
            - Regex used: (?i:\\s*\\([^)]*\\btake\\b[^)]*\\))
                * (?i: ...) → case-insensitive group
                * \\s*\\( ... \\) → a parenthesized block (optional leading spaces)
                * \\btake\\b → matches the whole word 'take'
    """

    pattern: str = r"(?i:\s*\([^)]*\btake\b[^)]*\))"
    c: Column = regexp_replace(c, pattern, "")
    c = regexp_replace(c, r"\s+", " ")

    return trim(c)

# ------------------------
# Transformer
# ------------------------

class GoldTransformer:
    """
        Build Gold tables from the Bronze dataset.

        This class reads data from the Bronze path (given in the config) and
        provides small helper methods that return clean DataFrames ready for
        the Gold layer.

        - Dimensions:
              * albums(bronze_df)
              * artists(bronze_df)
              * works(bronze_df)
              * labels(bronze_df)
        - Facts / link tables:
              * tracks(bronze_df)
              * album_artist(bronze_df)
              * album_work(bronze_df)

        The transformer relies on small text-normalization helpers (e.g.
        `_norm_title`, `_strip_take_parens`) to create stable, reproducible IDs
        via SHA-256 hashes (artist_id, label_id, work_id, track_id) and to
        deduplicate records.

        Expected input fields in Bronze (per album record):
            - id, artists, title, year, label
            - style: ARRAY<STRING>
            - cover_url: STRING
            - tracklist: ARRAY<STRING>
            - musicians: ARRAY<STRING>
            - leaders: ARRAY<STRING>

        Config dict should include:
            - paths: {"bronze": str}
            - datasets: {"albums": {"subdir": str}}
            - bronze_options: {"format": str}  # defaults to "delta" if missing

        Notes:
            - `bronze_reader(spark)` loads the Bronze dataset in **batch** mode.
            - All public methods return new DataFrames; this class does not write.
            - Text normalization removes accents/punctuation, collapses spaces,
              trims, and strips parenthetical “take” notes to improve matching.
    """


    def __init__(self, config: dict) -> None:
        """
            Initialize the GoldTransformer from a configuration dictionary.

            It stores the provided config and resolves the Bronze dataset path
            for the target subdirectory (e.g., `bronze/albums`), which will be
            used as the input for Gold transformations.

            Expected config keys:
                - paths: {"bronze": str}
                - datasets: {"albums": {"subdir": str}}

            Args:
                config: configuration dictionary with required paths and dataset info.

            Returns:
                None.
        """

        self.config: dict = config
        bronze_base: Path = Path(config["paths"]["bronze"])
        subdir: str = config["datasets"]["albums"]["subdir"]
        self.dataset_bronze_path: str = (bronze_base / subdir).as_posix()

    def bronze_reader(self, spark: SparkSession)-> DataFrame:
        """
            Load the Bronze dataset as a batch DataFrame.

            The input format is taken from `bronze_options.format`
            (defaults to "delta") and the data is loaded directly from
            the resolved Bronze path.

            Args:
                spark: active SparkSession.

            Returns:
                A Spark DataFrame with the Bronze dataset contents.

            Notes:
                - For Delta format, reading by path works without table registration
                because the transaction log (`_delta_log`) is discovered at runtime.
        """

        fmt: str = self.config.get("bronze_options", {}).get("format", "delta")
        return spark.read.format(fmt).load(self.dataset_bronze_path)

    # -------------------------------------------
    # Dimensions
    # -------------------------------------------

    @staticmethod
    def albums(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Albums dimension from the Bronze dataset.

            It selects and cleans the main album fields:
                - id        → album_id
                - artists   → ensemble (trimmed)
                - title     → title (trimmed)
                - year      → cast to int
                - label     → label (trimmed)
                - style     → styles (array, kept as is)
                - cover_url → kept as is

            Duplicates are removed by album_id so there is one row per album.

            Args:
                bronze_df: input DataFrame with album-level columns.

            Returns:
                A DataFrame for the Albums dimension with one row per album.
        """

        return (bronze_df
                .select(
                    col("id").alias("album_id"),
                    trim(col("artists")).alias("ensemble"),
                    trim(col("title")).alias("title"),
                    col("year").cast("int").alias("year"),
                    trim(col("label")).alias("label"),
                    col("style").alias("styles"),
                    col("cover_url")
                )
                .dropDuplicates(["album_id"]))

    @staticmethod
    def labels(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Labels dimension from the Bronze dataset.

            Steps:
                - Take the `label` column and rename it to `name`.
                - Trim spaces, drop null/empty names.
                - Deduplicate labels.
                - Create a stable `label_id` by hashing the lowercase name (SHA-256).

            Args:
                bronze_df: input DataFrame that contains the `label` column.

            Returns:
                A DataFrame with:
                    - label_id: SHA-256 hash of the lowercase label name.
                    - name: cleaned label name.
        """

        labels: DataFrame = bronze_df.select(col("label").alias("name"))

        return (labels
                .select(trim(col("name")).alias("name"))
                .where(col("name").isNotNull() & (length(col("name")) > 0))
                .dropDuplicates()
                .select(
                    sha2(lower(col("name")), 256).alias("label_id"),
                    col("name")
                ))

    @staticmethod
    def artists(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Artists dimension from the Bronze dataset.

            Steps:
                - Collect names from `musicians` and `leaders` arrays.
                - Trim spaces and drop null/empty names.
                - Normalize names with `_norm_title` to help deduplication.
                - Keep one row per normalized name.
                - Create a stable `artist_id` as SHA-256 of `norm_name`.

            Args:
                bronze_df: input DataFrame with `musicians` and `leaders` arrays.

            Returns:
                A DataFrame with:
                    - artist_id: SHA-256 hash of the normalized name.
                    - name: cleaned display name.
        """

        names: DataFrame = (bronze_df.select(explode_outer("musicians").alias("name"))
                 .union(bronze_df.select(explode_outer("leaders").alias("name"))))

        # dedup por forma normalizada del nombre
        clean: DataFrame = (names
                 .select(trim(col("name")).alias("name"))
                 .where(col("name").isNotNull() & (length(col("name")) > 0))
                 .withColumn("norm_name", _norm_title(col("name")))
                 .where(length(col("norm_name")) > 0)
                 .dropDuplicates(["norm_name"]))

        return clean.select(
            sha2(col("norm_name"), 256).alias("artist_id"),
            col("name")
        )

    @staticmethod
    def works(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Works dimension from track titles in Bronze.

            Steps:
                - Take every track title from `tracklist`.
                - Trim spaces and drop null/empty titles.
                - Remove parenthetical “take” notes (e.g., "(take 2)").
                - Normalize titles (lowercase, remove accents/punctuation, collapse spaces).
                - Deduplicate by the normalized title.
                - Create a stable `work_id` as SHA-256 of the normalized title.

            Args:
                bronze_df: input DataFrame that contains the `tracklist` array.

            Returns:
                A DataFrame with:
                    - work_id: SHA-256 hash of `norm_title`.
                    - work_title: cleaned, human-friendly title (without take notes).

        """

        titles: DataFrame = (bronze_df
                  .select(explode_outer("tracklist").alias("raw_title"))
                  .select(trim(col("raw_title")).alias("title"))
                  .where(col("title").isNotNull() & (length(col("title")) > 0))
                  )

        titles: DataFrame = (titles
                  .withColumn("clean_title", _strip_take_parens(col("title")))
                  .withColumn("norm_title", _norm_title(col("clean_title")))
                  .where(length(col("norm_title")) > 0)
                  .dropDuplicates(["norm_title"])
                  )

        return titles.select(
            sha2(col("norm_title"), 256).alias("work_id"),
            col("clean_title").alias("work_title")
        )

    # ---------------------------------------------
    # Facts / link tables
    # --------------------------------------------

    @staticmethod
    def tracks(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Tracks fact table (one row per distinct track per album).

            Steps:
                - Explode `tracklist` and keep the album id.
                - Clean the track name: remove "(... take ...)" notes, trim, normalize.
                - Deduplicate by (album_id, norm_name) to collapse alternate takes.
                - Create `track_id` as SHA-256 of "album_id::norm_name".
                - Return a readable `name` (cleaned) instead of the fully normalized one.

            Args:
                bronze_df: input DataFrame with columns `id` and `tracklist`.

            Returns:
                A DataFrame with:
                    - track_id  : SHA-256 hash of album_id and normalized title.
                    - album_id  : album identifier.
                    - name      : cleaned, human-friendly track name.
        """

        from pyspark.sql.functions import sha2

        tracks: DataFrame = (bronze_df
                  .select(col("id").alias("album_id"),
                          explode_outer("tracklist").alias("raw_name")))

        clean: DataFrame = (tracks
                 .withColumn("name", _strip_take_parens(trim(col("raw_name"))))
                 .withColumn("norm_name", _norm_title(col("name")))
                 .where(length(col("norm_name")) > 0)
                 .dropDuplicates(["album_id", "norm_name"])
                 )

        return clean.select(
            sha2(concat_ws("::",
                           col("album_id").cast("string"),
                           col("norm_name")), 256).alias("track_id"),
            col("album_id"),
            col("name")
        )

    @staticmethod
    def album_artist(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Album–Artist link table.

            It combines the `leaders` and `musicians` arrays for each album,
            tags each name with a role ("leader" / "musician"), cleans and
            normalizes the names, and maps each to a stable `artist_id`
            (SHA-256 of the normalized name). Duplicates are removed per
            (album_id, artist_id, role).

            Args:
                bronze_df: input DataFrame with `id`, `leaders`, and `musicians`.

            Returns:
                A DataFrame with:
                    - album_id  : album identifier.
                    - artist_id : SHA-256 hash of the normalized artist name.
                    - role      : "leader" or "musician".
        """

        leaders: DataFrame = (bronze_df
                   .select(col("id").alias("album_id"),
                           explode_outer("leaders").alias("name"))
                   .withColumn("role", lit("leader")))
        musicians = (bronze_df
                     .select(col("id").alias("album_id"),
                             explode_outer("musicians").alias("name"))
                     .withColumn("role", lit("musician")))
        rel = leaders.unionByName(musicians)

        clean: DataFrame = (rel
                 .select(
            col("album_id"),
            trim(col("name")).alias("name"),
            col("role")
        )
                 .where(col("name").isNotNull() & (length(col("name")) > 0))
                 .withColumn("norm_name", _norm_title(col("name")))
                 .where(length(col("norm_name")) > 0)
                 .select(
            col("album_id"),
            sha2(col("norm_name"), 256).alias("artist_id"),
            col("role")
        )
                 .dropDuplicates(["album_id", "artist_id", "role"])
                 )
        return clean

    @staticmethod
    def album_work(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Album–Work link table.

            Steps:
                - Explode `tracklist` per album and trim each title.
                - Remove parenthetical “take” notes and normalize the title.
                - Deduplicate by (album_id, norm_title) to collapse alternate takes.
                - Create `work_id` as SHA-256 of the normalized title.

            Args:
                bronze_df: input DataFrame with columns `id` and `tracklist`.

            Returns:
                A DataFrame with:
                    - album_id: album identifier.
                    - work_id : SHA-256 hash of the normalized work title.
        """

        from pyspark.sql.functions import sha2, explode_outer

        titles: DataFrame = (bronze_df
                  .select(col("id").alias("album_id"),
                          explode_outer("tracklist").alias("raw_title"))
                  .select(col("album_id"),
                          trim(col("raw_title")).alias("title"))
                  .where(col("title").isNotNull() & (length(col("title")) > 0))
                  )

        titles: DataFrame = (titles
                  .withColumn("clean_title", _strip_take_parens(col("title")))
                  .withColumn("norm_title", _norm_title(col("clean_title")))
                  .where(length(col("norm_title")) > 0)
                  .dropDuplicates(["album_id", "norm_title"])
                  )

        return titles.select(
            col("album_id"),
            sha2(col("norm_title"), 256).alias("work_id")
        )

    @staticmethod
    def album_label(bronze_df: DataFrame) -> DataFrame:
        """
            Build the Album–Label link table.

            Steps:
                - Select album id and label name.
                - Trim spaces and drop null/empty labels.
                - Create a stable `label_id` by hashing the lowercase name (SHA-256).
                - Drop duplicates so each (album_id, label_id) pair appears once.

            Args:
                bronze_df: input DataFrame with columns `id` and `label`.

            Returns:
                A DataFrame with:
                    - album_id : album identifier.
                    - label_id : SHA-256 hash of the lowercase label name.
        """

        return (
            bronze_df
            .select(
                col("id").alias("album_id"),
                trim(col("label")).alias("name")
            )
            .where(col("name").isNotNull() & (length(col("name")) > 0))
            .select(
                col("album_id"),
                sha2(lower(col("name")), 256).alias("label_id")
            )
            .dropDuplicates()
        )
