from pathlib import Path
import pytest
from pipeline.data_archiver import DataArchiver


@pytest.fixture
def fs_paths(tmp_path):
    """
    Fixture useful to create folder names:
      - landing/
      - raw/
    It returns config dict necessary for DataArchiver.
    """
    landing = tmp_path / "landing"
    raw = tmp_path / "raw"
    return {
        "config": {"paths": {"landing": str(landing), "raw": str(raw)}},
        "landing": landing,
        "raw": raw,
    }


def make_file(path: Path, content="{}") -> Path:
    """
    Helper: Creates a file with content (JSON as default).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_init_creates_directories(fs_paths):
    archiver = DataArchiver(fs_paths["config"])

    assert fs_paths["landing"].exists()
    assert fs_paths["raw"].exists()
    assert (fs_paths["raw"] / "albums").exists()


def test_move_only_json(fs_paths):
    landing = fs_paths["landing"]
    raw_albums = Path(fs_paths["raw"]) / "albums"

    # Creates a json and txt files
    make_file(landing / "a.json", '{"id": 1}')
    make_file(landing / "b.txt", "not json")

    archiver = DataArchiver(fs_paths["config"])
    moved = archiver.move_all()

    assert moved == 1
    assert (raw_albums / "a.json").exists()
    assert (landing / "b.txt").exists()  # remains


def test_skip_if_exists(fs_paths, capsys):
    landing = fs_paths["landing"]
    raw_albums = Path(fs_paths["raw"]) / "albums"

    # Creates same file in raw/albums
    make_file(landing / "d.json", '{"id": 3}')
    make_file(raw_albums / "d.json", '{"id": 3}')

    archiver = DataArchiver(fs_paths["config"])
    moved = archiver.move_all()

    captured = capsys.readouterr()
    assert moved == 0
    assert "Skip" in captured.out


def test_idempotent_calls(fs_paths):
    landing = fs_paths["landing"]
    make_file(landing / "e.json", '{"id": 4}')

    archiver = DataArchiver(fs_paths["config"])
    moved_first = archiver.move_all()
    moved_second = archiver.move_all()

    assert moved_first == 1
    assert moved_second == 0