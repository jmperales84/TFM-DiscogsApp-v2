from pathlib import Path
import shutil

class DataArchiver:
    """
        Main class to move files from the landing zone into the raw zone.

        It scans the landing folder and moves supported files to the raw folder
        following simple rules:
            - JSON files are moved into `raw/albums`.
            - Other formats are ignored for now.

        The class also creates all needed folders if they do not exist yet.

        Config dict should include:
            - paths: {"landing": str, "raw": str}

        Notes:
            - Current implementation is basic and easy to extend. For example,
            you could add rules to handle covers or audio files in the future.

            - Existing files in the target directory are skipped (no overwrite).
    """

    def __init__(self, config: dict):
        """
            Initialize the DataArchiver with landing/raw paths.

            It reads the required paths from the config and prepares the folder
            structure used by the archiver:
                - <landing>
                - <raw>
                - <raw>/albums

            Missing directories are created if they do not exist.

            Args:
                config: configuration dictionary with:
                    - paths.landing: landing folder path (str)
                    - paths.raw: raw folder path (str)

            Side effects:
                - Creates the directories listed above (idempotent: no error if they
                      already exist).

            Returns:
                None.
        """

        self.landing: Path = Path(config["paths"]["landing"])
        self.raw: Path = Path(config["paths"]["raw"])
        self.raw_albums: Path = self.raw / "albums"

        # Make directories in case they haven't been created yet
        for p in [self.landing, self.raw, self.raw_albums]:
            p.mkdir(parents=True, exist_ok=True)

    def move_all(self) -> int:
        """
            Move all supported files from landing to raw.

            It scans the landing folder recursively. For each file:
                - If it is a JSON file, move it into `raw/albums`.
                - Other formats are ignored for now.

            Files that already exist in the destination are skipped
            (no overwrite is performed).

            Returns:
                Number of files successfully moved.
        """

        moved: int = 0

        # Iterate over landing folder
        for file in self.landing.rglob("*"):

            if not file.is_file():
                continue

            suf: str = file.suffix.lower()

            if suf == ".json":

                dst_dir = self.raw_albums

            else:
                continue  # ignore other formats

            moved += self._move_dir(file, dst_dir)

        return moved

    @staticmethod
    def _move_dir(src_file: Path, dst_dir: Path) -> int:
        """
            Move one file into the target directory.

            - If a file with the same name already exists in the target folder,
                the move is skipped (no overwrite).
            - Otherwise, the target directory is created if needed and the file
                is moved.

            Args:
                src_file: path of the file to move.
                dst_dir: destination directory where the file will be placed.

            Returns:
                1 if the file was moved successfully, 0 if it was skipped.
        """

        target: Path = dst_dir / src_file.name

        if target.exists():

            print(f"It already exists. Skip: {target.name}")

            return 0

        dst_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_file), str(target))
        print(f"Moved: {src_file.name}")

        return 1
