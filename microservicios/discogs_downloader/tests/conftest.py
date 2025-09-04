import sys
from pathlib import Path
# añade /microservicios/discogs_downloader/src al sys.path de los tests
sys.path.append(str(Path(__file__).parents[1] / "src"))