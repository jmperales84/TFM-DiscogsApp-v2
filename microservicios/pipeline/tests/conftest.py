import sys
import urllib.request
from pathlib import Path
# Add /microservicios/pipeline/src to test's sys.path
sys.path.append(str(Path(__file__).parents[1] / "src"))

import os
from pathlib import Path

# Choose JDK
JDKS = [
    r"C:\Program Files\Java\jdk-21",
    r"C:\Program Files\Eclipse Adoptium\jdk-17",
    r"C:\Program Files\Java\jdk-17",
]
for jdk in JDKS:
    if Path(jdk).exists():
        os.environ["JAVA_HOME"] = jdk
        os.environ["PATH"] = jdk + r"\bin;" + os.environ["PATH"]
        break


repo_root = Path(__file__).resolve().parents[2]
jars_dir = repo_root / "deps" / "jars"
jars_dir.mkdir(parents=True, exist_ok=True)

files = {
    "delta-spark_2.12-3.2.0.jar": "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar",
    "delta-storage-3.2.0.jar": "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar",
}

for name, url in files.items():
    dest = jars_dir / name
    if not dest.exists():
        print(f"Downloading {name} from {url}")
        urllib.request.urlretrieve(url, dest)
