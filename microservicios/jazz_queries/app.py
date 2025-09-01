from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
import os
from typing import List, Optional, Union
import logging
from neo4j import Driver  # driver type
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# --- Basic logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("jazz_queries")


# --- Environment variables  ---
class Settings(BaseSettings):
    """
    Read configuration from environment file.

    Fields:
      - ROOT_PATH: Optional FastAPI root path (useful behind a proxy).
      - NEO4J_URI / USER / PASS: Connection settings for Neo4j.
    """

    ROOT_PATH: str = ""
    NEO4J_URI: str = "bolt://neo4j:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASS: str = "password"
    model_config = SettingsConfigDict(
        env_file="ms.env",
        env_file_encoding="utf-8"
    )


settings = Settings()

ROOT_PATH = settings.ROOT_PATH
NEO4J_URI = settings.NEO4J_URI
NEO4J_USER = settings.NEO4J_USER
NEO4J_PASS = settings.NEO4J_PASS


# --- Global driver (initialized at startup) ---
driver: Optional[Driver] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    App lifecycle handler.

    On startup:
      - Create the Neo4j driver.
      - Run a simple smoke test (RETURN 1).

    On shutdown:
      - Close the driver cleanly.
    """

    global driver
    logger.info("Initializing Neo4j driver…")
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASS),
    )
    # Test: check that Neo4j responds
    try:

        with driver.session() as s:

            s.run("RETURN 1 AS ok").single()

        logger.info("Connection to Neo4j OK.")

    except Exception:

        logger.exception("Failed to connect to Neo4j.")
        # Re-raise so the container fails fast if DB is down
        raise

    # Yield control to the app while it's alive
    yield

    # Shutdown
    if driver is not None:
        driver.close()
        logger.info("Neo4j driver closed.")


# --- Create FastAPI app with lifespan ---
app = FastAPI(title="Jazz Queries API", root_path=ROOT_PATH, lifespan=lifespan)


# --- CORS  ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_driver() -> Driver:
    """Return the initialized Neo4j driver (asserts on startup errors)."""

    # The assert pleases the type checker and protects in case startup failed
    assert driver is not None, "Neo4j driver not initialized"

    return driver

# --- PYDANTIC MODELS ---

# For /suggest/*
class SuggestItem(BaseModel):
    """Minimal item for autocomplete lists (id for API, name for display)."""

    id: Union[str, int]
    name: str


class ArtistSession(BaseModel):
    """Album where an artist plays, with leaders/participants and track list."""

    artist: str
    album_id: int
    ensemble: Optional[str] = None
    title: str
    tracks: List[str] = Field(default_factory=list)
    leaders: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    year: Optional[int] = None
    label: Optional[str] = None
    cover: Optional[str] = None


class LeaderSession(BaseModel):
    """Album led by a given artist, including tracks and personnel."""

    artist: str
    ensemble: Optional[str] = None
    title: str
    tracks: List[str] = Field(default_factory=list)
    leaders: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    year: Optional[int] = None
    label: Optional[str] = None
    cover: Optional[str] = None


class CollabAlbum(BaseModel):
    """Album where the selected set of artists all appear together."""

    album_id: int
    ensemble: Optional[str] = None
    title: str
    leaders: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    year: Optional[int] = None
    label: Optional[str] = None
    cover: Optional[str] = None


class AlbumDetail(BaseModel):
    """Detailed album info including tracks, leaders, participants and cover."""

    album_id: int
    ensemble: Optional[str] = None
    title: str
    tracks: List[str] = Field(default_factory=list)
    leaders: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    year: Optional[int] = None
    label: Optional[str] = None
    cover: Optional[str] = None


class WorkAppearance(BaseModel):
    """Where a given work (song) appears: album, personnel, label, year, cover."""

    work_title: str
    ensemble: Optional[str] = None
    album_title: str
    tracks: List[str] = Field(default_factory=list)
    leaders: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    year: Optional[int] = None
    label: Optional[str] = None
    cover: Optional[str] = None


# --- 1) Healthcheck ---
@app.get("/healthz")
def healthz():
    """Return OK if the service and database make connection."""

    try:

        with get_driver().session() as s:

            s.run("RETURN 1").single()

        return {"status": "ok"}

    except Exception:

        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# --- 2) Artist suggestions (autocomplete, case-insensitive) ---
@app.get("/suggest/artists", response_model=List[SuggestItem])
def suggest_artists(
    q: str = Query("", description="Artist name (case-insensitive)")
):
    """
    Return a list of artist suggestions for autocomplete.

    Args:
      q: Partial text typed by the user (case-insensitive). Empty returns top items.

    Returns:
      List of {id, name} items ordered by prefix match first, then alphabetically.
    """

    cy = """
    WITH toLower($q) AS q
    MATCH (p:Artist)
    WHERE q = '' OR toLower(p.name) CONTAINS q
    RETURN p.artist_id AS id, p.name AS name
    ORDER BY
      CASE WHEN q <> '' AND toLower(p.name) STARTS WITH q THEN 0 ELSE 1 END,
      toLower(p.name)
    """

    with get_driver().session() as s:
        return s.run(cy, q=q).data()


# --- 3) Work suggestions (autocomplete, case-insensitive) ---
@app.get("/suggest/works", response_model=List[SuggestItem])
def suggest_works(
    q: str = Query("", description="Song title (case-insensitive)")
):
    """
    Return a list of work (song) suggestions for autocomplete.
    """

    cy = """
    WITH toLower($q) AS q
    MATCH (w:Work)
    WHERE q = '' OR toLower(w.work_title) CONTAINS q
    RETURN w.work_id AS id, w.work_title AS name
    ORDER BY
      CASE WHEN q <> '' AND toLower(w.work_title) STARTS WITH q THEN 0 ELSE 1 END,
      toLower(w.work_title)
    """

    with get_driver().session() as s:
        return s.run(cy, q=q).data()


# --- 4) Album suggestions (autocomplete, case-insensitive) ---
@app.get("/suggest/albums", response_model=List[SuggestItem])
def suggest_albums(
    q: str = Query("", description="Record title (case-insensitive)")
):
    """
    Return a list of album suggestions for autocomplete.
    """

    cy = """
    WITH toLower($q) AS q
    MATCH (a:Album)
    WHERE q = '' OR toLower(a.title) CONTAINS q
    RETURN a.album_id AS id, a.title AS name
    ORDER BY
      CASE WHEN q <> '' AND toLower(a.title) STARTS WITH q THEN 0 ELSE 1 END,
      coalesce(a.year, 9999),
      toLower(a.title)
    """

    with get_driver().session() as s:
        return s.run(cy, q=q).data()


# 5) ---------- Minimal UI ----------
@app.get("/ui", response_class=HTMLResponse)
def ui():
    """Serve a very small HTML UI to test the endpoints from a browser."""
    html = """
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <title>Jazz Queries</title>
      <style>
        body { font-family: system-ui, sans-serif; margin: 24px; max-width: 1100px; }
        .row { display: flex; gap: 12px; align-items: center; margin-bottom: 12px; }
        input, select, button { padding: 8px; }
        #results { white-space: pre-wrap; background:#f7f7f7; padding:12px; border-radius:8px; min-height: 160px; }
        small.hint { color:#666; margin-left: 8px; }
        label { min-width: 90px; }
        /* selected chips */
        #selectedList { display:flex; gap:8px; flex-wrap: wrap; }
        .chip {
          display:inline-flex; align-items:center; gap:6px;
          padding:4px 8px; border-radius:16px; background:#e9eef7; border:1px solid #c9d6ee;
        }
        .chip button {
          border:none; background:transparent; cursor:pointer; font-weight:bold;
        }
        .f1 { flex:1 }
        .hidden { display:none }
      </style>
    </head>
    <body>
      <h1>Jazz Explorer (Neo4j)</h1>

      <div class="row">
        <label>Type:</label>
        <select id="tipo">
          <option value="artist">Artist → Sessions</option>
          <option value="leader">Leader → Sessions</option>
          <option value="album">Album → Details</option>
          <option value="work">Song → Appearances</option>
          <option value="collab">Collaborations → Albums</option>
        </select>
        <small id="hint" class="hint"></small>
      </div>

      <div class="row">
        <label>Find:</label>
        <input id="query" placeholder="Type a name/title..." class="f1"/>
      </div>

      <div class="row">
        <label>Suggestions:</label>
        <select id="suggestions" size="8" class="f1"></select>
        <div id="collabButtons" class="hidden" style="display:flex; flex-direction:column; gap:8px;">
          <button id="addBtn">Add →</button>
          <button id="clearBtn">Clear</button>
        </div>
      </div>

      <div id="selectedWrap" class="row hidden">
        <label>Selection:</label>
        <div id="selectedList" class="f1"></div>
      </div>

      <div class="row">
        <button id="run">Run</button>
      </div>

      <h3>Results</h3>
      <div id="results"></div>

    <script>
    const tipoEl = document.getElementById('tipo');
    const qEl = document.getElementById('query');
    const sugEl = document.getElementById('suggestions');
    const outEl = document.getElementById('results');
    const hintEl = document.getElementById('hint');
    const selectedWrap = document.getElementById('selectedWrap');
    const selectedListEl = document.getElementById('selectedList');
    const addBtn = document.getElementById('addBtn');
    const clearBtn = document.getElementById('clearBtn');
    const collabButtons = document.getElementById('collabButtons');

    let selectedSingle = null;       // for artist / leader / album / work
    let chosenCollab = [];           // [{id, name}] for collaborations
    let debounceId = null;

    function endpointSuggest() {
      const t = tipoEl.value;
      if (t === 'artist' || t === 'leader' || t === 'collab') return './suggest/artists';
      if (t === 'album')  return './suggest/albums';
      if (t === 'work')   return './suggest/works';
      return '';
    }

    function setHintAndVisibility() {
      const isCollab = (tipoEl.value === 'collab');
      hintEl.textContent = isCollab ? 'Tip: find several artists and click “Run”.' : '';
      selectedWrap.classList.toggle('hidden', !isCollab);
      collabButtons.classList.toggle('hidden', !isCollab);
      // for collabs we keep select simple and hit Add
      sugEl.multiple = false;
      sugEl.size = isCollab ? 8 : 6;
      if (!isCollab) chosenCollab = [];
      renderChosen();
    }

    function renderOptions(items) {
      sugEl.innerHTML = '';
      items.forEach(it => {
        const opt = document.createElement('option');
        opt.value = it.id ?? it.name ?? it.title;
        opt.textContent = it.name ?? it.title ?? it.id;
        opt.dataset.payload = JSON.stringify(it);
        sugEl.appendChild(opt);
      });
    }

    function renderChosen() {
      selectedListEl.innerHTML = '';
      chosenCollab.forEach(p => {
        const chip = document.createElement('span');
        chip.className = 'chip';
        chip.innerHTML = `<span>${p.name}</span><button title="Remove" data-id="${p.id}">×</button>`;
        selectedListEl.appendChild(chip);
      });
      // delegate remove
      selectedListEl.querySelectorAll('button[data-id]').forEach(btn => {
        btn.addEventListener('click', () => {
          const id = btn.getAttribute('data-id');
          chosenCollab = chosenCollab.filter(x => String(x.id) !== String(id));
          renderChosen();
        });
      });
    }

    async function fetchJson(url) {
      const res = await fetch(url);
      const text = await res.text();
      if (!res.ok) throw new Error(`HTTP ${res.status} – ${text}`);
      try { return JSON.parse(text); } catch { return text; }
    }

    tipoEl.addEventListener('change', () => {
      qEl.value = '';
      sugEl.innerHTML = '';
      selectedSingle = null;
      outEl.textContent = '';
      setHintAndVisibility();
    });

    qEl.addEventListener('input', () => {
      selectedSingle = null;
      clearTimeout(debounceId);
      const q = qEl.value.trim();
      if (!q) { sugEl.innerHTML=''; return; }
      debounceId = setTimeout(async () => {
        const base = endpointSuggest();
        if (!base) return;
        try {
          const url = `${base}?q=${encodeURIComponent(q)}&limit=20`;
          const data = await fetchJson(url);
          renderOptions(data);
        } catch (e) {
          outEl.textContent = e.message;
        }
      }, 250);
    });

    // simple selection when no collab
    sugEl.addEventListener('change', () => {
      if (tipoEl.value === 'collab') return;
      const opt = sugEl.options[sugEl.selectedIndex];
      selectedSingle = opt ? JSON.parse(opt.dataset.payload) : null;
    });

    // double click on suggestion -> add to collab
    sugEl.addEventListener('dblclick', () => {
      if (tipoEl.value !== 'collab') return;
      const opt = sugEl.options[sugEl.selectedIndex];
      if (!opt) return;
      const item = JSON.parse(opt.dataset.payload);
      if (!chosenCollab.some(x => String(x.id) === String(item.id))) {
        chosenCollab.push({ id: item.id, name: item.name ?? item.title ?? String(item.id) });
        renderChosen();
      }
    });

    // collab buttons
    addBtn.addEventListener('click', () => {
      if (tipoEl.value !== 'collab') return;
      const opt = sugEl.options[sugEl.selectedIndex];
      if (!opt) return;
      const item = JSON.parse(opt.dataset.payload);
      if (!chosenCollab.some(x => String(x.id) === String(item.id))) {
        chosenCollab.push({ id: item.id, name: item.name ?? item.title ?? String(item.id) });
        renderChosen();
      }
    });

    clearBtn.addEventListener('click', () => {
      chosenCollab = [];
      renderChosen();
    });

    document.getElementById('run').addEventListener('click', async () => {
      outEl.textContent = 'Loading...';
      const t = tipoEl.value;

      try {
        let url;
        if (t === 'artist') {
          if (!selectedSingle?.id) throw new Error('Select an artist.');
          url = `./artists/${encodeURIComponent(selectedSingle.id)}/sessions?limit=50`;
        } else if (t === 'leader') {
          if (!selectedSingle?.id) throw new Error('Select a leader.');
          url = `./leaders/${encodeURIComponent(selectedSingle.id)}/sessions?limit=50`;
        } else if (t === 'album') {
          if (!selectedSingle?.id) throw new Error('Select an album.');
          url = `./albums/${encodeURIComponent(selectedSingle.id)}`;
        } else if (t === 'work') {
          if (!selectedSingle?.id) throw new Error('Select a work.');
          url = `./works/${encodeURIComponent(selectedSingle.id)}`;
        } else if (t === 'collab') {
          const ids = chosenCollab.map(x => x.id);
          if (ids.length < 2) throw new Error('Pick at least 2 artists (double-click or “Add →”).');
          const params = ids.map(id => `artists=${encodeURIComponent(id)}`).join('&');
          url = `./collabs/albums?${params}&limit=50`;
        } else {
          throw new Error('Unsupported type.');
        }

        const data = await fetchJson(url);
        outEl.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        outEl.textContent = e.message;
      }
    });

    // initial state
    setHintAndVisibility();
    </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


# --- 6) Artist sessions (albums where they play + leaders) ---
@app.get("/artists/{artist_id}/sessions",
    response_model=List[ArtistSession],
    response_model_exclude_none=True
)
def artist_sessions(
    artist_id: str,
    year_min: Optional[int] = Query(None, ge=1900, le=2100),
    year_max: Optional[int] = Query(None, ge=1900, le=2100),
):
    """
    List albums where the given artist participates (with leaders and personnel).

    Args:
      artist_id: ID of the artist of interest.
      year_min (optional): first year of the time period.
      year_max (optional): last year of the time period.
    """

    cy = """
    MATCH (me:Artist {artist_id: $id})-[:PLAYS_IN]->(al:Album)
    OPTIONAL MATCH (al)-[:RELEASED_BY]->(lab:Label)
    OPTIONAL MATCH (lead:Artist)-[:LEADS]->(al)
    WITH me, al, lab, collect(DISTINCT lead.name) AS leaders
    WHERE ($year_min IS NULL OR al.year >= $year_min)
      AND ($year_max IS NULL OR al.year <= $year_max)

    OPTIONAL MATCH (a:Artist)-[:PLAYS_IN]->(al)
    WITH me, al, lab, leaders, collect(DISTINCT a.name) AS participants

    OPTIONAL MATCH (al)-[:CONTAINS]->(w:Work)
    WITH me, al, lab, leaders, participants, collect(DISTINCT w.work_title) AS tracks

    RETURN
      me.name      AS artist,
      al.album_id  AS album_id,
      al.ensemble  AS ensemble,
      al.title     AS title,
      tracks,
      leaders,
      participants,
      al.year      AS year,
      lab.name     AS label,
      al.cover_url AS cover

    ORDER BY year, title
    """

    with get_driver().session() as s:

        rows = s.run(cy, id=artist_id, year_min=year_min, year_max=year_max).data()

    if not rows:

        raise HTTPException(404, "No results for that artist.")

    return rows

# 7) --- Albums where a selected set of artists all play together ---
@app.get("/collabs/albums",
         response_model=List[CollabAlbum],
         response_model_exclude_none=True
)
def collabs_albums(
    artists: List[str] = Query(..., description="Artist IDs; repeat the parameter: ?artists=id1&artists=id2"),
    year_min: Optional[int] = Query(None, ge=1900, le=2100),
    year_max: Optional[int] = Query(None, ge=1900, le=2100)
):
    """
    Find albums that include **all** the provided artists.

    Args:
      artists: IList of artists ID.
      year_min (optional): first year of the time period.
      year_max (optional): last year of the time period.
    """

    # Minimal validation
    wanted = list(dict.fromkeys(artists))

    if len(wanted) < 2:
        raise HTTPException(status_code=422, detail="You must provide at least 2 artists.")

    cy = """
    // 1) Pick albums where any of the wanted artists play
    WITH $artist_ids AS wanted
    MATCH (p:Artist)-[:PLAYS_IN]->(al:Album)
    WHERE p.artist_id IN wanted
    WITH wanted, al, collect(DISTINCT p.artist_id) AS involved

    // 2) Keep only albums that contain ALL requested artists
    WHERE size(involved) = size(wanted)
      AND ($year_min IS NULL OR al.year >= $year_min)
      AND ($year_max IS NULL OR al.year <= $year_max)

    // 3) Get label, leaders and participants
    OPTIONAL MATCH (al)-[:RELEASED_BY]->(lab:Label)
    OPTIONAL MATCH (lead:Artist)-[:LEADS]->(al)
    OPTIONAL MATCH (x:Artist)-[:PLAYS_IN]->(al)

    WITH wanted, al, lab, lead, collect(DISTINCT x.name) AS participants
    WITH wanted, al, lab,
         participants,
         collect(DISTINCT lead.name) AS leaders

    RETURN
      al.album_id  AS album_id,
      al.ensemble  AS ensemble,
      al.title     AS title,
      leaders,
      participants,
      al.year      AS year,
      lab.name     AS label,
      al.cover_url AS cover

    ORDER BY year, title
    """

    with get_driver().session() as s:
        rows = s.run(cy, artist_ids=wanted, year_min=year_min, year_max=year_max).data()

    if not rows:

        raise HTTPException(404, detail="No common albums for those artists in the given range.")

    return rows

# 8) --- Album details by id ---
@app.get("/albums/{album_id}",
         response_model=AlbumDetail,
         response_model_exclude_none=True
)
def get_album(album_id: int):
    """
    Return album details: tracks, leaders, participants, label, year and cover.

    Args:
      album_id: ID of the album of interest.
    """

    cy = """
    // 1) Find the album and its label (if any)
    MATCH (al:Album {album_id: $id})
    OPTIONAL MATCH (al)-[:RELEASED_BY]->(lab:Label)
    WITH al, lab

    // 2) Works contained in the album
    OPTIONAL MATCH (w:Work)<-[:CONTAINS]-(al)
    WITH al, lab, collect(DISTINCT w.work_title) AS tracks

    // 3) Leaders → names
    OPTIONAL MATCH (ld:Artist)-[:LEADS]->(al)
    WITH al, lab, tracks, collect(DISTINCT ld.name) AS leaders

    // 4) Participants → names
    OPTIONAL MATCH (p:Artist)-[:PLAYS_IN]->(al)
    WITH al, lab, tracks, leaders,
         collect(DISTINCT p.name) AS participants

    // 5) Final projection as a single map
    RETURN {
      album_id:     al.album_id,
      ensemble:     al.ensemble,
      title:        al.title,
      tracks:       tracks,
      leaders:      leaders,
      participants: participants,
      year:         al.year,
      label:        lab.name,
      cover:        al.cover_url
    } AS album
    """
    with get_driver().session() as s:
        rec = s.run(cy, id=album_id).single()
    if not rec or not rec["album"]:
        raise HTTPException(404, "Album not found")
    return rec["album"]


# 9) --- Sessions led by a given artist ---
@app.get("/leaders/{artist_id}/sessions",
         response_model=List[LeaderSession],
         response_model_exclude_none=True
)
def leader_sessions(
    artist_id: str,
    year_min: Optional[int] = Query(None, ge=1900, le=2100),
    year_max: Optional[int] = Query(None, ge=1900, le=2100),
):
    """
    List albums **led** by the given artist, including tracks and personnel.

    Args:
      artist_id: ID of the artist of interest.
      year_min (optional): first year of the time period.
      year_max (optional): last year of the time period.
    """

    cy = """
    // 1) Albums led by the artist
    MATCH (l1:Artist {artist_id: $id})-[:LEADS]->(al:Album)
    WHERE ($year_min IS NULL OR al.year >= $year_min)
      AND ($year_max IS NULL OR al.year <= $year_max)

    // 2) Label of each album
    OPTIONAL MATCH (al)-[:RELEASED_BY]->(lab:Label)
    WITH l1, al, lab

    // 3) Works contained in the album
    OPTIONAL MATCH (al)-[:CONTAINS]->(w:Work)
    WITH l1, al, lab, collect(DISTINCT w.work_title) AS tracks

    // 4) Participants of the album
    OPTIONAL MATCH (p:Artist)-[:PLAYS_IN]->(al)
    WITH l1, al, lab, tracks, collect(DISTINCT p.name) AS participants

    // 5) Leaders for each album (could be several)
    MATCH (l2:Artist)-[:LEADS]->(al)
    WITH l1, al, lab, tracks, participants, collect(DISTINCT l2.name) AS leaders

    RETURN
      l1.name      AS artist,
      al.ensemble  AS ensemble,
      al.title     AS title,
      tracks,
      leaders,
      participants,
      al.year      AS year,
      lab.name     AS label,
      al.cover_url AS cover

    ORDER BY year, title
    """

    with get_driver().session() as s:

        rows = s.run(cy, id=artist_id, year_min=year_min, year_max=year_max).data()

    if not rows:

        raise HTTPException(404, "No results for that artist.")

    return rows


# 10) --- Appearances of a given work (standard) ---
@app.get("/works/{work_id}",
         response_model=List[WorkAppearance],
         response_model_exclude_none=True
)
def get_work(work_id: str):
    """
    List appearances of a given work (song) in albums. It provides: album, personnel, label, year.

    Args:
      work_id: ID of the song of interest.
    """

    cy = """
    // 1) Find the work, albums where it appears, and labels
    MATCH (w1:Work {work_id: $id})
    OPTIONAL MATCH (al:Album)-[:CONTAINS]->(w1)
    OPTIONAL MATCH (al)-[:RELEASED_BY]->(lab:Label)
    WITH w1, al, lab
    WHERE al IS NOT NULL  // avoid empty row if there are no albums

    // 2) All tracks contained in those albums
    OPTIONAL MATCH (al)-[:CONTAINS]->(w2)
    WITH w1, al, lab, collect(DISTINCT w2.work_title) AS tracks

    // 3) Leaders → names
    OPTIONAL MATCH (ld:Artist)-[:LEADS]->(al)
    WITH w1, al, lab, tracks, collect(DISTINCT ld.name) AS leaders

    // 4) Participants → names
    OPTIONAL MATCH (p:Artist)-[:PLAYS_IN]->(al)
    WITH w1, al, lab, tracks, leaders,
         collect(DISTINCT p.name) AS participants

    // 5) Final projection
    RETURN 
      w1.work_title  AS work_title,
      al.ensemble    AS ensemble,
      al.title       AS album_title,
      tracks,
      leaders,
      participants,
      al.year        AS year,
      lab.name       AS label,
      al.cover_url   AS cover

    ORDER BY year, album_title
    """

    with get_driver().session() as s:
        row = s.run(cy, id=work_id).data()
    if not row:
        raise HTTPException(404, "Work not found")
    return row


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)
