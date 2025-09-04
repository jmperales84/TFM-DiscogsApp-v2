import json
from pathlib import Path
import pytest
from discogs_downloader.discogs_download_json import DiscogsDownloader

def test_data_from_master(monkeypatch, tmp_path):

    # --- 1) Config and environment ---
    landing = tmp_path / "landing"; landing.mkdir()
    raw = tmp_path / "raw" / "albums"; raw.mkdir(parents=True)

    config = {
        "years": {"first": 1960, "last": 1960},
        "headers": {"User-Agent": "pytest"},
        "max_results": 50,
        "subgenres_download": {"hard bop": True},
        "allowed_labels": {"blue note": {"name": "Blue Note", "download": True}},
        "performing_roles": ["trumpet", "sax", "piano", "bass", "drums"],
        "excluded_instruments": ["producer", "engineer"],
        "sleep_between_requests": 0,
        "paths": {"landing": str(landing), "raw": str(tmp_path / "raw")}
    }

    monkeypatch.setenv("DISCOGS_TOKEN", "fake-token")
    monkeypatch.setenv("SEARCH_URL", "https://api.discogs.com/database/search")

    # --- 2) Fake urls and returned resources ---
    master_id = 123456
    master_url = f"https://api.discogs.com/masters/{master_id}"
    main_release_url = "https://api.discogs.com/releases/9999"
    artist_url = "https://api.discogs.com/artists/42"

    search_page1 = {
        "pagination": {"pages": 1},
        "results": [{
            "type": "master",
            "id": master_id,
            "year": 1960,
            "format": ["LP", "Album"],
            "label": ["Blue Note"],
            "style": ["Hard Bop"],
            "cover_image": "http://x/img.jpg",
            "master_url": master_url,
        }]
    }

    master_data = {
        "id": master_id,
        "year": 1960,
        "title": "Test Master",
        "main_release_url": main_release_url,
        "most_recent_release_url": main_release_url
    }

    main_release = {
        "title": "Test Album",
        "tracklist": [{"title": "Track A"}, {"title": "Track B"}],
        "extraartists": [
            {"name": "Lee Morgan", "role": "Trumpet"},
            {"name": "Rudy Van Gelder", "role": "Engineer"}
        ],
        "artists": [{
            "name": "Lee Morgan Quintet",
            "resource_url": artist_url
        }]
    }

    artist_detail = {
        "members": [{"name": "Lee Morgan"}, {"name": "Another Person"}]
    }

    # Dictionary url -> get resource
    url_map = {
        "https://api.discogs.com/database/search": search_page1,
        master_url: master_data,
        main_release_url: main_release,
        artist_url: artist_detail,
    }

    # --- 3) Mocks _safe_request ---
    # _safe_request will return resources depending on the url
    def fake_safe_request(self, url, max_retries=5):
        for key, val in url_map.items():
            if url.startswith(key):
                return val
        return None

    # mock sleep method from time to avoid waiting
    monkeypatch.setattr(DiscogsDownloader, "_safe_request", fake_safe_request, raising=True)
    monkeypatch.setattr(time, "sleep", lambda *_args, **_kwargs: None)

    # --- 4) Execute ---
    dl = DiscogsDownloader(config)
    dl.download_albums()

    # --- 5) Assertions ---
    assert len(dl.get_albums_info()) == 1
    album = dl.get_albums_info()[0]
    assert album["id"] == master_id
    assert album["label"] == "Blue Note"
    assert album["tracklist"] == ["Track A", "Track B"]
    # Rudy Van Gelder (Engineer) excluded from musicians list.
    # Lee Morgan (leader) included in musicians list
    assert "Rudy Van Gelder" not in album["musicians"]
    assert "Lee Morgan" in album["leaders"]

    # File created in landing
    saved = list(Path(config["paths"]["landing"]).glob(f"{master_id}.json"))
    assert len(saved) == 1
    # y su JSON contiene el título esperado
    data = json.loads(saved[0].read_text(encoding="utf-8"))
    assert data["title"] == "Test Album"

# Try specific methods

def _dl():
    return DiscogsDownloader({
        "years": {"first": 1960, "last": 1960},
        "headers": {"User-Agent": "pytest"},
        "max_results": 50,
        "subgenres_download": {"hard bop": True},
        "allowed_labels": {"blue note": {"name": "Blue Note", "download": True}},
        "performing_roles": ["trumpet","sax","piano","bass","drums"],
        "excluded_instruments": ["engineer","producer"],
        "sleep_between_requests": 0,
        "paths": {"landing": "/tmp/landing", "raw": "/tmp/raw"},
    })

@pytest.mark.parametrize("result,expected", [
    ({"format": ["LP","Album"]}, True),
    ({"format": ['7"']}, False),
    ({"format_descriptions": ["45 RPM"]}, False),
    ({"format": ["78 RPM"]}, False),
    ({"format": ["LP"], "format_descriptions": ["Mono"]}, True),
])

def test_is_valid_format(result, expected):
    dl = _dl()
    assert dl._is_valid_format(result) is expected

def test_filter_label():
    dl = _dl()
    assert dl._filter_label({"label": ["Blue Note"]}) == "Blue Note"
    assert dl._filter_label({"label": ["Random"]}) is None

def test_clean_musicians():
    dl = _dl()
    out = dl._clean_musicians(["Lee Morgan (1)", "Art Blakey", "Lee Morgan (1)"])
    assert set(out) == {"Lee Morgan", "Art Blakey"}

def test_identify_leaders():
    dl = _dl()
    leaders = dl._identify_leaders("Lee Morgan Quintet", ["Lee Morgan","Art Blakey"])
    assert "Lee Morgan" in leaders

def test_identify_leaders_from_members(monkeypatch):
    dl = _dl()
    artist_url = "https://api.discogs.com/artists/42"
    release_data = {"artists": [{"name":"Lee Morgan Quintet", "resource_url": artist_url}]}

    def fake_safe(self, url, max_retries=5):
        if url == artist_url:
            return {"members":[{"name":"Lee Morgan"},{"name":"Wayne Shorter"}]}
        return None

    monkeypatch.setattr(type(dl), "_safe_request", fake_safe, raising=True)
    leaders = dl._identify_leaders_from_members(release_data)
    assert leaders == ["Lee Morgan"]

import types, time

def test_safe_request_rate_limit(monkeypatch):
    dl = _dl()
    # evitar sleeps
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    calls = {"n": 0}
    class Resp:
        def __init__(self, status, text, payload=None):
            self.status_code = status
            self.text = text
            self._payload = payload or {}
        def raise_for_status(self):
            if self.status_code >= 400 and self.status_code != 429:
                raise Exception("HTTP error")
        def json(self): return self._payload

    def fake_get(url, headers=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return Resp(429, "You are sending requests too quickly")
        return Resp(200, "ok", {"ok": True})

    import requests
    monkeypatch.setattr(requests, "get", fake_get)
    assert dl._safe_request("http://x") == {"ok": True}
    assert calls["n"] == 2