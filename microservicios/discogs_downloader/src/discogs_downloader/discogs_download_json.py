import requests
import json
import re
import time
from typing import Dict, List, Tuple, Optional, Set, Any
import os

class DiscogsDownloader:
    """
        Main class to download jazz albums data from the Discogs API.

        It searches albums by style, year and label, always trying to resolve
        results to their master release. For every album it collects basic info
        (artists, title, year, label, style, cover URL, tracklist, musicians,
        leaders) and saves it as JSON in the landing folder.

        Config dict should include:
            - years: {"first": int, "last": int}
            - headers: {"User-Agent": str}
            - max_results: int
            - subgenres_download: {style: bool}
            - allowed_labels: {label_key: {"name": str, "download": bool}}
            - performing_roles: [str]
            - excluded_instruments: [str]
            - sleep_between_requests: int
            - paths: {"landing": str, "raw": str}

        Environment:
            DISCOGS_TOKEN: personal token for Discogs API.
        """

    def __init__(self, config: Dict[str, Any]):

        self.__config: Dict[str, Any] = config
        self.__token:str = os.getenv("DISCOGS_TOKEN", "")
        self.__albums_id: Set[int] = set()
        self.__albums_info: List[Dict[str, Any]] = []

        allowed_labels:dict = { lab[0]: lab[1]["name"] for lab in self.__config["allowed_labels"].items() if lab[1]["download"] }
        landing_dir:str = config["paths"]["landing"]
        raw_dir:str = config["paths"]["raw"]

        self.__allowed_labels = allowed_labels
        self.__landing_dir = landing_dir
        self.__raw_dir = raw_dir

    def get_albums_info(self) -> List[Dict[str, Any]]:
        """
            Return the list with all album info collected so far.
        """

        return self.__albums_info

    def get_albums_id(self) -> Set[int]:
        """
            Return the set of album IDs already processed.
        """

        return self.__albums_id

    def _safe_request(self, url: str, max_retries: int = 5) -> Optional[dict]:
        """
            Make a GET request with retries and simple backoff.

            - Uses the headers from config.
            - If the API says we are sending requests too fast (429 or message),
              wait and try again with a longer delay.
            - On success, return the response as JSON.
            - On 404, return None.
            - On other errors, retry until the max number of attempts.

            Args:
                url: API endpoint to request.
                max_retries: how many times to retry before giving up.

            Returns:
                The JSON response as a dict, or None if all attempts fail.
            """

        delay: int = 2

        for attempt in range(max_retries):

            try:

                response: requests.Response = requests.get(url, headers=self.__config["headers"])

                if response.status_code == 429 or "too quickly" in response.text.lower():

                    print(f"Rate limited. Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2

                    continue

                response.raise_for_status()
                data: Any = response.json()
                time.sleep(self.__config.get("sleep_between_requests", 3))  # Pausa fija

                return data

            except requests.exceptions.HTTPError as e:

                if response.status_code == 404:

                    print(f"Resource not found (404): {url}")

                    return None

                print(f"Error in request attempt {attempt + 1} for {url}: {e}")
                time.sleep(delay)
                delay *= 2

            except Exception as e:

                print(f"Unexpected error in request attempt {attempt + 1} for {url}: {e}")
                time.sleep(delay)
                delay *= 2

        return None

    def _get_params(self, style: str, page: int, year: int) -> Dict[str, Any]:
        """
            Build the query parameters for the Discogs search API.

            Args:
                style: music style to search (e.g. "hard bop").
                page: page number of the results (1-based).
                year: release year to filter.

            Returns:
                Dictionary with the query parameters, including the API token.
            """

        return {
            "style": style,
            "year": year,
            "per_page": self.__config["max_results"],
            "page": page,
            "token": self.__token
        }

    def _filter_label(self, result: Dict[str, Any]) -> Optional[str]:
        """
            Check if the album label is in the allowed list. Use it as a first filter

            Args:
                result: one search result from the Discogs API.

            Returns:
                The canonical label name if it is allowed, or None if not.
            """

        labels: List[str] = result.get("label", [])

        for lab in [lab.lower() for lab in labels]:

            for key, canonical in self.__allowed_labels.items():

                if key in lab:
                    return canonical
        return None

    def _label_from_release(self, release_data: dict) -> Optional[str]:
        """
        Get the label from main release.
        - If name is stored in config, it returns the canonical name.
        - Return first name from the release otherway.
        """
        labs = release_data.get("labels") or []
        if not labs:
            return None

        # 1) check if label name is stored in config
        for lab in labs:
            name_lc = (lab.get("name") or "").lower()
            for key, canonical in self.__allowed_labels.items():
                if key in name_lc:
                    return canonical

        # 2) return the first name
        return labs[0].get("name")

    @staticmethod
    def _is_valid_format(result: Dict[str, Any]) -> bool:
        """
            Check if the album format is valid.

            Excludes 7-inch / 45 rpm / 78 releases.

            Args:
                result: one search result from the Discogs API.

            Returns:
                True if the format is accepted, False otherwise.
        """

        formats: List[str] = result.get("format", []) + result.get("format_descriptions", [])
        formats_lower: List[str] = [f.lower() for f in formats]

        if any("45 rpm" in f or "78 rpm" in f or '7"' in f or '7 inch' in f for f in formats_lower):

            return False

        return True

    @staticmethod
    def _clean_track(title: Optional[str]) -> Optional[str]:
        """
           Return a clean, canonical track title.

           This removes common editorial noise found in Discogs reissues:
             - Leading "Band #N" prefixes at the start.
             - Matrix codes at the end (e.g., " D831-1", "(S5710-1)") and anything after them.
             - Trailing " - N" take enumerations (e.g., " - 3").
             - Final parentheses that contain the word "take" (e.g., "(alt. take)", "(take #2)").
             - Trailing descriptors like " - Orig./Original/New/Short/Only Take #N".
             - Wrapping quotes around the whole title.
             - Extra spaces.

           Args:
               title: Raw track title as scraped from Discogs.

           Returns:
               The cleaned track title as a string, or None if the input is empty or just "-".
        """

        if not title or title == "-":
            return None

        # 0) Initial compilation prefix
        # e.g. :  'Band #10 ' / 'Band#10:' ...
        title = re.sub(r'^\s*Band\s*#\d+\s*[:\-]?\s*', '', title, flags=re.IGNORECASE)

        # 1) Remove matrix suffixes like D831-1: e.g. "Blue Bird D831-1" -> "Blue Bird",
        # "Blue Bird (D831-1)" -> "Blue Bird" (found in Parker's records)
        title = re.sub(r"\s+\(?[A-Z]{1,3}\d{3,5}.*$", "", title)

        # 2) Remove alternate takes suffixes: "Home Cooking - 1" -> "Home Cooking" (found in Parker's records)
        title = re.sub(r"\s*-\s*\d{1,2}\s*$", "", title)

        # 3) Remove parenthesis in alternate takes: e.g. "But Not For Me (alt. take)" -> "But Not For Me"
        m = re.match(r"^(?P<title>.*?)\s*(?P<paren>\([^)]*\btake\b[^)]*\))\s*$",
                     title,
                     flags=re.IGNORECASE)
        if m:
            title = m.group("title")

        # 4) Others take suffixes: ' - Orig. Take #4', 'New Take #1', 'Only Take'…
        title = re.sub(
            r'\s*(?:-|—)?\s*(?:New|Short|Orig(?:inal)?\.?|Only)\s+Take(?:\s*#?\s*\d+)?\s*$',
            '',
            title,
            flags=re.IGNORECASE,
        )

        # 5) Remove quotes round title:  "Billie's Bounce"
        m = re.match(r'^\s*(["“”\'])(?P<core>.+)\1\s*$', title)
        if m:
            title = m.group('core').strip()

        # 6) Substitute double spaces
        title = re.sub(r'\s{2,}', ' ', title).strip()

        return title

    def _get_tracklist_and_musicians(self, result: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, Any]]:
        """
            Get the tracklist and musicians from a Discogs result.

            Steps:
              1. From the given result, follow the master_url.
              2. Get the main release for that master.
              3. Collect track titles and extraartists.
              4. If no musicians are found, try with the most recent release.

            Args:
                result: one search result (type master or release).

            Returns:
                A tuple with:
                    - tracklist: list of track titles
                    - musicians_raw: list of musician names (not cleaned yet)
                    - release_data: JSON dict of the main release
        """

        def collect_musicians(extraartists: List[Dict], musicians_raw: List[str]):

            excluded_instruments: List[str] = self.__config["excluded_instruments"]
            performing_roles: List[str] = self.__config["performing_roles"]

            for art in extraartists:

                name: str = art.get("name")

                if not name:

                    continue

                role: str = art.get("role", "").lower()
                has_perf_role: bool = any(tok in role for tok in performing_roles)
                has_exc_inst: bool =  any(tok in role for tok in excluded_instruments)

                # If it is a performing role or not in excluded list, keep the name
                if has_perf_role or not has_exc_inst:

                    musicians_raw.append(name)


        tracklist: List[str] = []
        musicians_raw: List[str] = []
        release_data: Dict[str, Any] = {}

        master_url: str | None = result.get("master_url")

        if not master_url:

            return tracklist, musicians_raw, release_data

        master_data: dict | None = self._safe_request(master_url)

        if not master_data:

            return tracklist, musicians_raw, release_data

        main_release_url: str | None = master_data.get("main_release_url")

        if not main_release_url:

            return tracklist, musicians_raw, release_data

        release_data: dict | None = self._safe_request(main_release_url)

        if not release_data:

            return tracklist, musicians_raw, release_data

        # Get the tracklist
        for t in release_data.get("tracklist", []):

            title = t.get("title")
            title = self._clean_track(title)

            if title:

                tracklist.append(title.strip())

        # Get the musicians from the album
        extraartists: List[dict] = release_data.get("extraartists", [])

        # First, try to obtain the artist list from filed extraartist from main release
        collect_musicians(extraartists, musicians_raw)

        # If not possible, we try to get the artist list from most_recent_release
        if not musicians_raw:

            mrr_url: str | None  = master_data.get("most_recent_release_url")

            if mrr_url and mrr_url != main_release_url:

                mrr_data: Dict[str, Any] = self._safe_request(mrr_url) or {}
                extraartists: List[dict] = mrr_data.get("extraartists", [])
                collect_musicians(extraartists, musicians_raw)

        return tracklist, musicians_raw, release_data

    @staticmethod
    def _clean_musicians(musicians_raw: List[str]) -> List[str]:
        """
            Clean the raw list of musician names.

            - Remove duplicates.
            - Remove "(1)", "(2)", ... that Discogs uses for disambiguation.

            Args:
                musicians_raw: list of musician names, may contain duplicates.

            Returns:
                List of unique and cleaned musician names.
        """

        musicians: List[str] = list(set(musicians_raw))

        # Remove parenthesis with numbers used by Discogs for artist disambiguation
        return [re.sub(r"\s*\(\d+\)$", "", name) for name in musicians]

    @staticmethod
    def _identify_leaders(artists: str, musicians: List[str]) -> List[str]:
        """
            First method to identify session leaders.

            Idea: if a musician's name appears inside the artist string,
            we consider that musician as a leader.
            Example: "Miles Davis Quintet" -> leader: "Miles Davis".

            Args:
                artists: artist or group name from the release.
                musicians: list of musician names.

            Returns:
                List of detected leaders (can be empty).
        """

        if not artists or not musicians:
            return []

        artists: str = artists.lower()
        return [m for m in musicians if m.lower() in artists]

    def _identify_leaders_from_members(self, release_data: Dict[str, Any]) -> List[str]:
        """
            Second method to identify session leaders.

            If we cannot find leaders with the first method, we try this:
            - Take the resource_url of the main artist or group.
            - Ask Discogs for the details of that artist (this includes
              all members that have ever been part of the group).
            - Consider as leaders those members whose names appear
              inside the group/artist name string.

            Example:
                Artist: "Miles Davis Quintet"
                Members: ["Miles Davis", "John Coltrane", ...]
                -> Leader: "Miles Davis"

            Args:
                release_data: JSON dict with release info, must include "artists".

            Returns:
                List of detected leaders (unique names).
        """

        leaders: List[str] = []
        artists: List[str] = release_data.get("artists", [])

        if not artists:
            return leaders

        group: str = artists[0]
        artists_str: str = (group.get("name") or group.get("anv") or "").strip()

        # Return empty list if it is not possible to get the artist/band string
        if not artists_str:
            return leaders

        resource_url: str | None = group.get("resource_url")
        # Return empty list if it is not possible to get the artist/band resource url
        if not resource_url:
            return leaders

        artist_detail: dict | None = self._safe_request(resource_url)
        # Return empty list if the request for artist/band details fails
        if not artist_detail:
            return leaders

        # Only keep members whose names appear inside the artist/band name
        for member in artist_detail.get("members", []):
            name: str | None = member.get("name")
            if name.lower() in artists_str.lower():
                leaders.append(name)
        return list(set(leaders))

    def print_albums(self) -> None:
        """
            Print all albums collected so far.

            It goes through the internal list of albums and shows:
            - artist name
            - leaders
            - title, id, year, label, style
            - cover URL
            - tracklist
            - musicians

            All albums are printed together at the end of the process.
        """

        print("\n============================================================")
        print("\nAlbum Information:\n")

        for id, album in enumerate(self.__albums_info, start=1):
            print(f"  Album {id}")
            print(f"  Artists : {album['artists']}")
            print(f"  Leaders : {', '.join(album['leaders'])}")
            print(f"  Title   : {album['title']}")
            print(f"  ID      : {album['id']}")
            print(f"  Year    : {album['year']}")
            print(f"  Label   : {album['label']}")
            print(f"  Style   : {', '.join(album['style'])}")
            print(f"  Cover   : {album['cover_url']}")
            print("  Tracklist:")
            if album['tracklist']:
                for i, track in enumerate(album['tracklist'], start=1):
                    print(f"    {i}. {track}")
            else:
                print("    (Not available)")
            print("  Musicians:")
            if album['musicians']:
                for mus in album['musicians']:
                    print(f"    - {mus}")
            else:
                print("    (Not available)")
            print("\n------------------------------------------------------------\n")

    @staticmethod
    def save_json(album_data: dict, landing_path_json: str):
        """
            Save one album as a JSON file in the landing folder.

            Notes:
                Before calling this method, the code checks that the file does not
                already exist in landing or raw. Because of that, this method is not
                expected to overwrite any file.

            Args:
                album_data: dictionary with album information.
                landing_path_json: full path where the JSON will be saved.

            Side effects:
                Creates a new JSON file on disk and prints a message with the path.
        """

        with open(landing_path_json, "w", encoding="utf-8") as f:
            json.dump(album_data, f, ensure_ascii=False, indent=4)
        print(f"Saved: {landing_path_json}")

    def download_albums(self) -> None:
        """
            Orchestrate the Discogs crawl and save album JSON files.

            Flow:
              1) For each selected style and year, call the Discogs search API (paginated).
              2) Filter out formats we don't want (7"/45 rpm) and disallowed labels.
              3) Normalize results to master:
                 - If result is a master -> process directly.
                 - If result is a release -> resolve it to its master and then process.
              4) For each master, fetch tracklist and musicians, detect leaders, and
                 save the album as a JSON file in the landing folder.

            Notes:
              - Deduplication is done by master_id using an in-memory set and by
                checking if files already exist in landing/raw. Because of that,
                this process is not expected to overwrite files.
              - It prints progress (style/year/page) and a running count of stored albums.
        """

        def download_from_master(result: dict, label_hint: str, landing_path_json: str) -> None:
            """
                Process a 'master-like' result and persist its album data.
            """

            mid: str | None = result.get("id")  # master_id
            year: Any = result.get("year")

            # Check again that the master year is inside the study period.
            # This is needed when the album came from a 'release' result (not from a master).
            if not year or int(year) < self.__config["years"]["first"]:
                return

            tracklist, raw_musicians, release_data = self._get_tracklist_and_musicians(result)
            musicians: List[str] = self._clean_musicians(raw_musicians)

            artists: str = release_data.get("artists", [{}])[0].get("name", "") if release_data else ""

            # Discard compilations credited to "Various"
            if artists.lower() == "various":
                return

            title: str = release_data.get("title", "") if release_data else ""

            # try method 1 (string match) or method 2 (members)
            leaders: List[str] = self._identify_leaders(artists, musicians)

            if not leaders and release_data:
                leaders = self._identify_leaders_from_members(release_data)

            # If both methods failed, assign the artist/band name to leaders
            if not leaders and artists:
                leaders = [artists]

            # Get definitive label from main_release
            label_final: str = self._label_from_release(release_data) or label_hint
            # Quitamos posibles paréntesis de desambiguación "Argo (6)" -> "Argo"
            m = re.match(r"(.*)(\s+\(\d+\))$", label_final)
            if m:
                label_final = m.group(1)

            album_data = {
                "artists": artists,
                "title": title,
                "id": mid,
                "year": year,
                "label": label_final,
                "tracklist": tracklist,
                "musicians": musicians,
                "leaders": leaders,
                "style": result.get("style", []),
                "cover_url": result.get("cover_image"),
            }

            self.__albums_id.add(mid)
            self.__albums_info.append(album_data)
            self.save_json(album_data, landing_path_json)


        def get_master_from_release(result: dict):
            """
                From a 'release' search result, resolve and return a 'master-like' object.
            """

            # Get full release information
            rel_url: str = result.get("resource_url") or f"https://api.discogs.com/releases/{result['id']}"
            rel_json: dict | None = self._safe_request(rel_url)

            if not rel_json:

                print(f"Couldn't get the release {result['id']}. Skipping.")

                return None

            # Most releases point to their master via master_id/master_url
            mid: str | None = rel_json.get("master_id")
            murl: str | None = rel_json.get("master_url") or (f"https://api.discogs.com/masters/{mid}" if mid else None)

            if not murl:

                print(f"Release {result['id']} with no master_id/master_url -> Skipping process.")

                return None

            # Request the master information
            master_json: dict | None = self._safe_request(murl)

            if not master_json:

                print(f"Release {result['id']} had master, but the request failed.")

                return None

            else:

                # Build a 'master-like' object so we can reuse download_from_master
                master_like = {
                    "id": master_json.get("id"),
                    "type": "master",
                    "title": master_json.get("title"),
                    "style": master_json.get("styles") or [],
                    "cover_image": result.get("cover_image"),
                    "resource_url": murl,
                    "master_url": murl,
                    "year": master_json.get("year")
                }

                return master_like

        search_url: str = os.getenv("SEARCH_URL", "")

        # We use these paths to check if an album was already downloaded
        raw_dir: str = self.__config["paths"]["raw"]
        landing_dir: str = self.__config["paths"]["landing"]

        # Get the subgenres to study from user's choice
        subgenres: List[str] = [ style[0] for style in self.__config["subgenres_download"].items() if style[1] ]

        for style in subgenres:

            for year in range(self.__config["years"]["first"], self.__config["years"]["last"]+1):

                params: dict[str, Any] = self._get_params(style=style, page=1, year=year)
                first_url: str = f"{search_url}?style={params['style']}&year={params['year']}"\
                                 f"&per_page={params['per_page']}&page={params['page']}&token={params['token']}"

                first_data: dict | None = self._safe_request(first_url)

                if not first_data:
                    continue

                total_pages: int = first_data.get("pagination", {}).get("pages", 1)
                print(f"Style: {style}, Year: {year}, Total pages: {total_pages}")

                for page in range(1, total_pages + 1):

                    print(f"Style: {style}, Year: {year}, Page: {page}")
                    params: dict[str, Any] = self._get_params(style, page, year)
                    url: str = f"{search_url}?style={params['style']}&year={params['year']}"\
                               f"&per_page={params['per_page']}&page={params['page']}&token={params['token']}"
                    data: dict | None = self._safe_request(url)

                    if not data or not data.get("results"):
                        break

                    for result in data.get("results", []):

                        year_val: Any | None = result.get("year")

                        # Check the year is valid
                        if not (year_val and str(year_val).isdigit() and
                                self.__config["years"]["first"] <= int(year_val) <= self.__config["years"]["last"]):
                            continue

                        # Filter out 7"/45 rpm release
                        if not self._is_valid_format(result):
                            continue

                        # Check album was released by a company listed in config
                        label: str | None = self._filter_label(result)

                        if not label:
                            continue

                        if result["type"] == "master":

                            album_id: int | None = result.get("id")

                            # Deduplicate by master_id before processing
                            if album_id in self.__albums_id:

                                print(f"Skipping {album_id} (master), stored in memory.")

                                continue

                            raw_path_json: str = os.path.join(raw_dir, "albums", f"{album_id}.json")
                            landing_path_json: str = os.path.join(landing_dir, f"{album_id}.json")

                            # Check file wasn't created before
                            if os.path.exists(raw_path_json) or os.path.exists(landing_path_json):

                                print(f"Skipping {album_id}, it's been already saved.")

                                continue

                            download_from_master(result, label, landing_path_json)

                        elif result["type"] == "release":

                            master_like: dict | None = get_master_from_release(result)

                            if not master_like:
                                continue

                            album_id: int | None = master_like.get("id")

                            if album_id in self.__albums_id:
                                print(f"Skipping {album_id} (master), stored in memory.")
                                continue

                            raw_path_json: str = os.path.join(raw_dir, "albums", f"{album_id}.json")
                            landing_path_json: str = os.path.join(landing_dir, f"{album_id}.json")

                            if os.path.exists(raw_path_json) or os.path.exists(landing_path_json):

                                print(f"Skipping {album_id}, it's been already saved.")

                                continue

                            download_from_master(master_like, label, landing_path_json)

                    time.sleep(1.5)
                    print(f"Number of stored albums: {len(self.__albums_info)}")