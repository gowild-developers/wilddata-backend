"""
Country-specific government data sources.
Each function is an async generator yielding result dicts.
"""

import httpx
from typing import AsyncGenerator, Dict, Any, List
from utils.rate_limiter import rate_limiter
from extractors.greece import fetch_greece

# ── USA — USGS + NPS ──────────────────────────────────────────────────────────
import os
NPS_API_KEY = os.getenv("USA_NPS_KEY", "DEMO_KEY")

async def fetch_usa(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    USA: National Park Service API (free, no key needed for basic endpoints)
    + USGS Geographic Names Information System (GNIS)
    """
    # NPS — find parks near coordinates
    try:
        await rate_limiter.wait("developer.nps.gov", 0.5)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://developer.nps.gov/api/v1/parks",
                params={
                    "limit": 50,
                    "start": 0,
                    "q": "",
                    "fields": "addresses,contacts,entranceFees,hours,images,operatingHours",
                },
                headers={"X-Api-Key": NPS_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                parks = data.get("data", [])
                for park in parks:
                    try:
                        park_lat = float(park.get("latitude", 0) or 0)
                        park_lng = float(park.get("longitude", 0) or 0)
                        if park_lat == 0:
                            continue
                        # Check if within radius
                        import math
                        dlat = math.radians(park_lat - lat)
                        dlng = math.radians(park_lng - lng)
                        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat)) * math.cos(math.radians(park_lat)) * math.sin(dlng/2)**2
                        dist = 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                        if dist > radius_km:
                            continue

                        images = park.get("images", [])
                        image_url = images[0].get("url", "") if images else ""

                        yield {
                            "name": park.get("fullName", park.get("name", "")),
                            "type": "National Park",
                            "type_id": "park",
                            "lat": park_lat,
                            "lng": park_lng,
                            "elevation": "",
                            "description": park.get("description", "")[:500],
                            "wikipedia": f"https://en.wikipedia.org/wiki/{park.get('fullName','').replace(' ', '_')}",
                            "website": park.get("url", ""),
                            "region": park.get("states", ""),
                            "country": "United States",
                            "image": image_url,
                            "osm_id": "",
                            "source": "NPS (USA)",
                            "confidence": "High",
                        }
                    except:
                        continue
    except Exception as e:
        print(f"[USA NPS] error: {e}")

    # USGS GNIS — Geographic Names (waterfalls, peaks, streams)
    feature_class_map = {
        "waterfall": "Falls",
        "peak": "Summit",
        "cave": "Cave",
        "beach": "Beach",
        "hot_spring": "Spring",
        "waterway": "Lake",
    }

    for fid in feature_ids:
        fc = feature_class_map.get(fid)
        if not fc:
            continue
        try:
            await rate_limiter.wait("geonames.usgs.gov", 1.0)
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    "https://geonames.usgs.gov/api/geonames/search",
                    params={
                        "featureClass": fc,
                        "radius": radius_km,
                        "latitude": lat,
                        "longitude": lng,
                        "maxRows": 100,
                        "type": "json",
                        "username": "wilddata",
                    },
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                for item in data.get("geonames", []):
                    yield {
                        "name": item.get("name", ""),
                        "type": fc,
                        "type_id": fid,
                        "lat": float(item.get("lat", 0)),
                        "lng": float(item.get("lng", 0)),
                        "elevation": str(item.get("elevation", "")) + "m" if item.get("elevation") else "",
                        "description": item.get("fcodeName", ""),
                        "wikipedia": f"https://en.wikipedia.org/wiki/{item.get('name','').replace(' ','_')}",
                        "website": "",
                        "region": item.get("adminName1", ""),
                        "country": "United States",
                        "image": "",
                        "osm_id": "",
                        "source": "USGS GNIS (USA)",
                        "confidence": "High",
                    }
        except Exception as e:
            print(f"[USA USGS] {fid} error: {e}")


# ── France — data.gouv.fr ─────────────────────────────────────────────────────

async def fetch_france(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    France: data.gouv.fr — IGN official trails and natural areas.
    Uses the Geo API for communes and natural features.
    """
    try:
        # France GeoAPI — get nearby communes
        await rate_limiter.wait("geo.api.gouv.fr", 0.5)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://geo.api.gouv.fr/communes",
                params={
                    "lat": lat,
                    "lon": lng,
                    "fields": "nom,code,departement,region,centre",
                    "format": "json",
                    "geometry": "centre",
                    "limit": 20,
                },
            )
            if resp.status_code == 200:
                communes = resp.json()
                # Use commune names to search IGN data
                for commune in communes[:5]:
                    region = commune.get("region", {}).get("nom", "")
                    dept = commune.get("departement", {}).get("nom", "")
                    centre = commune.get("centre", {}).get("coordinates", [lng, lat])

                    yield {
                        "name": f"Commune: {commune.get('nom', '')}",
                        "type": "Administrative Area",
                        "type_id": "park",
                        "lat": centre[1],
                        "lng": centre[0],
                        "elevation": "",
                        "description": f"Commune in {dept}, {region}, France",
                        "wikipedia": "",
                        "website": "https://www.data.gouv.fr",
                        "region": f"{dept}, {region}",
                        "country": "France",
                        "image": "",
                        "osm_id": "",
                        "source": "data.gouv.fr (France)",
                        "confidence": "High",
                    }
    except Exception as e:
        print(f"[France data.gouv.fr] error: {e}")

    # France National Parks via Wikipedia (most reliable free source)
    try:
        await rate_limiter.wait("en.wikipedia.org", 0.5)
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "geosearch",
                    "gscoord": f"{lat}|{lng}",
                    "gsradius": min(radius_km * 1000, 10000),
                    "gslimit": 50,
                    "format": "json",
                    "origin": "*",
                },
            )
            data = resp.json()
            for page in data.get("query", {}).get("geosearch", []):
                title = page.get("title", "")
                french_keywords = ['forêt', 'parc', 'cascade', 'gorge', 'mont', 'lac', 'rivière', 'grotte', 'col']
                if any(kw in title.lower() for kw in french_keywords + ['park', 'forest', 'waterfall', 'lake', 'mountain', 'cave', 'pass']):
                    from extractors.wikipedia import guess_type
                    type_label, type_id = guess_type(title)
                    yield {
                        "name": title,
                        "type": type_label,
                        "type_id": type_id,
                        "lat": page.get("lat", 0),
                        "lng": page.get("lon", 0),
                        "elevation": "",
                        "description": "",
                        "wikipedia": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
                        "website": "",
                        "region": "",
                        "country": "France",
                        "image": "",
                        "osm_id": "",
                        "source": "Wikipedia+data.gouv.fr (France)",
                        "confidence": "High",
                    }
    except Exception as e:
        print(f"[France Wikipedia] error: {e}")


# ── UK — Ordnance Survey Open Data ───────────────────────────────────────────

OS_API_KEY = os.getenv("OS_API_KEY", "")

# OS Names API local type → WildData type mapping
OS_TYPE_MAP = {
    "waterfall":        ("Waterfall",      "waterfall"),
    "lake":             ("Lake",           "lake"),
    "loch":             ("Lake",           "lake"),
    "reservoir":        ("Lake",           "lake"),
    "mountain":         ("Peak",           "peak"),
    "hill":             ("Peak",           "peak"),
    "fell":             ("Peak",           "peak"),
    "ben":              ("Peak",           "peak"),
    "summit":           ("Peak",           "peak"),
    "forest":           ("Forest",         "forest"),
    "wood":             ("Forest",         "forest"),
    "national park":    ("National Park",  "national_park"),
    "country park":     ("Park",           "park"),
    "valley":           ("Valley",         "valley"),
    "glen":             ("Valley",         "valley"),
    "dale":             ("Valley",         "valley"),
    "gorge":            ("Canyon",         "canyon"),
    "cave":             ("Cave",           "cave"),
    "cliff":            ("Viewpoint",      "viewpoint"),
    "bay":              ("Beach",          "beach"),
    "beach":            ("Beach",          "beach"),
    "moor":             ("Moor",           "nature_reserve"),
    "heath":            ("Moor",           "nature_reserve"),
    "nature reserve":   ("Nature Reserve", "nature_reserve"),
    "river":            ("River",          "river"),
    "stream":           ("River",          "river"),
    "island":           ("Island",         "island"),
}

def _os_guess_type(local_type: str, name: str):
    """Map OS local type string to WildData type."""
    lt = (local_type or "").lower()
    nm = (name or "").lower()
    for key, val in OS_TYPE_MAP.items():
        if key in lt or key in nm:
            return val
    return ("Natural Feature", "natural")

def _os_within_radius(lat, lng, feat_lat, feat_lng, radius_km):
    import math
    R = 6371
    dlat = math.radians(feat_lat - lat)
    dlng = math.radians(feat_lng - lng)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat)) * math.cos(math.radians(feat_lat)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)) <= radius_km

async def fetch_uk(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    UK: Ordnance Survey Names API (official UK geographic names database)
    Covers: peaks, lakes, waterfalls, forests, national parks, valleys, caves,
            beaches, rivers, moors — all official OS named features.
    API key: OS Data Hub free tier (1M transactions/month)
    """

    # OS Names categories relevant to outdoor features
    OS_LOCAL_TYPES = [
        "Waterfall", "Lake", "Loch", "Reservoir", "Mountain", "Hill", "Fell",
        "Forest", "Wood", "National Park", "Country Park", "Valley", "Glen",
        "Dale", "Gorge", "Cave", "Cliff", "Bay", "Beach", "Moor", "Heath",
        "Nature Reserve", "River", "Stream", "Island", "Summit", "Nature Reserve",
        "Area of Outstanding Natural Beauty", "Site of Special Scientific Interest",
    ]

    seen = set()
    radius_m = int(min(radius_km * 1000, 100000))  # OS max 100km

    # OS Names API /find with bbox — returns up to 100 results per query
    # Correct format: bbox=minX,minY,maxX,maxY (lng/lat order)
    deg = radius_km / 111.0
    bbox = f"{lng-deg},{lat-deg},{lng+deg},{lat+deg}"

    # Search terms covering all outdoor feature types
    search_terms = [
        "fell", "moor", "forest", "lake", "loch", "reservoir",
        "waterfall", "force", "beck", "tarn", "peak", "mountain",
        "hill", "dale", "valley", "gorge", "cave", "cliff",
        "national park", "nature reserve", "bay", "beach", "island",
    ]

    async with httpx.AsyncClient(timeout=30) as client:
        for term in search_terms:
            try:
                await rate_limiter.wait("api.os.uk", 0.6)
                resp = await client.get(
                    "https://api.os.uk/search/names/v1/find",
                    params={
                        "query":      term,
                        "fq":         f"bbox:{bbox}",
                        "maxresults": 100,
                        "key":        OS_API_KEY,
                    },
                )
                if resp.status_code != 200:
                    print(f"[UK/OS] {term} → HTTP {resp.status_code}: {resp.text[:200]}")
                    continue

                data = resp.json()
                for feat in data.get("results", []):
                    g = feat.get("GAZETTEER_ENTRY", {})
                    name = g.get("NAME1", "") or g.get("NAME2", "")
                    if not name or name in seen:
                        continue
                    # OS returns coordinates in EPSG:27700 (BNG) as GEOMETRY_X/Y
                    # but also provides LNG/LAT directly
                    f_lat = g.get("LAT") or g.get("GEOMETRY_Y")
                    f_lng = g.get("LNG") or g.get("GEOMETRY_X")
                    if not f_lat or not f_lng:
                        continue
                    try:
                        f_lat, f_lng = float(f_lat), float(f_lng)
                    except (TypeError, ValueError):
                        continue
                    # Sanity check — UK is lat 49-61, lng -8 to 2
                    if not (49 < f_lat < 61 and -8 < f_lng < 2):
                        continue
                    if not _os_within_radius(lat, lng, f_lat, f_lng, radius_km):
                        continue
                    seen.add(name)
                    local_type = g.get("LOCAL_TYPE", "")
                    type_label, type_id = _os_guess_type(local_type, name)
                    yield {
                        "name":        name,
                        "type":        type_label,
                        "lat":         round(f_lat, 6),
                        "lng":         round(f_lng, 6),
                        "elevation":   "",
                        "region":      g.get("DISTRICT_BOROUGH", "") or g.get("COUNTY_UNITARY", "") or g.get("POPULATED_PLACE", ""),
                        "country":     "United Kingdom",
                        "description": f"{local_type}",
                        "wikipedia":   "",
                        "website":     "https://osdatahub.os.uk",
                        "image":       "",
                        "osm_id":      g.get("OS_ID", ""),
                        "source":      "Ordnance Survey (OS Names API)",
                        "confidence":  "High",
                    }
            except Exception as e:
                print(f"[UK/OS] {term} error: {e}")
                continue


# ── New Zealand — DOC API ─────────────────────────────────────────────────────

async def fetch_newzealand(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    New Zealand: Department of Conservation (DOC) API — completely free.
    Has every hut, trail, campsite, park in NZ wilderness.
    """
    DOC_BASE = "https://api.doc.govt.nz/v2"

    endpoint_map = {
        "hiking": "/tracks",
        "camp": "/campsites",
        "park": "/parks",
        "viewpoint": "/tracks",  # closest match
    }

    headers = {"x-api-key": ""}  # DOC API is fully open, no key needed

    for fid in feature_ids:
        ep = endpoint_map.get(fid)
        if not ep:
            continue

        try:
            await rate_limiter.wait("api.doc.govt.nz", 0.5)
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{DOC_BASE}{ep}",
                    params={
                        "lat": lat,
                        "lon": lng,
                        "radius": radius_km,
                        "limit": 100,
                    },
                )
                if resp.status_code != 200:
                    continue
                items = resp.json()

            for item in (items if isinstance(items, list) else []):
                coords = item.get("location", {})
                item_lat = coords.get("lat", 0) or 0
                item_lng = coords.get("lon", 0) or coords.get("lng", 0) or 0
                if not item_lat:
                    continue

                yield {
                    "name": item.get("name", ""),
                    "type": {"hiking": "Hiking Route", "camp": "Campsite", "park": "National Park"}.get(fid, fid),
                    "type_id": fid,
                    "lat": item_lat,
                    "lng": item_lng,
                    "elevation": "",
                    "description": item.get("introductory", item.get("description", ""))[:500],
                    "wikipedia": "",
                    "website": f"https://www.doc.govt.nz{item.get('url', '')}",
                    "region": item.get("region", ""),
                    "country": "New Zealand",
                    "image": (item.get("images") or [{}])[0].get("url", "") if item.get("images") else "",
                    "osm_id": "",
                    "source": "DOC (New Zealand)",
                    "confidence": "High",
                }

        except Exception as e:
            print(f"[NZ DOC] {fid} error: {e}")


# ── Australia — data.gov.au ───────────────────────────────────────────────────

async def fetch_australia(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Australia: data.gov.au CKAN API — free, no key needed.
    """
    try:
        await rate_limiter.wait("data.gov.au", 0.5)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://data.gov.au/api/3/action/package_search",
                params={
                    "q": "national park trails",
                    "rows": 20,
                },
            )
            # data.gov.au returns dataset metadata, not geo features directly
            # For actual geo features, use Australian OSM + Wikipedia
    except Exception as e:
        print(f"[AU data.gov.au] error: {e}")

    # Australia Wikipedia GeoSearch
    try:
        await rate_limiter.wait("en.wikipedia.org", 0.5)
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "geosearch",
                    "gscoord": f"{lat}|{lng}",
                    "gsradius": min(radius_km * 1000, 10000),
                    "gslimit": 50,
                    "format": "json",
                    "origin": "*",
                },
            )
            data = resp.json()
            au_keywords = ['gorge', 'falls', 'national park', 'ranges', 'beach', 'reef', 'lagoon', 'creek', 'billabong', 'outback', 'rock', 'cave', 'mountain', 'peak', 'coast', 'bay']
            for page in data.get("query", {}).get("geosearch", []):
                title = page.get("title", "")
                if any(kw in title.lower() for kw in au_keywords):
                    from extractors.wikipedia import guess_type
                    type_label, type_id = guess_type(title)
                    yield {
                        "name": title,
                        "type": type_label,
                        "type_id": type_id,
                        "lat": page.get("lat", 0),
                        "lng": page.get("lon", 0),
                        "elevation": "",
                        "description": "",
                        "wikipedia": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
                        "website": "",
                        "region": "",
                        "country": "Australia",
                        "image": "",
                        "osm_id": "",
                        "source": "Wikipedia (Australia)",
                        "confidence": "High",
                    }
    except Exception as e:
        print(f"[AU Wikipedia] error: {e}")


# ── Japan — GSI API ───────────────────────────────────────────────────────────

async def fetch_japan(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Japan: GSI (Geospatial Information Authority) — free API.
    + Wikipedia Japanese/English GeoSearch.
    """
    # GSI Reverse Geocode to get area name
    try:
        await rate_limiter.wait("mreversegeocoder.gsi.go.jp", 0.5)
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                "https://mreversegeocoder.gsi.go.jp/reverse-geocoder/LonLatToAddress",
                params={"lat": lat, "lon": lng},
            )
            if resp.status_code == 200:
                data = resp.json()
                muniNm = data.get("results", {}).get("muniNm", "")
                lv01Nm = data.get("results", {}).get("lv01Nm", "")
    except Exception as e:
        print(f"[Japan GSI] error: {e}")

    # Japan Wikipedia GeoSearch (English + Japanese features)
    try:
        await rate_limiter.wait("en.wikipedia.org", 0.5)
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "geosearch",
                    "gscoord": f"{lat}|{lng}",
                    "gsradius": min(radius_km * 1000, 10000),
                    "gslimit": 50,
                    "format": "json",
                    "origin": "*",
                },
            )
            data = resp.json()
            jp_keywords = ['mount', 'peak', 'lake', 'falls', 'onsen', 'shrine', 'park', 'coast', 'beach', 'forest', 'trail', 'gorge', 'valley', 'volcano', 'jinja', 'yama', 'ko', 'taki']
            for page in data.get("query", {}).get("geosearch", []):
                title = page.get("title", "")
                if any(kw in title.lower() for kw in jp_keywords):
                    from extractors.wikipedia import guess_type
                    type_label, type_id = guess_type(title)
                    yield {
                        "name": title,
                        "type": type_label,
                        "type_id": type_id,
                        "lat": page.get("lat", 0),
                        "lng": page.get("lon", 0),
                        "elevation": "",
                        "description": "",
                        "wikipedia": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
                        "website": "",
                        "region": "",
                        "country": "Japan",
                        "image": "",
                        "osm_id": "",
                        "source": "GSI+Wikipedia (Japan)",
                        "confidence": "High",
                    }
    except Exception as e:
        print(f"[Japan Wikipedia] error: {e}")


# ── India — Bhuvan + data.gov.in ─────────────────────────────────────────────

async def fetch_india(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    India: Bhuvan NRSC + data.gov.in + Wikipedia GeoSearch.
    Wikidata is primary for India (OSM is sparse).
    """
    # India Wikipedia GeoSearch — best source for India
    try:
        await rate_limiter.wait("en.wikipedia.org", 0.5)
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "geosearch",
                    "gscoord": f"{lat}|{lng}",
                    "gsradius": min(radius_km * 1000, 10000),
                    "gslimit": 50,
                    "format": "json",
                    "origin": "*",
                },
            )
            data = resp.json()
            india_keywords = [
                'falls', 'waterfall', 'peak', 'pass', 'trek', 'trail',
                'lake', 'river', 'valley', 'forest', 'reserve', 'sanctuary',
                'national park', 'wildlife', 'beach', 'cave', 'temple',
                'kund', 'tal', 'dhar', 'ghati', 'nala', 'jharna',
            ]
            for page in data.get("query", {}).get("geosearch", []):
                title = page.get("title", "")
                if any(kw in title.lower() for kw in india_keywords):
                    from extractors.wikipedia import guess_type
                    type_label, type_id = guess_type(title)
                    yield {
                        "name": title,
                        "type": type_label,
                        "type_id": type_id,
                        "lat": page.get("lat", 0),
                        "lng": page.get("lon", 0),
                        "elevation": "",
                        "description": "",
                        "wikipedia": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
                        "website": "",
                        "region": "",
                        "country": "India",
                        "image": "",
                        "osm_id": "",
                        "source": "Wikipedia (India)",
                        "confidence": "High",
                    }
    except Exception as e:
        print(f"[India Wikipedia] error: {e}")

    # data.gov.in — Protected Areas
    if "park" in feature_ids:
        try:
            await rate_limiter.wait("api.data.gov.in", 1.0)
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    "https://api.data.gov.in/resource/f1a8c4ca-186e-4b46-be9e-ae32fd54e9fa",
                    params={
                        "api-key": "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b",
                        "format": "json",
                        "limit": 100,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for record in data.get("records", []):
                        try:
                            r_lat = float(record.get("latitude", 0) or 0)
                            r_lng = float(record.get("longitude", 0) or 0)
                            if r_lat == 0:
                                continue
                            import math
                            dlat = math.radians(r_lat - lat)
                            dlng_r = math.radians(r_lng - lng)
                            a = math.sin(dlat/2)**2 + math.cos(math.radians(lat)) * math.cos(math.radians(r_lat)) * math.sin(dlng_r/2)**2
                            dist = 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                            if dist > radius_km:
                                continue
                            yield {
                                "name": record.get("name", ""),
                                "type": "National Park",
                                "type_id": "park",
                                "lat": r_lat,
                                "lng": r_lng,
                                "elevation": "",
                                "description": f"Protected area in {record.get('state', 'India')}",
                                "wikipedia": "",
                                "website": "https://data.gov.in",
                                "region": record.get("state", ""),
                                "country": "India",
                                "image": "",
                                "osm_id": "",
                                "source": "data.gov.in (India)",
                                "confidence": "High",
                            }
                        except:
                            continue
        except Exception as e:
            print(f"[India data.gov.in] error: {e}")


# ── Dispatcher ────────────────────────────────────────────────────────────────

# Country code → fetch function
COUNTRY_EXTRACTORS = {
    "US": fetch_usa,
    "FR": fetch_france,
    "GB": fetch_uk,
    "NZ": fetch_newzealand,
    "AU": fetch_australia,
    "JP": fetch_japan,
    "IN": fetch_india,
    "GR": fetch_greece,
}

async def fetch_country_specific(
    country_code: str,
    lat: float,
    lng: float,
    radius_km: float,
    feature_ids: List[str],
) -> AsyncGenerator[Dict[str, Any], None]:
    """Route to the correct country extractor."""
    fn = COUNTRY_EXTRACTORS.get(country_code.upper())
    if fn:
        async for item in fn(lat, lng, radius_km, feature_ids):
            yield item
