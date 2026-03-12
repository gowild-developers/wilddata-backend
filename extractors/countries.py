"""
Country-specific government data sources.
Each function is an async generator yielding result dicts.
"""

import httpx
from typing import AsyncGenerator, Dict, Any, List
from utils.rate_limiter import rate_limiter

# ── USA — USGS + NPS ──────────────────────────────────────────────────────────

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
                headers={"X-Api-Key": "DEMO_KEY"},  # DEMO_KEY works for testing, 40 req/hr
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

async def fetch_uk(
    lat: float, lng: float, radius_km: float, feature_ids: List[str]
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    UK: OS Open Names API (free, no key for basic use)
    + Natural England protected areas
    """
    try:
        await rate_limiter.wait("api.os.uk", 0.5)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.os.uk/search/names/v1/nearest",
                params={
                    "point": f"{lng},{lat}",
                    "radius": min(radius_km * 1000, 100000),
                    "key": "FREE_TIER",  # OS free tier
                },
            )
            # OS API requires a free key — fallback to Wikipedia
    except:
        pass

    # UK Wikipedia GeoSearch as reliable fallback
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
                uk_keywords = ['moor', 'fell', 'ben', 'loch', 'glen', 'peak', 'dale', 'forest', 'park', 'coast', 'cliff', 'waterfall', 'lake', 'mountain', 'hill', 'cave', 'bay']
                if any(kw in title.lower() for kw in uk_keywords):
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
                        "country": "United Kingdom",
                        "image": "",
                        "osm_id": "",
                        "source": "Wikipedia (UK)",
                        "confidence": "High",
                    }
    except Exception as e:
        print(f"[UK] error: {e}")


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
