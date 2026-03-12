import httpx
from typing import List, Dict, Any, AsyncGenerator
from utils.rate_limiter import rate_limiter

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

FEATURE_TAGS = {
    "waterfall":    ('node', '"waterway"="waterfall"'),
    "hiking":       ('relation', '"route"="hiking"'),
    "mtb":          ('relation', '"route"="mtb"'),
    "motorbiking":  ('relation', '"route"="motorcycle"'),
    "peak":         ('node', '"natural"="peak"'),
    "park":         ('relation', '"boundary"="national_park"'),
    "viewpoint":    ('node', '"tourism"="viewpoint"'),
    "camp":         ('node', '"tourism"="camp_site"'),
    "cave":         ('node', '"natural"="cave_entrance"'),
    "hot_spring":   ('node', '"natural"="hot_spring"'),
    "waterway":     ('node', '"natural"~"water|spring"'),
    "beach":        ('node|way', '"natural"="beach"'),
    "glacier":      ('way', '"natural"="glacier"'),
    "volcano":      ('node', '"natural"="volcano"'),
    "forest":       ('relation', '"boundary"="protected_area"'),
}

FEATURE_LABELS = {
    "waterfall": "Waterfall", "hiking": "Hiking Route", "mtb": "MTB / Cycling",
    "motorbiking": "Motorbiking Route", "peak": "Mountain Peak", "park": "National Park",
    "viewpoint": "Viewpoint", "camp": "Campsite", "cave": "Cave",
    "hot_spring": "Hot Spring", "waterway": "River / Lake", "beach": "Beach",
    "glacier": "Glacier", "volcano": "Volcano", "forest": "Protected Forest",
}

async def fetch_osm(
    lat: float,
    lng: float,
    radius_m: int,
    feature_ids: List[str],
    limit: int = 500,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Fetch features from OSM Overpass API.
    Yields one result dict at a time.
    """
    timeout = 60

    for fid in feature_ids:
        if fid not in FEATURE_TAGS:
            continue

        el_type, tag = FEATURE_TAGS[fid]
        label = FEATURE_LABELS.get(fid, fid)

        query = f"""[out:json][timeout:{timeout}];
(
  {el_type}[{tag}](around:{radius_m},{lat},{lng});
);
out tags center {limit};"""

        try:
            await rate_limiter.wait("overpass-api.de", 1.5)
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    OVERPASS_URL,
                    data={"data": query},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                resp.raise_for_status()
                data = resp.json()

            elements = data.get("elements", [])

            for el in elements:
                el_lat = el.get("lat") or (el.get("center") or {}).get("lat")
                el_lng = el.get("lon") or (el.get("center") or {}).get("lon")
                if not el_lat or not el_lng:
                    continue

                tags = el.get("tags", {})
                name = (
                    tags.get("name:en") or
                    tags.get("name") or
                    tags.get("int_name") or
                    ""
                )
                wiki_tag = tags.get("wikipedia", "")
                wiki_url = ""
                if wiki_tag:
                    parts = wiki_tag.split(":", 1)
                    page = parts[1] if len(parts) == 2 else parts[0]
                    lang = parts[0] if len(parts) == 2 else "en"
                    wiki_url = f"https://{lang}.wikipedia.org/wiki/{page.replace(' ', '_')}"

                yield {
                    "name": name,
                    "type": label,
                    "type_id": fid,
                    "lat": el_lat,
                    "lng": el_lng,
                    "elevation": tags.get("ele", ""),
                    "description": tags.get("description") or tags.get("description:en") or "",
                    "wikipedia": wiki_url,
                    "website": tags.get("website") or tags.get("url") or "",
                    "region": tags.get("addr:state") or tags.get("is_in:state") or "",
                    "country": tags.get("addr:country") or tags.get("is_in:country") or "",
                    "image": "",
                    "osm_id": f"{el.get('type','node')}/{el.get('id','')}",
                    "source": "OSM",
                    "confidence": "High" if name else "Low",
                }

        except Exception as e:
            # Log and continue — don't crash entire extraction
            print(f"[OSM] {fid} error: {e}")
            continue
