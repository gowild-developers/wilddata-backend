from typing import List, Dict, Any
import math

def haversine_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def deduplicate(results: List[Dict[str, Any]], radius_km: float = 0.05) -> List[Dict[str, Any]]:
    """
    Merge duplicate entries within radius_km of each other with same type.
    Keeps entry with most data, merges fields from others.
    """
    merged = []

    for r in results:
        found = False
        for m in merged:
            # Same type and within radius
            if m.get('type_id') == r.get('type_id'):
                dist = haversine_km(m['lat'], m['lng'], r['lat'], r['lng'])
                if dist <= radius_km:
                    # Merge — prefer non-empty values
                    if not m.get('name') and r.get('name'):
                        m['name'] = r['name']
                    if not m.get('description') and r.get('description'):
                        m['description'] = r['description']
                    if not m.get('wikipedia') and r.get('wikipedia'):
                        m['wikipedia'] = r['wikipedia']
                    if not m.get('elevation') and r.get('elevation'):
                        m['elevation'] = r['elevation']
                    if not m.get('region') and r.get('region'):
                        m['region'] = r['region']
                    if not m.get('country') and r.get('country'):
                        m['country'] = r['country']
                    if not m.get('image') and r.get('image'):
                        m['image'] = r['image']
                    if not m.get('website') and r.get('website'):
                        m['website'] = r['website']
                    # Upgrade confidence
                    conf_rank = {'High': 3, 'Medium': 2, 'Low': 1}
                    if conf_rank.get(r.get('confidence','Low'), 1) > conf_rank.get(m.get('confidence','Low'), 1):
                        m['confidence'] = r['confidence']
                    # Merge source labels
                    existing_sources = m.get('source', '').split('+')
                    new_source = r.get('source', '')
                    if new_source and new_source not in existing_sources:
                        m['source'] = m.get('source', '') + '+' + new_source
                    found = True
                    break
        if not found:
            merged.append(dict(r))  # copy

    return merged
