import argparse
import base64
import json
import logging
import math
import os
import re
import struct
import sys
import time
import urllib.parse
import urllib.request
import requests

logger = logging.getLogger("property_pipeline")

def hex_hash_to_place_id(hex_hash: str) -> str:
    """
    Deterministically converts a Google Maps Feature Hex Hash (data_id)
    e.g. '0x80857e9d5171073b:0xb2acccd25f0eadde'
    into its official Google Place ID:
    e.g. 'ChIJOwdxUZ1-hYAR3q0OX9LMrLI'
    """
    try:
        if not hex_hash or ":" not in hex_hash:
            return None
        parts = hex_hash.strip().split(":")
        part1 = int(parts[0], 16)
        part2 = int(parts[1], 16)
        b1 = struct.pack("<Q", part1)
        b2 = struct.pack("<Q", part2)
        # Protobuf binary tag structure for Google Place ID
        proto_bytes = b"\x0a\x12\x09" + b1 + b"\x11" + b2
        return base64.urlsafe_b64encode(proto_bytes).decode("utf-8").rstrip("=")
    except Exception as e:
        logger.debug(f"Could not convert hex hash '{hex_hash}' to place_id: {e}")
        return None

def place_id_to_hex_hash(place_id: str) -> str:
    """
    Deterministically decodes a Google Place ID
    e.g. 'ChIJOwdxUZ1-hYAR3q0OX9LMrLI'
    into its Google Maps Feature Hex Hash (data_id):
    e.g. '0x80857e9d5171073b:0xb2acccd25f0eadde'
    """
    try:
        if not place_id or not place_id.startswith("ChIJ"):
            return None
        padding = "=" * ((4 - len(place_id) % 4) % 4)
        raw = base64.urlsafe_b64decode(place_id + padding)
        if raw.startswith(b"\x0a\x12\x09") and len(raw) >= 20:
            part1 = struct.unpack("<Q", raw[3:11])[0]
            part2 = struct.unpack("<Q", raw[12:20])[0]
            return f"0x{part1:016x}:0x{part2:016x}"
    except Exception as e:
        logger.debug(f"Could not decode place_id '{place_id}' to hex hash: {e}")
    return None

def hex_hash_to_cid(hex_hash: str) -> str:
    """
    Extracts the decimal Google Maps Customer ID (CID) from the hex hash.
    e.g. '0x80857e9d5171073b:0xb2acccd25f0eadde' -> '12874890638655401438'
    """
    try:
        if not hex_hash or ":" not in hex_hash:
            return None
        parts = hex_hash.strip().split(":")
        return str(int(parts[1], 16))
    except Exception:
        return None

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates great-circle distance between two points in meters."""
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (math.sin(delta_phi / 2.0) ** 2 +
         math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

class GoogleMapsPropertyScraper:
    def __init__(self, max_retries=3, timeout=15):
        self.max_retries = max_retries
        self.timeout = timeout
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://maps.google.com/",
        }

    def resolve_by_hex_hash(self, hex_hash: str):
        """
        Instantly resolves a Google Maps hex hash (e.g. 0x80857e9d5171073b:0xb2acccd25f0eadde)
        into its corresponding Place ID, CID, and Google Maps direct URLs.
        """
        pid = hex_hash_to_place_id(hex_hash)
        cid = hex_hash_to_cid(hex_hash)
        return {
            "hex_hash": hex_hash,
            "data_id": hex_hash,
            "place_id": pid,
            "cid": cid,
            "cid_url": f"https://maps.google.com/?cid={cid}" if cid else None,
            "place_url": f"https://www.google.com/maps/place/?q=place_id:{pid}" if pid else None
        }

    def _fetch_search_rpc(self, title, lat=0.0, lan=0.0, zoom=16.0):
        """
        Executes Google Maps structured RPC search endpoint.
        Returns the parsed top-level JSON data array.
        """
        lat_val = float(lat) if lat is not None else 0.0
        lan_val = float(lan) if lan is not None else 0.0
        w, h = 600, 800

        # Protocol buffer parameter specifying location viewport and search modes
        pb = (
            f"!4m12!1m3!1d3826.902183192154!2d{lan_val:.4f}!3d{lat_val:.4f}!2m3!1f0!2f0!3f0!3m2!1i{w}!2i{h}!4f{zoom:.1f}!7i20!8i0"
            "!10b1!12m22!1m3!18b1!30b1!34e1!2m3!5m1!6e2!20e3!4b0!10b1!12b1!13b1!16b1!17m1!3e1!20m3!5e2!6b1!14b1!46m1!1b0"
            "!96b1!19m4!2m3!1i360!2i120!4i8"
        )
        params = {
            "tbm": "map",
            "authuser": "0",
            "hl": "en",
            "q": title.strip(),
            "pb": pb
        }
        url = "https://maps.google.com/search?" + urllib.parse.urlencode(params)

        for attempt in range(1, self.max_retries + 1):
            try:
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    raw_bytes = resp.read()
                    nl_idx = raw_bytes.find(b"\n")
                    if nl_idx != -1:
                        raw_bytes = raw_bytes[nl_idx + 1:]
                    return json.loads(raw_bytes)
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{self.max_retries} failed for '{title}': {e}")
                if attempt == self.max_retries:
                    return None
                time.sleep(attempt * 1.5)
        return None

    def search_property(self, title, lat=None, lan=None, max_reviews=10):
        """
        Searches Google Maps for a property by business name and optional coordinates.
        Returns a dict containing:
          - hex_hash (e.g. '0x80857e9d5171073b:0xb2acccd25f0eadde')
          - place_id (e.g. 'ChIJOwdxUZ1-hYAR3q0OX9LMrLI')
          - data_id (alias for hex_hash)
          - cid (e.g. '12874890638655401438')
          - title (canonical name)
          - address (full formatted address)
          - latitude
          - longitude
          - rating (Google star rating, float)
          - review_count (total reviews count, int)
          - category
          - reviews (list of dicts)
        """
        logger.info(f"Querying Google Maps: '{title}' (Coordinates: {lat}, {lan})")
        data = self._fetch_search_rpc(title, lat=lat, lan=lan)
        if not data or not isinstance(data, list) or len(data) == 0:
            logger.warning(f"Empty search response for '{title}'")
            return None

        container = data[0] if isinstance(data, list) and len(data) > 0 else None
        if not isinstance(container, list) or len(container) < 2:
            logger.warning(f"Unrecognized business list structure for '{title}'")
            return None

        items = container[1]
        if not isinstance(items, list) or len(items) == 0:
            logger.warning(f"No business entities found in search for '{title}'")
            return None

        candidates = []
        for it in items:
            if not isinstance(it, list) or len(it) < 15 or not isinstance(it[14], list):
                continue
            biz = it[14]

            data_id = biz[10] if len(biz) > 10 else None
            place_id = biz[78] if len(biz) > 78 else None
            title_res = biz[11] if len(biz) > 11 else title

            # Bidirectional reconciliation: Guarantee BOTH hex_hash and place_id exist
            if not place_id and data_id:
                place_id = hex_hash_to_place_id(data_id)
            if not data_id and place_id:
                data_id = place_id_to_hex_hash(place_id)

            # Customer ID (CID) derived from Hex data_id or direct field
            cid = None
            if data_id and ":" in data_id:
                try:
                    cid = hex_hash_to_cid(data_id)
                except Exception:
                    pass

            # Rating & Review Count
            rating = None
            review_count = 0
            reviews_link = None
            if len(biz) > 4 and isinstance(biz[4], list):
                rating = biz[4][7] if len(biz[4]) > 7 and isinstance(biz[4][7], (int, float)) else None
                review_count = biz[4][8] if len(biz[4]) > 8 and isinstance(biz[4][8], (int, float)) else 0
                if len(biz[4]) > 3 and isinstance(biz[4][3], list) and len(biz[4][3]) > 0:
                    reviews_link = biz[4][3][0]

            # Address
            addr_parts = biz[2] if len(biz) > 2 and isinstance(biz[2], list) else []
            address = ", ".join([str(p).strip() for p in addr_parts if p and str(p).strip()])
            if not address and len(biz) > 18 and isinstance(biz[18], str):
                address = biz[18]

            # Coordinates
            cand_lat = biz[9][2] if len(biz) > 9 and isinstance(biz[9], list) and len(biz[9]) > 2 else lat
            cand_lan = biz[9][3] if len(biz) > 9 and isinstance(biz[9], list) and len(biz[9]) > 3 else lan

            # Primary Category
            category = None
            if len(biz) > 13 and isinstance(biz[13], list) and len(biz[13]) > 0:
                category = str(biz[13][0])

            # Description
            description = None
            if len(biz) > 154 and isinstance(biz[154], list) and len(biz[154]) > 0:
                if isinstance(biz[154][0], list) and len(biz[154][0]) > 0:
                    description = biz[154][0][0]

            # Calculate proximity distance if lat/lan provided
            dist = float("inf")
            if lat is not None and lan is not None and cand_lat is not None and cand_lan is not None:
                try:
                    dist = haversine_distance(float(lat), float(lan), float(cand_lat), float(cand_lan))
                except Exception:
                    dist = float("inf")

            candidates.append({
                "hex_hash": data_id,
                "place_id": place_id,
                "data_id": data_id,
                "cid": cid,
                "title": title_res,
                "category": category,
                "address": address,
                "latitude": cand_lat,
                "longitude": cand_lan,
                "rating": rating,
                "review_count": review_count,
                "reviews_link": reviews_link,
                "description": description,
                "distance_meters": dist,
                "reviews": []
            })

        if not candidates:
            logger.warning(f"Could not parse valid business candidates for '{title}'")
            return None

        # Sort by distance if coordinates provided, otherwise keep primary match
        if lat is not None and lan is not None and any(c["distance_meters"] != float("inf") for c in candidates):
            candidates.sort(key=lambda c: c["distance_meters"])

        best_match = candidates[0]
        logger.info(
            f"  ✓ Matched: '{best_match['title']}' | HexHash: {best_match['hex_hash']} | "
            f"PlaceID: {best_match['place_id']} | CID: {best_match['cid']}"
        )
        return best_match

def main():
    parser = argparse.ArgumentParser(description="Google Maps Scraper & Hex Hash / Place ID Resolver")
    parser.add_argument("identifier", nargs="?", help="Hex Hash (e.g. 0x80857e9d5171073b:0xb2acccd25f0eadde), Place ID, or Search Query")
    parser.add_argument("--query", "-q", help="Search query string")
    parser.add_argument("--lat", type=float, help="Latitude")
    parser.add_argument("--lon", type=float, help="Longitude")

    args = parser.parse_args()
    target = args.query or args.identifier

    if not target:
        parser.print_help()
        sys.exit(1)

    scraper = GoogleMapsPropertyScraper()

    # Check if target is a hex hash
    if ":" in target and target.startswith("0x"):
        res = scraper.resolve_by_hex_hash(target)
        print("\n" + "=" * 50)
        print("GOOGLE MAPS IDENTIFIER RESOLUTION")
        print("=" * 50)
        print(f"Hex Hash : {res['hex_hash']}")
        print(f"Place ID : {res['place_id']}")
        print(f"CID      : {res['cid']}")
        print(f"CID URL  : {res['cid_url']}")
        print(f"Maps URL : {res['place_url']}")
        print("=" * 50 + "\n")
        return

    # Check if target is a Place ID
    if target.startswith("ChIJ"):
        hex_hash = place_id_to_hex_hash(target)
        cid = hex_hash_to_cid(hex_hash) if hex_hash else None
        print("\n" + "=" * 50)
        print("GOOGLE MAPS IDENTIFIER RESOLUTION")
        print("=" * 50)
        print(f"Place ID : {target}")
        print(f"Hex Hash : {hex_hash}")
        print(f"CID      : {cid}")
        print(f"CID URL  : https://maps.google.com/?cid={cid}" if cid else "CID URL  : N/A")
        print("=" * 50 + "\n")
        return

    # Otherwise perform live Google Maps search
    result = scraper.search_property(target, lat=args.lat, lan=args.lon)
    if result:
        print("\n" + json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"No results found for query: '{target}'")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
