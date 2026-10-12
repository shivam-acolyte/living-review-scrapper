import requests
import json
import re

url = "https://www.google.com/maps/place/Crystal+Beach+Community+Park/@45.351875,-75.83942,16z?hl=en"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

r = requests.get(url, headers=headers)
html = r.text

def extract_place_from_html(html):
    # Find all )]}' strings or search in APP_INITIALIZATION_STATE
    matches = re.findall(r"\)\]\}'\s*\n(\[.+?\])\s*<\/", html, re.DOTALL)
    if not matches:
        # Search anywhere in script tags
        matches = re.findall(r"\)\]\}'\s*\n(\[.+?\])", html)
    
    print(f"Found {len(matches)} JSON blobs starting with )]}}'")
    for m in matches:
        try:
            jd = json.loads(m)
            if isinstance(jd, list) and len(jd) > 6 and isinstance(jd[6], list):
                darray = jd[6]
                title = darray[11] if len(darray) > 11 else None
                place_id = darray[78] if len(darray) > 78 else None
                data_id = darray[10] if len(darray) > 10 else None
                rating = darray[4][7] if len(darray) > 4 and darray[4] else None
                rev_count = darray[4][8] if len(darray) > 4 and darray[4] else None
                
                cid = None
                try:
                    cid = jd[25][3][0][13][0][0][1]
                except Exception:
                    pass
                
                revs = []
                try:
                    raw_revs = darray[175][9][0][0]
                    for rv in raw_revs:
                        r_info = rv[0]
                        rev_id = r_info[0] if len(r_info) > 0 else None
                        author = r_info[1][4][0][4] if len(r_info) > 1 and len(r_info[1]) > 4 else "Anonymous"
                        star = r_info[2][0][0] if len(r_info) > 2 and r_info[2] else None
                        when = r_info[1][6] if len(r_info) > 1 and len(r_info[1]) > 6 else ""
                        text = r_info[2][15][0][0] if len(r_info) > 2 and len(r_info[2]) > 15 and r_info[2][15] and r_info[2][15][0] else ""
                        revs.append({"id": rev_id, "author": author, "rating": star, "when": when, "text": text})
                except Exception:
                    pass
                
                return {
                    "title": title,
                    "place_id": place_id,
                    "data_id": data_id,
                    "cid": cid,
                    "rating": rating,
                    "review_count": rev_count,
                    "reviews": revs
                }
        except Exception:
            continue
    return None

res = extract_place_from_html(html)
print("Extracted result:")
print(json.dumps(res, indent=2))
