import urllib.request
import re
import json

url = "https://www.google.com/maps/place/Crystal+Beach,+Ottawa,+ON,+Canada/@45.351875,-75.83942,15z?hl=en"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
req = urllib.request.Request(url, headers=headers)
with urllib.request.urlopen(req) as resp:
    content = resp.read().decode("utf-8", errors="ignore")

m = re.search(r"window\.APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);\s*window\.", content)
if m:
    data = json.loads(m.group(1))
    raw3 = data[3] if len(data) > 3 else None
    if isinstance(raw3, list):
        inner = raw3
        print("data[3] is already parsed as a list")
    elif isinstance(raw3, str):
        lines = raw3.split("\n", 2)
        inner = None
        for l_idx in range(len(lines)):
            try:
                cand = "\n".join(lines[l_idx:])
                inner = json.loads(cand)
                print("Successfully parsed inner JSON starting from line", l_idx)
                break
            except Exception:
                pass
    else:
        inner = []

    # Let us extract place metadata using exact gmaps parser logic!
    # In gmaps/entry.go:
    # jd is inner[2]
    # darray is jd[0][1]
    jd = inner[2]
    darray = jd[0][1]
    
    # Title
    try:
        title = darray[11]
    except Exception:
        title = None
    # Category
    try:
        category = darray[13]
    except Exception:
        category = None
    # Address
    try:
        address = darray[18]
    except Exception:
        address = None
    # CID
    # entry.Cid = getNthElementAndCast[string](jd, 25, 3, 0, 13, 0, 0, 1)
    def get_path(obj, *indices):
        curr = obj
        for idx in indices:
            if curr is None:
                return None
            if isinstance(curr, list) and 0 <= idx < len(curr):
                curr = curr[idx]
            else:
                return None
        return curr

    cid = get_path(jd, 25, 3, 0, 13, 0, 0, 1)
    data_id = get_path(darray, 10)
    place_id = get_path(darray, 78)
    rating = get_path(darray, 4, 7)
    review_count = get_path(darray, 4, 8)
    reviews_link = get_path(darray, 4, 3, 0)
    lat = get_path(darray, 9, 2)
    lng = get_path(darray, 9, 3)

    print("Title:", title)
    print("Category:", category)
    print("Address:", address)
    print("Place ID:", place_id)
    print("Data ID (Hex):", data_id)
    print("CID:", cid)
    print("Rating:", rating)
    print("Review Count:", review_count)
    print("Latitude:", lat)
    print("Longitude:", lng)
    print("Reviews Link:", reviews_link)

    # Check reviews in darray: darray[175][9][0][0]
    revs = get_path(darray, 175, 9, 0, 0)
    print("Inline reviews count:", len(revs) if revs else 0)
    if revs:
        for r in revs[:3]:
            # parse review
            author = get_path(r, 0, 1, 4, 0, 4)
            rating_r = get_path(r, 0, 2, 0, 0)
            text = get_path(r, 0, 2, 15, 0, 0)
            when = get_path(r, 0, 1, 6)
            print(f"  - Author: {author}, Rating: {rating_r}, When: {when}")
            print(f"    Text: {text}")
