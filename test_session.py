import requests
import json
import re
import secrets
import urllib.parse

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})

# 1. First visit place page to get session cookies and inline reviews
place_url = "https://www.google.com/maps/place/Crystal+Beach+Community+Park/@45.351875,-75.83942,16z?hl=en"
resp = session.get(place_url)
print("Place page status:", resp.status_code)
print("Cookies received:", list(session.cookies.keys()))

# 2. Extract inline reviews from page
m = re.search(r"window\.APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);\s*window\.", resp.text)
inline_reviews = []
data_id = None
if m:
    data = json.loads(m.group(1))
    raw3 = data[3] if len(data) > 3 else None
    if isinstance(raw3, list):
        inner = raw3
    elif isinstance(raw3, str):
        lines = raw3.split("\n", 2)
        inner = json.loads("\n".join(lines[2:]))
    else:
        inner = []
    
    # jd is inner[2]
    jd = inner[2] if len(inner) > 2 else []
    darray = jd[0][1] if jd and len(jd[0]) > 1 else []
    data_id = darray[10] if len(darray) > 10 else None
    print("Found data_id:", data_id)
    
    try:
        revs = darray[175][9][0][0]
        for r in revs:
            r_info = r[0]
            rev_id = r_info[0] if len(r_info) > 0 else None
            author = r_info[1][4][0][4] if len(r_info) > 1 and len(r_info[1]) > 4 else "Anonymous"
            rating = r_info[2][0][0] if len(r_info) > 2 and r_info[2] else None
            when = r_info[1][6] if len(r_info) > 1 and len(r_info[1]) > 6 else ""
            text = r_info[2][15][0][0] if len(r_info) > 2 and len(r_info[2]) > 15 and r_info[2][15] and r_info[2][15][0] else ""
            inline_reviews.append({
                "review_id": rev_id,
                "author": author,
                "rating": rating,
                "when": when,
                "text": text
            })
    except Exception as e:
        print("Inline parse error:", e)

print(f"Extracted {len(inline_reviews)} inline reviews!")
for r in inline_reviews[:3]:
    print("  *", r)

# 3. Now test RPC using the session cookies
if data_id:
    request_id = secrets.token_urlsafe(16)
    page_size = 10
    pb = f"!1m6!1s{urllib.parse.quote(data_id)}!6m4!4m1!1e1!4m1!1e3!2m2!1i{page_size}!2s!5m2!1s{request_id}!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e1"
    rpc_url = f"https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&pb={pb}"
    session.headers.update({"Referer": place_url})
    rpc_resp = session.get(rpc_url)
    print("RPC status with cookies:", rpc_resp.status_code)
    if rpc_resp.status_code == 200:
        body = rpc_resp.text
        if body.startswith(")]}'\n") or body.startswith(")]}\x27\n"):
            body = body[5:]
        rpc_data = json.loads(body)
        rpc_revs = rpc_data[2] if len(rpc_data) > 2 else []
        print(f"RPC returned {len(rpc_revs)} reviews!")
