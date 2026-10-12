import urllib.request
import urllib.parse
import secrets
import json

data_id = "0x4cce0401cd5e4905:0x9cd9893d258b6bd8"
request_id = secrets.token_urlsafe(16)
page_size = 10

pb = f"!1m6!1s{urllib.parse.quote(data_id)}!6m4!4m1!1e1!4m1!1e3!2m2!1i{page_size}!2s!5m2!1s{request_id}!7e81!8m9!2b1!3b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!11m0!13m1!1e1"
url = f"https://www.google.com/maps/rpc/listugcposts?authuser=0&hl=en&pb={pb}"

req = urllib.request.Request(url, headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"
})
try:
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        if body.startswith(")]}'\n"):
            body = body[5:]
        elif body.startswith(")]}\x27\n"):
            body = body[5:]
        data = json.loads(body)
        reviews_list = data[2] if len(data) > 2 else []
        print("Successfully fetched", len(reviews_list), "reviews via RPC!")
        for r in reviews_list[:5]:
            r_info = r[0]
            rev_id = r_info[0] if len(r_info) > 0 else None
            author = r_info[1][4][0][4] if len(r_info) > 1 and len(r_info[1]) > 4 else None
            rating = r_info[2][0][0] if len(r_info) > 2 and r_info[2] else None
            text = r_info[2][15][0][0] if len(r_info) > 2 and len(r_info[2]) > 15 and r_info[2][15] and r_info[2][15][0] else ""
            when = r_info[1][6] if len(r_info) > 1 and len(r_info[1]) > 6 else ""
            print(f"  * [{rating}★] {author} ({when}): {text[:60]}")
except Exception as e:
    print("RPC Error:", e)
