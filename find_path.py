import requests
import json
import re

url = "https://www.google.com/maps/place/Crystal+Beach+Community+Park/@45.351875,-75.83942,16z?hl=en"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
resp = requests.get(url, headers=headers)

m = re.search(r"window\.APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);\s*window\.", resp.text)
if m:
    data = json.loads(m.group(1))
    target = data[3] if len(data) > 3 else None
    print("target type:", type(target))
    
    # If target is string, parse it
    if isinstance(target, str):
        lines = target.split("\n", 2)
        target = json.loads(lines[2])
    
    # Recursively find any list containing Place ID "ChIJ"
    def find_place_id(obj, path=""):
        if isinstance(obj, str) and obj.startswith("ChIJ"):
            print(f"Found Place ID: {obj} at path {path}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                find_place_id(item, f"{path}[{i}]")
        elif isinstance(obj, dict):
            for k, v in obj.items():
                find_place_id(v, f"{path}['{k}']")
    
    find_place_id(target)
