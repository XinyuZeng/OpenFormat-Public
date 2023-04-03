import json

# read json
with open('one-line-json.json', 'r') as f:
    s = json.loads(f.read())
    print(s)