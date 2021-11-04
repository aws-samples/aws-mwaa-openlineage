import requests

url = f"http://ec2-107-21-174-143.compute-1.amazonaws.com:5000/api/v1"
# resp = requests.get(url)

# print(resp.status_code)
# get namespaces
# curl http://ec2-107-21-174-143.compute-1.amazonaws.com:5000/api/v1/namespaces
# create a source
# curl http://ec2-107-21-174-143.compute-1.amazonaws.com:5000/api/v1/sources
# get namespaces

resp = requests.get(f"{url}/namespaces")
print(resp.status_code)
print(resp.reason)
print(resp.content)

namespace = "cdkdl-dev"
dataset = "mydataset1"

payload = {
    "type": "DB_TABLE",
    "physicalName": "public.mytable",
    "sourceName": "my-source",
    "fields": [
        {"name": "a", "type": "INTEGER"},
        {"name": "b", "type": "TIMESTAMP"},
        {"name": "c", "type": "INTEGER"},
        {"name": "d", "type": "INTEGER"},
    ],
    "description": "My first dataset!",
}

resp = requests.put(
    f"{url}/namespaces/{namespace}/datasets/{dataset}",
    json=payload,
    headers={"Content-Type": "application/json"},
    timeout=5,
)

print(resp.status_code)
print(resp.reason)
print(resp.content)
