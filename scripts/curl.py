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

payload = {
    "type": "S3",
    "connectionUrl": "s3://cdkdl-dev-foundationstoragef3-s3bucketraw88473b86-rba84mz2wapn",
    "description": "S3 RAW",
}

resp = requests.post(
    f"{url}/lineage",
    data=payload,
    headers={"Content-Type": "application/json"},
    timeout=5,
)

print(resp.status_code)
print(resp.reason)
