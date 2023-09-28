from azure.storage.blob import BlobServiceClient

account_name = "rfcappstorage"
account_key = "si=obecontainer-policy&spr=https&sv=2021-12-02&sr=c&sig=Fm3IjNp%2BiQDfKSlgdoGyqhyLmpVtMF10xRtWB2ZRCWs%3D"

bsc = BlobServiceClient(
    account_url = f"https://{account_name}.blob.core.windows.net",
    credential=account_key
)

containers = bsc.list_containers()

for container in containers:
    print(container)
