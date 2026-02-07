import os, json
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
TEXT_CONTAINER = os.environ.get("TEXT_CONTAINER","text")
CORPUS_CONTAINER = os.environ.get("CORPUS_CONTAINER","corpus")
OUT_BLOB = os.environ.get("OUT_BLOB","corpus_anon.jsonl")

def main():
    bsc = BlobServiceClient(
        account_url=f"https://{ACCOUNT}.blob.core.windows.net",
        credential=KEY,
        transport=RequestsTransport(connection_timeout=10, read_timeout=180),
    )
    textc = bsc.get_container_client(TEXT_CONTAINER)
    corpusc = bsc.get_container_client(CORPUS_CONTAINER)

    lines = []
    n = 0
    for b in textc.list_blobs():
        if not b.name.lower().endswith(".txt"):
            continue
        txt = textc.get_blob_client(b.name).download_blob().readall().decode("utf-8", errors="ignore").strip()
        if len(txt) < 50:
            continue

        rec = {
            "filename": b.name,   # ključno polje za kasnije učenje
            "text": txt
        }
        lines.append(json.dumps(rec, ensure_ascii=False))
        n += 1

    payload = ("\n".join(lines) + "\n").encode("utf-8")
    corpusc.get_blob_client(OUT_BLOB).upload_blob(payload, overwrite=True)
    print(f"Wrote {n} docs to {CORPUS_CONTAINER}/{OUT_BLOB} ({len(payload)} bytes)")

if __name__ == "__main__":
    main()
