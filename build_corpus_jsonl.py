import os, json, hashlib
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
TEXT_CONTAINER = os.environ.get("TEXT_CONTAINER","text")
CORPUS_CONTAINER = os.environ.get("CORPUS_CONTAINER","corpus")
OUT_BLOB = os.environ.get("CORPUS_BLOB_NAME","corpus.jsonl")

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def main():
    transport = RequestsTransport(connection_timeout=10, read_timeout=180)
    url = f"https://{ACCOUNT}.blob.core.windows.net"
    bsc = BlobServiceClient(account_url=url, credential=KEY, transport=transport)

    textc = bsc.get_container_client(TEXT_CONTAINER)
    corpusc = bsc.get_container_client(CORPUS_CONTAINER)

    lines = []
    count = 0
    for b in textc.list_blobs():
        if not b.name.lower().endswith(".txt"):
            continue
        data = textc.get_blob_client(b.name).download_blob().readall().decode("utf-8", errors="ignore")
        txt = data.strip()
        if len(txt) < 50:
            continue

        rec = {
            "doc_id": sha256(b.name + "|" + str(len(txt))),
            "source_text_blob": b.name,
            "char_len": len(txt),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "text": txt,
        }
        lines.append(json.dumps(rec, ensure_ascii=False))
        count += 1

    blob = corpusc.get_blob_client(OUT_BLOB)
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    blob.upload_blob(payload, overwrite=True)
    print(f"Wrote {count} docs to {CORPUS_CONTAINER}/{OUT_BLOB} ({len(payload)} bytes)")

if __name__ == "__main__":
    main()
