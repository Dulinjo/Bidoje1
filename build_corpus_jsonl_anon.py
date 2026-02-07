import os, json, hashlib
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
TEXT_CONTAINER = os.environ.get("TEXT_CONTAINER","text")
CORPUS_CONTAINER = os.environ.get("CORPUS_CONTAINER","corpus")
OUT_BLOB = os.environ.get("CORPUS_BLOB_NAME","corpus_anon.jsonl")

def doc_id(name: str, txt: str) -> str:
    return hashlib.sha256((name + "|" + str(len(txt))).encode("utf-8")).hexdigest()

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
        if not b.name.endswith(".anon.txt"):
            continue
        txt = textc.get_blob_client(b.name).download_blob().readall().decode("utf-8", errors="ignore").strip()
        if len(txt) < 50:
            continue
        rec = {
            "doc_id": doc_id(b.name, txt),
            "source_text_blob": b.name,
            "char_len": len(txt),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "text": txt,
        }
        lines.append(json.dumps(rec, ensure_ascii=False))
        n += 1

    payload = ("\n".join(lines) + "\n").encode("utf-8")
    corpusc.get_blob_client(OUT_BLOB).upload_blob(payload, overwrite=True)
    print(f"Wrote {n} docs to {CORPUS_CONTAINER}/{OUT_BLOB} ({len(payload)} bytes)")

if __name__ == "__main__":
    main()
