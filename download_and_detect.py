import os, io, time, hashlib, zipfile
import requests
from azure.storage.blob import BlobServiceClient

AZ_CONN = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
RAW_CONTAINER = os.environ.get("RAW_CONTAINER", "raw")
RATE_SECONDS = float(os.environ.get("RATE_SECONDS", "2"))

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def detect_format(data: bytes, content_type: str | None) -> str:
    ct = (content_type or "").lower()

    if data.startswith(b"%PDF"):
        return "pdf"
    if data.startswith(b"PK\x03\x04"):
        z = zipfile.ZipFile(io.BytesIO(data))
        names = set(z.namelist())
        if "content.xml" in names:
            return "odf"   # ODP/ODT/...
        if "word/document.xml" in names:
            return "docx"
        return "zip"

    if "pdf" in ct:
        return "pdf"
    if "opendocument" in ct or "oasis.opendocument" in ct:
        return "odf"
    if "wordprocessingml" in ct:
        return "docx"
    return "bin"

def download(url: str) -> tuple[bytes, str | None]:
    r = requests.get(
        url,
        timeout=60,
        headers={"User-Agent":"corpus-agent/1.0"},
        allow_redirects=True,
    )
    r.raise_for_status()
    return r.content, r.headers.get("Content-Type")

def main():
    if not AZ_CONN:
        raise SystemExit("Set AZURE_STORAGE_CONNECTION_STRING env var first.")

    bsc = BlobServiceClient.from_connection_string(AZ_CONN)
    container = bsc.get_container_client(RAW_CONTAINER)

    with open("urls.txt", "r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]

    for u in urls:
        try:
            data, ct = download(u)
            fmt = detect_format(data, ct)
            h = sha256_bytes(data)
            name = f"{h}.{fmt}"

            bc = container.get_blob_client(name)
            if bc.exists():
                print("SKIP (cached):", name)
            else:
                bc.upload_blob(data, overwrite=False, metadata={"source_url": u, "content_type": (ct or "")[:200]})
                print("UPLOADED:", name, "type:", fmt, "ct:", ct)
            time.sleep(RATE_SECONDS)
        except Exception as e:
            print("ERROR:", u, e)
            time.sleep(RATE_SECONDS * 2)

if __name__ == "__main__":
    main()
