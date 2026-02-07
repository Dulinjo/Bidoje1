import os, io, zipfile, hashlib
from azure.storage.blob import BlobServiceClient
from pdfminer.high_level import extract_text as pdf_extract_text
from lxml import etree
from docx import Document

AZ_CONN = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
RAW_CONTAINER = os.environ.get("RAW_CONTAINER", "raw")
TEXT_CONTAINER = os.environ.get("TEXT_CONTAINER", "text")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def detect_format(data: bytes) -> str:
    if data.startswith(b"%PDF"):
        return "pdf"
    if data.startswith(b"PK\x03\x04"):
        z = zipfile.ZipFile(io.BytesIO(data))
        names = set(z.namelist())
        if "content.xml" in names:
            return "odf"   # odp/odt/...
        if "word/document.xml" in names:
            return "docx"
        return "zip"
    return "bin"

def extract_odf_text(data: bytes) -> str:
    z = zipfile.ZipFile(io.BytesIO(data))
    xml = z.read("content.xml")
    root = etree.fromstring(xml)
    # узми све текст чворове
    text = " ".join(root.xpath("//text()"))
    # мало чишћења
    text = " ".join(text.split())
    return text

def extract_docx_text(data: bytes) -> str:
    with io.BytesIO(data) as f:
        doc = Document(f)
        parts = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    return "\n".join(parts)

def extract_pdf_text(data: bytes) -> str:
    # pdfminer ради са путањом или file-like; најлакше је temp фајл
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
        tmp.write(data)
        tmp.flush()
        txt = pdf_extract_text(tmp.name) or ""
    return txt.strip()

def main():
    bsc = BlobServiceClient.from_connection_string(AZ_CONN)
    raw = bsc.get_container_client(RAW_CONTAINER)
    textc = bsc.get_container_client(TEXT_CONTAINER)

    blobs = list(raw.list_blobs())
    if not blobs:
        print("No blobs found in container:", RAW_CONTAINER)
        return

    for b in blobs:
        name = b.name
        bc = raw.get_blob_client(name)
        data = bc.download_blob().readall()

        fmt = detect_format(data)
        base = os.path.splitext(os.path.basename(name))[0]
        out_name = f"{base}.txt"

        out_bc = textc.get_blob_client(out_name)
        if out_bc.exists():
            print("SKIP text exists:", out_name)
            continue

        try:
            if fmt == "pdf":
                txt = extract_pdf_text(data)
            elif fmt == "odf":
                txt = extract_odf_text(data)
            elif fmt == "docx":
                txt = extract_docx_text(data)
            else:
                print("SKIP unsupported:", name, "type:", fmt)
                continue

            if not txt or len(txt.strip()) < 50:
                print("WARN empty/short text:", name, "type:", fmt)
                continue

            out_bc.upload_blob(
                txt.encode("utf-8"),
                overwrite=False,
                metadata={"source_blob": name, "file_type": fmt}
            )
            print("WROTE:", out_name, "from:", name, "type:", fmt, "chars:", len(txt))

        except Exception as e:
            print("ERROR processing:", name, "type:", fmt, "err:", e)

if __name__ == "__main__":
    main()
