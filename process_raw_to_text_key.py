import os, io, zipfile
import tempfile
import subprocess
import re
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport
from pdfminer.high_level import extract_text as pdf_extract_text
from lxml import etree
from docx import Document

def anonymize_sr(text: str) -> str:
    """Basic Serbian PII scrubber (best-effort). NOT perfect; safety net."""
    if not text:
        return text
    t = text.replace("\u00a0", " ")

    import re as _re

    t = _re.sub(r'(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b', '[EMAIL]', t)
    t = _re.sub(r'(?i)\bhttps?://\S+\b', '[URL]', t)
    t = _re.sub(r'(?i)\bwww\.\S+\b', '[URL]', t)

    t = _re.sub(r'\b\d{13}\b', '[JMBG]', t)
    t = _re.sub(r'(?x)(?<!\d)(?:\+?381|0)\s*(?:\(?\d{2,3}\)?[\s/-]*)\d{3}[\s/-]*\d{3,4}(?!\d)', '[PHONE]', t)
    t = _re.sub(r'(?i)\bRS\s*\d(?:\s*\d){19}\b', '[IBAN]', t)
    t = _re.sub(r'(?<!\d)(?:\d[ -]?){13,19}(?!\d)', '[CARD_OR_LONG_NUMBER]', t)
    t = _re.sub(r'\b[A-ZČĆŠĐŽ]{1,2}\s*\d{3,4}\s*[- ]?\s*[A-ZČĆŠĐŽ]{1,2}\b', '[PLATE]', t)
    t = _re.sub(r'(?i)\b(ličn(?:a|e)\s+karta|lk|pasoš|putna\s+isprava|broj\s+dokumenta)\s*[:#]?\s*\w+\b', r'\1: [DOC_ID]', t)
    t = _re.sub(r'(?i)\b(ul\.?|ulica|bulevar|булевар|bb)\s+[A-Za-zА-Яа-яČĆŠĐŽčćšđž0-9 .-]{2,}\b', '[ADDRESS]', t)

    role_pat = _re.compile(r'(?i)\b(tužilac|okrivljeni|okrivljena|optuženi|optužena|tuženi|tužena|tužilja|branilac|punomoćnik|puno(?:m|ć)nik|oštećeni|oštećena|svedok|svjedok|sudija|predsednik\s+veća|predsjednik\s+vijeća)\b\s*[:\-]\s*([A-ZČĆŠĐŽ][a-zčćšđž]+(?:\s+[A-ZČĆŠĐŽ][a-zčćšđž]+){1,2})')
    t = role_pat.sub(lambda m: f"{m.group(1)}: [NAME]", t)

    t = _re.sub(r'\b([A-ZА-ЯČĆŠĐŽ]\.)\s*([A-ZА-ЯČĆŠĐŽ]\.)\b', '[INITIALS]', t)

    t = _re.sub(r'[ \t]+', ' ', t)
    t = _re.sub(r'\n{3,}', '\n\n', t)
    return t.strip()

ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
RAW_CONTAINER = os.environ.get("RAW_CONTAINER","raw")
TEXT_CONTAINER = os.environ.get("TEXT_CONTAINER","text")

def detect_format(data: bytes) -> str:
    if data.startswith(b"%PDF"):
        return "pdf"
    if data.startswith(b"PK\x03\x04"):
        z = zipfile.ZipFile(io.BytesIO(data))
        names = set(z.namelist())
        if "content.xml" in names:
            return "odf"
        if "word/document.xml" in names:
            return "docx"
        return "zip"
    return "bin"


def detect_format_with_name(name: str, data: bytes) -> str:
    n = (name or "").lower()
    if n.endswith(".doc") and not n.endswith(".docx"):
        return "doc"
    return detect_format(data)


def extract_odf_text(data: bytes) -> str:
    z = zipfile.ZipFile(io.BytesIO(data))
    xml = z.read("content.xml")
    root = etree.fromstring(xml)
    text = " ".join(root.xpath("//text()"))
    return " ".join(text.split())

def extract_docx_text(data: bytes) -> str:
    with io.BytesIO(data) as f:
        doc = Document(f)
        parts = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    return "\n".join(parts)

def extract_pdf_text(data: bytes) -> str:
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
        tmp.write(data); tmp.flush()
        txt = pdf_extract_text(tmp.name) or ""
    return txt.strip()


def extract_doc_text(data: bytes) -> str:
    # Uses antiword (installed via apt)
    with tempfile.NamedTemporaryFile(suffix=".doc", delete=True) as tmp:
        tmp.write(data); tmp.flush()
        try:
            out = subprocess.check_output(["antiword", tmp.name], stderr=subprocess.STDOUT)
            return out.decode("utf-8", errors="ignore").strip()
        except subprocess.CalledProcessError as e:
            msg = e.output.decode("utf-8", errors="ignore")
            raise RuntimeError(f"antiword failed: {msg[:300]}")

def main():
    transport = RequestsTransport(connection_timeout=10, read_timeout=180)
    url = f"https://{ACCOUNT}.blob.core.windows.net"
    bsc = BlobServiceClient(account_url=url, credential=KEY, transport=transport)

    raw = bsc.get_container_client(RAW_CONTAINER)
    textc = bsc.get_container_client(TEXT_CONTAINER)

    count = 0
    for b in raw.list_blobs():
        count += 1
        name = b.name
        data = raw.get_blob_client(name).download_blob().readall()
        fmt = detect_format_with_name(name, data)
        base = os.path.splitext(os.path.basename(name))[0]
        out_name = f"{base}.txt"

        out_bc = textc.get_blob_client(out_name)
        if out_bc.exists():
            print("SKIP:", out_name)
            continue

        try:
            if fmt == "pdf":
                txt = extract_pdf_text(data)
            elif fmt == "odf":
                txt = extract_odf_text(data)
            elif fmt == "docx":
                txt = extract_docx_text(data)
            elif fmt == "doc":
                txt = extract_doc_text(data)
                txt = anonymize_sr(txt)
            else:
                print("SKIP unsupported:", name, fmt)
                continue

            if not txt or len(txt.strip()) < 50:
                print("WARN short:", name, fmt)
                continue

            out_bc.upload_blob(
                txt.encode("utf-8"),
                overwrite=False,
                metadata={"source_blob": name, "file_type": fmt}
            )
            print("WROTE:", out_name, "from:", name, "type:", fmt, "chars:", len(txt))

        except Exception as e:
            print("ERROR processing:", name, fmt, e)

    print("Done. Processed blobs:", count)

if __name__ == "__main__":
    main()
