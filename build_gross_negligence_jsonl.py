import os, re, json, hashlib
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
TEXT_CONTAINER = os.environ.get("TEXT_CONTAINER","text")
CORPUS_CONTAINER = os.environ.get("CORPUS_CONTAINER","corpus")
OUT_BLOB = os.environ.get("GOLD_BLOB_NAME","gross_negligence_gold_candidates.jsonl")

FNAME_RE = re.compile(r"^(?P<court>.+?)-(?P<upisnik>[a-z]+\d*)-(?P<broj>\d{3,5})-(?P<godina>\d{4})\.txt$", re.IGNORECASE)

NEG_PATTERNS = [
    r"\bnije\s+.*grub(a|om|u)?\s+nepažnj",
    r"\bne\s+može\s+se\s+smatrati\s+grub(a|om|u)?\s+nepažnj",
    r"\bne\s+predstavlja\s+grub(a|om|u)?\s+nepažnj",
    r"\bne\s+ukazuje\s+na\s+grub(a|om|u)?\s+nepažnj",
]
POS_PATTERNS = [
    r"\bpostoji\s+grub(a|om|u)?\s+nepažnj",
    r"\butvr(đ|d)uje\s+se\s+grub(a|om|u)?\s+nepažnj",
    r"\bima\s+elemenata\s+grub(e|e)\s+nepažnj",
    r"\bgrub(a|om|u)?\s+nepažnj(a)?\s+je\s+.*(utvr(đ|d)ena|postojala|prisutan)",
]

NEG_RE = re.compile("|".join(f"(?:{p})" for p in NEG_PATTERNS), re.IGNORECASE)
POS_RE = re.compile("|".join(f"(?:{p})" for p in POS_PATTERNS), re.IGNORECASE)
KEYWORD_RE = re.compile(r"\bgrub(a|om|u)?\s+nepažnj", re.IGNORECASE)

def strip_diacritics(t: str) -> str:
    # minimalna mapa za srpski
    return (t.replace("ž","z").replace("Ž","Z")
             .replace("č","c").replace("Č","C")
             .replace("ć","c").replace("Ć","C")
             .replace("š","s").replace("Š","S")
             .replace("đ","d").replace("Đ","D"))

def normalize_court_slug(slug: str) -> str:
    return slug.strip().replace("_", "-")

def humanize_slug(slug: str) -> str:
    parts = slug.split("-")
    out = []
    for w in parts:
        if w in {"u", "na", "i"}:
            out.append(w)
        else:
            out.append(w.capitalize())
    return " ".join(out)

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def doc_id(name: str, txt: str) -> str:
    return hashlib.sha256((name + "|" + sha256(txt)).encode("utf-8")).hexdigest()

def split_paragraphs(txt: str):
    return [p.strip() for p in re.split(r"\n\s*\n+", txt) if p.strip()]

def matched_spans(paragraph: str, regex: re.Pattern, span_type: str):
    spans = []
    for m in regex.finditer(paragraph):
        spans.append({"start": m.start(), "end": m.end(), "type": span_type})
    return spans

def pick_decision_paragraph(txt: str):
    paras = split_paragraphs(txt)
    pcount = len(paras)

    def norm_lat(t: str) -> str:
        # latinica bez dijakritika
        return strip_diacritics(t).lower()

    def norm_any(t: str) -> str:
        return t.lower()

    # Latin + Cyrillic "gruba ne(p)aznja/nepažnja/nepaznja"
    GROSS_LAT = re.compile(r"\bgrub[a-z]*\s+nepaznj[a-z]*\b", re.IGNORECASE)
    GROSS_CYR = re.compile(r"\bгруб[а-я]*\s+непажњ[а-я]*\b", re.IGNORECASE)

    # Sinonimi (kandidat, ali abstain)
    SYN_LAT = re.compile(r"\b(krajnj[a-z]*|tesk[a-z]*|ocigledn[a-z]*|izrazit[a-z]*)\s+nepaznj[a-z]*\b", re.IGNORECASE)
    SYN_CYR = re.compile(r"\b(крајњ[а-я]*|тешк[а-я]*|очигледн[а-я]*|изразит[а-я]*)\s+непажњ[а-я]*\b", re.IGNORECASE)

    # Negacija (latin + cyr)
    NEG_LAT = re.compile(r"\b(nije|nisu|ne\s+moze|ne\s+moze\s+se\s+smatrati|ne\s+predstavlja|ne\s+ukazuje|ne\s+postoji)\b", re.IGNORECASE)
    NEG_CYR = re.compile(r"\b(није|нису|не\s+може|не\s+може\s+се\s+сматрати|не\s+представља|не\s+указује|не\s+постоји)\b", re.IGNORECASE)

    # 1) eksplicitno "gruba nepažnja" (oba pisma)
    for idx, p in enumerate(paras):
        pn_lat = norm_lat(p)
        pn_cyr = norm_any(p)

        if GROSS_LAT.search(pn_lat) or GROSS_CYR.search(pn_cyr):
            spans = []
            if GROSS_LAT.search(pn_lat):
                spans += matched_spans(pn_lat, GROSS_LAT, "GROSS_LAT")
            if GROSS_CYR.search(pn_cyr):
                spans += matched_spans(pn_cyr, GROSS_CYR, "GROSS_CYR")

            neg = bool(NEG_LAT.search(pn_lat) or NEG_CYR.search(pn_cyr))
            if neg:
                return p, idx, pcount, "GROSS_TERM_NEGATED", 0, 0.95, False, spans
            return p, idx, pcount, "GROSS_TERM", 1, 0.90, False, spans

    # 2) sinonimi -> abstain
    for idx, p in enumerate(paras):
        pn_lat = norm_lat(p)
        pn_cyr = norm_any(p)
        if SYN_LAT.search(pn_lat) or SYN_CYR.search(pn_cyr):
            spans=[]
            if SYN_LAT.search(pn_lat):
                spans += matched_spans(pn_lat, SYN_LAT, "SYN_LAT")
            if SYN_CYR.search(pn_cyr):
                spans += matched_spans(pn_cyr, SYN_CYR, "SYN_CYR")
            return p, idx, pcount, "SYNONYM_ONLY", None, 0.60, True, spans

    return "", None, pcount, "NO_MATCH", None, 0.0, True, []



def main():
    bsc = BlobServiceClient(
        account_url=f"https://{ACCOUNT}.blob.core.windows.net",
        credential=KEY,
        transport=RequestsTransport(connection_timeout=10, read_timeout=180),
    )
    textc = bsc.get_container_client(TEXT_CONTAINER)
    corpusc = bsc.get_container_client(CORPUS_CONTAINER)

    out_lines = []
    n = 0
    skipped_name = 0

    for b in textc.list_blobs():
        if not b.name.endswith(".txt"):
            continue

        name = b.name
        m = FNAME_RE.match(name)
        if not m:
            skipped_name += 1
            continue

        txt = textc.get_blob_client(name).download_blob().readall().decode("utf-8", errors="ignore").strip()
        if len(txt) < 50:
            continue

        court_slug = normalize_court_slug(m.group("court"))
        upisnik = m.group("upisnik").lower()
        broj = int(m.group("broj"))
        godina = int(m.group("godina"))

        para, pidx, pcount, rule, label, conf, abstain, spans = pick_decision_paragraph(txt)

        rec = {
            "doc_id": doc_id(name, txt),
            "file_name": name,
            "court": humanize_slug(court_slug),
            "court_slug": court_slug,
            "upisnik": upisnik,
            "broj": broj,
            "godina": godina,

            "gross_negligence": 1 if label == 1 else 0,
            "not_gross_negligence": 1 if label == 0 else 0,

            "decision_paragraph": para,
            "paragraph_index": pidx,
            "paragraph_count": pcount,
            "matched_spans": spans,
            "auto_rule": rule,

            "confidence": conf,
            "abstain": bool(abstain),

            "verification_status": "auto",
            "label_source": "auto_rule",
            "verified_label": None,

            "text_hash": sha256(txt),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "char_len": len(txt),
        }
        out_lines.append(json.dumps(rec, ensure_ascii=False))
        n += 1

    payload = ("\n".join(out_lines) + "\n").encode("utf-8")
    corpusc.get_blob_client(OUT_BLOB).upload_blob(payload, overwrite=True)
    print(f"Wrote {n} docs to {CORPUS_CONTAINER}/{OUT_BLOB} ({len(payload)} bytes). Skipped (bad filename): {skipped_name}")

if __name__ == "__main__":
    main()
