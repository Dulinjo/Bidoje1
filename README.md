# corpus-agent (Serbian court decisions → JSONL + mini-RAG search)

Ovaj repo je mali “agentic” pipeline koji:
1) pretvara presude iz Azure Blob Storage (PDF/TXT) u čist tekst,
2) automatski pronalazi pasus gde se sud izjašnjava o **gruboj nepažnji** (ili negaciji),
3) pravi **JSONL dataset sa metapodacima** za dalje eksperimente (ML/LLM/RAG),
4) opcionalno pravi mini “RAG” pretragu (BM25) preko izdvojenih pasusa.

> Nema tajni u repou. Azure ključevi idu preko ENV varijabli / `.env` (ne komituje se).

---

## Output / artefakti

### A) Text container (Azure)
`RAW_CONTAINER` (default: `raw`) → `TEXT_CONTAINER` (default: `text`)  
Script: `process_raw_to_text_key.py`

### B) Gross negligence JSONL (kandidati)
Script: `build_gross_negligence_jsonl.py`  
Blob: `gross_negligence_gold_candidates.jsonl` (default)

Svaka linija u JSONL = jedna presuda, sa poljima (kao “kolone” u CSV):
- `file_name` – naziv `.txt` blob-a
- `court` – nadležni sud (izvučeno iz naziva fajla)
- `upisnik` – npr. gzh, rev, kzz…
- `broj` – broj predmeta (3–5 cifara)
- `godina` – godina
- `decision_paragraph` – pasus gde se traži izjašnjenje o (ne)postojanju grube nepažnje
- `gross_negligence` / `not_gross_negligence` – auto-label (kad ima dovoljno jak signal)
- `auto_rule` – pravilo koje je pogodilo (`GROSS_TERM`, `GROSS_TERM_NEGATED`, `SYNONYM_ONLY`, `NO_MATCH`)
- `confidence` – heuristički skor
- `abstain` – true kad agent nije siguran (quality gating)
- `verification_status`, `verified_label` – placeholder za buduću verifikaciju (nije obavezno)

**Quality gating logika:**
- eksplicitno “gruba nepažnja” + provera negacije → visoka pouzdanost
- sinonimi (npr. “krajnja/teška/očigledna nepažnja”) → kandidat, ali često `abstain=true`

---

## Konfig (ENV)

Na VM napravi `.env` (ne gura se na GitHub):

```bash
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_ACCOUNT_KEY=...
RAW_CONTAINER=raw
TEXT_CONTAINER=text
CORPUS_CONTAINER=corpus
GOLD_BLOB_NAME=gross_negligence_gold_candidates.jsonl
python3 - <<'PY'
import json
from collections import Counter
rows=[json.loads(l) for l in open("gross_negligence_gold_candidates.jsonl","r",encoding="utf-8")]
n=len(rows)
ab=sum(1 for r in rows if r.get("abstain"))
print("TOTAL:", n)
print("ABSTAIN:", ab, f"({ab/n:.1%})")
rules=Counter(r.get("auto_rule","") for r in rows)
print("TOP RULES:", rules.most_common(10))
PY


Disclaimer

Ovo je istraživački/eksperimentalni alat. Heuristike mogu grešiti.
Ne koristiti kao jedini osnov za pravne zaključke bez provere.
