import re, pickle
from fastapi import FastAPI, Query

IDX = pickle.load(open("bm25_index.pkl","rb"))
bm25 = IDX["bm25"]
docs = IDX["docs"]

app = FastAPI(title="Gross Negligence RAG Search API", version="1.0")

def normalize(t:str)->str:
    t=t.lower()
    t=re.sub(r"[^0-9a-zA-Zа-яА-ЯčćšđžČĆŠĐŽ]+", " ", t)
    return t

@app.get("/health")
def health():
    return {"ok": True, "docs": len(docs)}

@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    k: int = Query(10, ge=1, le=25),
    court: str | None = None,
    upisnik: str | None = None,
    godina_from: int | None = None,
    godina_to: int | None = None,
):
    qtok = normalize(q).split()
    scores = bm25.get_scores(qtok)

    # kandidati sortirani po skoru
    order = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)

    out=[]
    for i in order:
        d = docs[i]
        m = d.get("meta", {}) or {}

        # filteri
        if court and (m.get("court") or "").lower() != court.lower():
            continue
        if upisnik and (m.get("upisnik") or "").lower() != upisnik.lower():
            continue
        g = m.get("godina")
        if godina_from and g and int(g) < godina_from:
            continue
        if godina_to and g and int(g) > godina_to:
            continue

        out.append({
            "score": float(scores[i]),
            "doc_id": d.get("doc_id"),
            "text": d.get("text"),
            "meta": m,
        })
        if len(out) >= k:
            break

    return {"query": q, "k": k, "results": out}
