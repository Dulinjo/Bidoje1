import sys, pickle, re
IDX=pickle.load(open("bm25_index.pkl","rb"))
bm25=IDX["bm25"]
docs=IDX["docs"]

def normalize(t:str)->str:
    t=t.lower()
    t=re.sub(r"[^0-9a-zA-Zа-яА-ЯčćšđžČĆŠĐŽ]+", " ", t)
    return t

q=" ".join(sys.argv[1:]).strip()
if not q:
    print("Usage: python3 search_bm25.py <upit>")
    raise SystemExit(1)

qtok=normalize(q).split()
scores=bm25.get_scores(qtok)
top=sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:10]

print("QUERY:", q)
for rank,i in enumerate(top,1):
    d=docs[i]
    m=d["meta"]
    print("\\n#", rank, "score=", round(float(scores[i]),4))
    print("meta:", m.get("court"), m.get("upisnik"), m.get("broj"), m.get("godina"), "| rule:", m.get("auto_rule"), "conf:", m.get("confidence"))
    t=d["text"].replace("\\n"," ")
    print("text:", t[:900])
