#!/usr/bin/env bash
set -euo pipefail

# venv
source .venv/bin/activate

# defaults
export RAW_CONTAINER="${RAW_CONTAINER:-raw}"
export TEXT_CONTAINER="${TEXT_CONTAINER:-text}"
export CORPUS_CONTAINER="${CORPUS_CONTAINER:-corpus}"
export OUT_BLOB="${OUT_BLOB:-corpus_anon.jsonl}"

echo "== Step 1: raw -> text (extract + anonymize) =="
python3 -u process_raw_to_text_key.py

echo "== Step 2: text -> corpus jsonl (filename + text) =="
python3 -u build_corpus_minimal.py

echo "Done."
