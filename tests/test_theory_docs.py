"""
Test kiểm tra module lý thuyết Bronze Layer bao quát đầy đủ 10 khái niệm (11-20):
Ingest as-is 1:1, Metadata columns, Record hash, Change detection & dedup,
Append-only & CDC, Schema drift, Structural quarantine, Load patterns,
PII masking HMAC-SHA256, và Requirements -> User story -> BD/DD -> EDA.
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os

def test_theory_concepts_coverage():
    path = "docs/week02/theory_bronze_layer.md"
    assert os.path.exists(path), f"File {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    
    concepts = [
        "Ingest as-is", "Metadata columns", "Record hash", "Change detection",
        "Append-only", "Schema drift", "Structural quarantine", "Load patterns",
        "PII masking", "User story"
    ]
    for c in concepts:
        assert c in content, f"Thiếu khái niệm lý thuyết: '{c}'!"
        
    assert "Made by Anh Tu - Share to be share" in content, "Thiếu footer bản quyền!"

def test_all_8_modular_docs_exist():
    expected_modules = [
        "01-bronze-layer-fundamentals.md",
        "02-metadata-columns-and-lineage.md",
        "03-record-hash-and-cdc.md",
        "04-schema-drift-and-quarantine.md",
        "05-load-patterns-and-watermarks.md",
        "06-pii-masking-and-privacy.md",
        "07-data-engineering-eda-and-profiling.md",
        "08-week02-interview-cheatsheet.md",
    ]
    for mod in expected_modules:
        mod_path = os.path.join("docs", "week02", "theory", mod)
        if not os.path.exists(mod_path):
            mod_path = os.path.join("docs", "week02", mod)
        assert os.path.exists(mod_path), f"File module {mod_path} không tồn tại!"
        mod_content = open(mod_path, "r", encoding="utf-8").read()
        assert "Made by Anh Tu - Share to be share" in mod_content, f"Module {mod} thiếu footer bản quyền!"
