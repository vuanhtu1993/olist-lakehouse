# AI Coding Agent Context Routing Rules: Olist-Lakehouse-M0

> **Objective:** Enforce Just-in-Time (JIT) context loading discipline for all AI Coding Agents (Claude, Gemini, Copilot, Cursor).  
> **Core Principle:** NEVER load the entire `requirements/` directory into context at once. Prevent context window pollution and *Lost in the Middle* failures.

---

## 🧭 Task-to-Context Mapping Matrix

| Specific Role / Task | Mandatory Files to Load | Optional Files (As Needed) | Strictly FORBIDDEN to Load |
|---|---|---|---|
| **Backend API Development** (`src/services/`, `src/routes/`) | 1. `requirements/global/business-rules.md`<br>2. `requirements/modules/<module>/api-contract.md` | `requirements/global/domain-models.md` | Other modules' `spec.md` and `acceptance-criteria.md` |
| **Frontend UI Development** (`public/`, UI rendering) | 1. `requirements/modules/<module>/api-contract.md`<br>2. `requirements/modules/<module>/spec.md` | `requirements/governance/nfr.md` (Usability) | Database SQL internals, migration scripts |
| **Unit & Acceptance Testing** (`tests/`) | 1. `requirements/modules/<module>/acceptance-criteria.md`<br>2. `requirements/modules/<module>/api-contract.md` | `requirements/global/business-rules.md` | Unrelated modules' files |
| **Database Schema & Migrations** | 1. `requirements/global/domain-models.md`<br>2. `requirements/governance/adr/` | `requirements/global/business-rules.md` | Frontend view templates, CSS styling |
| **Architecture Review & Audit** | 1. `requirements/governance/adr/`<br>2. `requirements/governance/traceability-matrix.md` | `requirements/governance/nfr.md` | Individual granular test scripts |

---

## ⛔ Forbidden Actions & Violations
1. **No Greedy Context Dumping:** Do not execute blanket glob patterns like `requirements/**/*.md` to ingest all files at once.
2. **No Arbitrary Rule Alteration:** If a mismatch between code and `global/business-rules.md` is discovered, query the user for clarification rather than modifying the business invariant.
3. **No Unilateral Architectural Overrides:** Prior to suggesting new libraries (e.g., ORM, UI Frameworks), check `governance/adr/` for binding precedent decisions.

---
*Made by Anh Tu - Share to be share*
