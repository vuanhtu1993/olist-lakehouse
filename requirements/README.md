# Olist-Lakehouse-M0 — Requirement Analysis Package (Enterprise Granular Architecture)

> **Version:** 1.0 (Baseline — Option C Architecture)  
> **Methodology:** AI-Driven Context Engineering & Progressive Disclosure

---

## 🧭 AI Agent Context Router

When executing tasks in the SDLC workflow (coding, testing, refactoring, reviewing), **DO NOT LOAD THE ENTIRE REPOSITORY CONTEXT AT ONCE**. Follow the **Just-in-Time (JIT) Context Loading** decision tree:

```mermaid
graph TD
    Task([Agent Receives Task]) --> Route{Identify Task Scope}
    Route -->|Overview / Onboarding| LoadOverview["Load global/overview.md & global/business-rules.md"]
    Route -->|Database & Migrations| LoadData["Load global/domain-models.md & governance/adr/"]
    Route -->|Backend API Development| LoadAPI["Load modules/<module>/api-contract.md & global/business-rules.md"]
    Route -->|Frontend UI Development| LoadUI["Load modules/<module>/api-contract.md & modules/<module>/spec.md"]
    Route -->|Acceptance & QA Testing| LoadAC["Load modules/<module>/acceptance-criteria.md & api-contract.md"]
    Route -->|Architecture & Quality Audit| LoadGov["Load governance/traceability-matrix.md & nfr.md"]
```

---

## 📁 Package Structure & Token Budgets

| Directory / File | Core Responsibility | Estimated Tokens | Target Consumer |
|---|---|---|---|
| **global/** | System-wide invariants, business rules, and domain models | ~3,000 tokens | All Agents / Tech Lead |
| **modules/** | Micro-specs decoupled by role (infra_stack, data_seeding, smoke_test) | ~1,500 tokens/module | PO / Dev / QA |
| **governance/** | Quality standards, RTM, change control, and ADRs | ~3,000 tokens | Tech Lead / Architect |

---
*Made by Anh Tu - Share to be share*
