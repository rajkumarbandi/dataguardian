# ADR-XXXX: [Short Decision Title]

**Status:** Draft | Proposed | Accepted | Rejected | Superseded by [ADR-XXXX]
**Date:** YYYY-MM-DD
**Author(s):** [Name / Team]
**Deciders:** [Names of people who made the final decision]

---

## Context

<!--
Describe the situation that necessitated this decision.
Include:
- What problem we were trying to solve
- Any constraints (technical, operational, time, cost) that shaped the decision space
- What triggered the decision (new requirement, incident, scaling concern, etc.)
- Any background knowledge needed to understand the options

This section should be factual, not opinionated.
-->

---

## Decision Drivers

<!--
List the key factors that mattered most in evaluating the options.
These become the scoring criteria for comparing alternatives.

Examples:
- Operational simplicity — fewer moving parts to monitor and maintain
- Unity Catalog lineage — must be traceable end-to-end
- Development velocity — engineers should be able to onboard a new source quickly
- Fault tolerance — AI service unavailability must not block data pipelines
-->

- ...
- ...
- ...

---

## Considered Options

<!--
List all options that were seriously evaluated.
Each option gets its own subsection below.
-->

- Option A: [Name]
- Option B: [Name]
- Option C: [Name — the chosen option]

---

## Decision

**We will [description of the chosen approach].**

<!--
State the decision clearly and unambiguously.
One or two sentences. This is what future readers need to know quickly.
-->

---

## Option Details

### Option A: [Name]

**Description:** [What this option entails]

**Pros:**
- ...

**Cons:**
- ...

**Why rejected / not chosen:**
- ...

---

### Option B: [Name]

**Description:** [What this option entails]

**Pros:**
- ...

**Cons:**
- ...

**Why rejected / not chosen:**
- ...

---

### Option C: [Name] — CHOSEN

**Description:** [What this option entails]

**Pros:**
- ...

**Cons:**
- ...

**Why chosen:**
- ...

---

## Consequences

### Positive

<!--
What becomes better or easier as a result of this decision?
-->

- ...

### Negative

<!--
What do we accept as a trade-off or limitation?
Be honest. Every decision has costs.
-->

- ...

### Neutral / Notes

<!--
Things that change but are neither clearly better nor worse.
Implementation notes or follow-up actions that stem from this decision.
-->

- ...

---

## Implementation Notes

<!--
Optional section. Include if there are specific implementation details,
integration patterns, or constraints that engineers need to know when
carrying this decision into code.
-->

---

## Related Decisions

<!--
Link to related ADRs, design documents, or GitHub issues.
-->

- [ADR-XXXX: Related Decision Title](XXXX-related-decision.md)
- [Design: Component Name](../design/component-design.md)

---

## Review History

| Date | Reviewer | Notes |
|---|---|---|
| YYYY-MM-DD | [Name] | Initial draft |
| YYYY-MM-DD | [Name] | Accepted after review |
