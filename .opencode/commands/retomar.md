---
description: Retoma el trabajo en este repositorio mostrando el estado real y proponiendo los siguientes pasos
---

Retoma el trabajo en este repositorio. Contexto completo en `AGENTS.md`.

**Estado actual del repositorio:**

!`git branch --show-current && echo "--- commits ---" && git log --oneline -8 && echo "--- estado ---" && git status --short`

**PR y revisión de la rama actual:**

!`gh pr status 2>/dev/null`

**Otros PRs abiertos en el repo:**

!`gh api repos/4ndr3s-00/lambda-crypto-platform/pulls --jq '.[] | select(.state=="open") | "  #\(.number) \(.head.ref) -> \(.base.ref) [\(.title[0:60])]"' 2>/dev/null`

**Issues abiertas asignadas a quien corre este comando:**

!`gh issue list --assignee @me --state open --json number,title,labels --jq '.[] | "  #\(.number) \(.title)"' 2>/dev/null`

**Qué necesito ahora:**

1. Resumen de en qué punto quedó el trabajo y qué está pendiente, en pocas líneas.
2. Si el entorno de Docker no está levantado, los comandos exactos para levantarlo (ver `AGENTS.md §3`).
3. Los siguientes pasos concretos, ordenados por prioridad.
4. Si detectas que algún PR, rama o archivo quedó a medias, dilo explícitamente.

**Convenciones del repo (no hace falta volver a preguntarlas):**

- Las issues se cierran cuando se mergea su PR, no antes (ver `AGENTS.md §1`).
  Si una issue aparece cerrada con su PR todavía abierto, es un error: reabrirla
  y avisar en el PR.
- No mergees un PR propio sin que el equipo lo apruebe: el repositorio exige
  revisión por pares (ver §3 del README).
- Si un PR tiene comentarios de revisión pendientes, el trabajo es iterar sobre
  la misma rama y hacer *push*; el PR se actualiza solo, no hace falta uno nuevo.

No rehagas trabajo ya hecho. No crees ramas nuevas sin avisar. Antes de proponer
cambios, verifica el estado real con los comandos de arriba.
