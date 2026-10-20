---
description: Retoma el trabajo de Spark mostrando el estado real y proponiendo los siguientes pasos
---

Retoma el trabajo de la capa Spark en este repositorio. Contexto completo en `AGENTS.md`.

**Estado actual del repositorio:**

!`git branch --show-current && echo "--- commits ---" && git log --oneline -8 && echo "--- estado ---" && git status --short && echo "--- PRs abiertos ---" && gh api repos/4ndr3s-00/lambda-crypto-platform/pulls --jq '.[] | select(.state=="open") | "  #\(.number) \(.head.ref) -> \(.base.ref) [\(.title[0:60])]"' 2>/dev/null`

**¿El PR #37 ya fue aceptado y mergeado?**

!`gh api repos/4ndr3s-00/lambda-crypto-platform/pulls/37 --jq '"  state=\(.state) merged=\(.merged)"' 2>/dev/null; echo "  --- develop remoto ---"; git log --oneline origin/develop -3 2>/dev/null`

**Revisiones pendientes en el PR #37:**

!`gh api repos/4ndr3s-00/lambda-crypto-platform/pulls/37/reviews --jq '.[] | "  REVIEW: \(.user.login) \(.state)"' 2>/dev/null; echo "  (sin revisiones si no hay líneas arriba)"; gh api repos/4ndr3s-00/lambda-crypto-platform/pulls/37/comments --jq '.[] | "  COMENTARIO: \(.user.login) [\(.path):\(.line // .original_line)]"' 2>/dev/null`

**Historias asignadas a Andrés Blanco (el usuario de esta sesión):**

!`for n in 10 11 12 21; do gh api repos/4ndr3s-00/lambda-crypto-platform/issues/$n --jq '"  US-\(.number|tostring) [\(.state)/\(.state_reason)] \(.title)"'; done`

**Qué necesito ahora:**

1. Resumen de en qué punto quedó el trabajo y qué está pendiente, en pocas líneas.
2. Si el entorno de Docker no está levantado, los comandos exactos para levantarlo.
3. Los siguientes pasos concretos, ordenados por prioridad.
4. Si detectas que algún PR, rama o archivo quedó a medias, dilo explícitamente.

**Reglas que ya están resueltas (no hace falta volver a preguntarlas):**

- Si el PR #37 aparece como **mergeado**, el trabajo ya está en `develop`: hay
  que trabajar sobre `develop`, la rama `feature/spark-batch-ohlcv` es historia
  y las issues deberían estar cerradas. En ese caso, los siguientes pasos
  continúan desde el punto 3 de "Pendiente" del `AGENTS.md`.
- Si el PR sigue **abierto**, está pendiente de revisión por parte de los
  compañeros: **no corresponde mergearlo sin que lo aprueben**. Si ya hay
  comentarios de revisión, el trabajo es responderlos iterando sobre la misma
  rama y haciendo push (el PR se actualiza solo).
- Las issues US-10, US-11, US-12 y US-21 están **abiertas** a propósito: en
  este repositorio las issues se cierran cuando su PR se mergea. Si aparecen
  cerradas con el PR aún abierto, reabrirlas y avisar en el PR.

No rehagas trabajo ya hecho. No crees ramas nuevas sin avisar. Antes de proponer
cambios, verifica el estado real con los comandos de arriba.
