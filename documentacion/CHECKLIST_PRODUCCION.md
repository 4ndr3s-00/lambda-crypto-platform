# 🔒 CHECKLIST DE HARDENING, SEGURIDAD Y PASE A PRODUCCIÓN (US-33)
**Plataforma de Datos Lambda (Finanzas & Cripto)**  
**Responsable:** Andrés Teherán (DevOps & Security Lead)  
**Versión:** 1.0 - Lista de Verificación Previa a Go-Live

---

## 1. Gestión de Secretos y Control de Acceso
- [x] **Cero Secretos en Git:** Verificación con herramientas de auditoría estática (`.gitignore` exhaustivo cubriendo `.env`, claves privadas y certificados).
- [x] **Plantilla de Configuración:** Existencia y mantenimiento de `.env.example` con valores de muestra sin secretos reales.
- [x] **Mínimo Privilegio en MinIO:** Usuarios de servicio dedicados (`spark_service`, `airflow_user`) con políticas IAM restringidas a sus respectivos buckets operativos (`market-lake`, `market-checkpoints`, `market-quarantine`).
- [x] **Aislamiento en PostgreSQL:** Bases de datos independientes (`serving_db`, `airflow_db`, `metastore_db`) con usuarios separados y contraseñas robustas.

---

## 2. Seguridad en la Capa de Servicio (API Backend)
- [x] **Autenticación en Endpoints Administrativos:** `/api/pipeline/status` protegido mediante cabecera `Authorization: Bearer <TOKEN>` o `X-API-Key` (retornando `HTTP 401 Unauthorized` si no está autorizado).
- [x] **Guardrails SQL Estrictos:** Validador AST y expresiones regulares prohibiendo consultas de modificación (`DROP`, `DELETE`, `UPDATE`, `ALTER`, `EXEC`) y forzando `LIMIT <= 100` con timeout de 5 segundos.
- [x] **Validación de Esquema y Sanitización:** Validación estricta con Pydantic y sanitización de caracteres especiales en símbolos bursátiles (`sanitize_symbol`).
- [x] **Políticas de CORS:** Configuración controlada de orígenes permitidos en FastAPI.

---

## 3. Observabilidad, Auditoría y Logging
- [x] **Logging Estructurado JSON:** Serialización de eventos de log en formato JSON con timestamp UTC, severidad y nombre de componente (`api/logging_config.py`).
- [x] **Trazabilidad de Linaje (OpenLineage):** Emisión continua de eventos `START` y `COMPLETE` con grafos de dependencias aguas arriba y aguas abajo (`metadata/openlineage/`).
- [x] **Auditoría de Ingesta:** Registro inmutable de ejecuciones en la tabla `pipeline_execution_log` de PostgreSQL con métricas de duración, registros y estado.
- [x] **Aislamiento de Eventos Anómalos:** Topic dedicado `market.dlq` y prefijo `market-quarantine` para evitar poison pills en tiempo real.
