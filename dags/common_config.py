"""
Módulo de Configuración Común para DAGs de Airflow (US-13).
Responsable: Andrés Teherán (DevOps, SCM & Airflow Infra)

Estandariza los parámetros operativos de los flujos de la plataforma Lambda:
- Política de reintentos (`retries=3`) con retroceso exponencial (`retry_exponential_backoff=True`).
- Tiempos de espera y tolerancia ante fallos de red transitorios.
- Callbacks globales para registro y notificación de fallos (`on_failure_callback`).
- Metadatos de auditoría y linaje para gobierno de datos.
"""

import logging
from datetime import timedelta
from typing import Any, Dict

logger = logging.getLogger("airflow.dag.common")


def default_failure_alert(context: Dict[str, Any]) -> None:
    """
    Callback global ejecutado cuando una tarea o DAG falla.
    Registra metadatos estructurados para facilitar la detección y depuración.
    """
    dag_id = context.get("dag", {}).dag_id if hasattr(context.get("dag"), "dag_id") else context.get("dag_id", "unknown_dag")
    task_id = context.get("task", {}).task_id if hasattr(context.get("task"), "task_id") else context.get("task_instance", {}).task_id if hasattr(context.get("task_instance"), "task_id") else "unknown_task"
    execution_date = context.get("logical_date") or context.get("execution_date")
    exception = context.get("exception", "No exception info provided")

    logger.error(
        f"🚨 [AIRFLOW ALERT] Tarea fallida en DAG '{dag_id}':\n"
        f"  - Task ID: {task_id}\n"
        f"  - Logical Date: {execution_date}\n"
        f"  - Error / Excepción: {exception}"
    )


def default_success_callback(context: Dict[str, Any]) -> None:
    """
    Callback informativo para auditar el cumplimiento exitoso de tareas críticas.
    """
    dag_id = context.get("dag", {}).dag_id if hasattr(context.get("dag"), "dag_id") else "unknown_dag"
    task_id = context.get("task_instance", {}).task_id if hasattr(context.get("task_instance"), "task_id") else "unknown_task"
    logger.info(f"✅ [AIRFLOW SUCCESS] Tarea finalizada exitosamente: {dag_id}.{task_id}")


# Argumentos estándar exigidos por los Criterios de Aceptación de US-13
DEFAULT_DAG_ARGS: Dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": default_failure_alert,
}


def get_default_args(owner: str = "airflow", retries: int = 3, **overrides: Any) -> Dict[str, Any]:
    """
    Retorna un diccionario seguro de argumentos por defecto para DAGs con soporte para overrides.
    """
    args = dict(DEFAULT_DAG_ARGS)
    args["owner"] = owner
    args["retries"] = retries
    args.update(overrides)
    return args
