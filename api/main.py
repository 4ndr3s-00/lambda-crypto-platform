"""
Serving Layer API Starter (FastAPI).
Punto de entrada base para el desarrollo de la capa de servicio de la plataforma Lambda.
"""

from fastapi import FastAPI

app = FastAPI(
    title="Lambda Crypto Platform - Serving API",
    version="2.0.0",
    description="Serving Layer starter para la plataforma de datos con arquitectura Lambda"
)

@app.get("/api/health")
def health_check():
    """Endpoint de comprobación de salud del contenedor de Serving."""
    return {
        "status": "healthy",
        "service": "serving-api",
        "architecture": "Lambda (Batch + Speed + Serving)"
    }
