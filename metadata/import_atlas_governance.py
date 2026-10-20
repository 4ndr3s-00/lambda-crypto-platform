"""
Script de importación y sincronización de Gobierno de Datos en Apache Atlas (US-27).

Historias de usuario cubiertas:
    - US-27 Criterio 1: Registro formal del Glosario Empresarial Unificado de Cripto y Finanzas
                        (Vela Japonesa OHLCV, Precio de Cierre, Volumen Ponderado, Event-Time,
                         Retorno Porcentual y Watermark).
    - US-27 Criterio 2: Creación de taxonomías y clasificación de entidades de la plataforma
                        (Datos_Publicos_Mercado, Metadatos_Sensibles, Datos_Provisionales_Speed).
    - US-27 Criterio 3: Delimitación de responsabilidades Atlas vs OpenMetadata.

Uso:
    python metadata/import_atlas_governance.py --dry-run
    python metadata/import_atlas_governance.py --atlas-url http://localhost:21000 --user admin --password admin
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    requests = None

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("atlas-governance-sync")

# Términos mandatorios por el Criterio 1 BDD
REQUIRED_GLOSSARY_TERMS = {
    "Vela_Japonesa_OHLCV",
    "Precio_de_Cierre",
    "Volumen_Ponderado",
    "Event_Time",
    "Retorno_Porcentual",
    "Watermark",
}

# Clasificaciones mandatorias por el Criterio 2 BDD
REQUIRED_CLASSIFICATIONS = {
    "Datos_Publicos_Mercado",
    "Metadatos_Sensibles",
    "Datos_Provisionales_Speed",
}


def parse_args(argv=None) -> argparse.Namespace:
    """Define los argumentos de la línea de comandos."""
    parser = argparse.ArgumentParser(
        prog="import_atlas_governance.py",
        description="Aprovisiona el Glosario y las Clasificaciones de la plataforma en Apache Atlas.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--atlas-url",
        type=str,
        default=os.environ.get("ATLAS_URL", "http://localhost:21000"),
        help="URL base de la API REST v2 de Apache Atlas.",
    )
    parser.add_argument(
        "--user",
        type=str,
        default=os.environ.get("ATLAS_USER", "admin"),
        help="Usuario administrador de Apache Atlas.",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.environ.get("ATLAS_PASSWORD", "admin"),
        help="Contraseña del usuario de Apache Atlas.",
    )
    parser.add_argument(
        "--metadata-dir",
        type=str,
        default=str(Path(__file__).resolve().parent),
        help="Directorio donde residen los archivos JSON de metadatos.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida los esquemas y definiciones locales sin enviar peticiones HTTP a Atlas.",
    )

    return parser.parse_args(argv)


def load_governance_files(metadata_dir: Path) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Carga los tres archivos canónicos de gobierno en formato JSON."""
    glossary_path = metadata_dir / "atlas_glossary.json"
    types_path = metadata_dir / "atlas_types_classifications.json"
    entities_path = metadata_dir / "atlas_entity_mappings.json"

    if not glossary_path.exists():
        raise FileNotFoundError(f"Archivo de glosario ausente: {glossary_path}")
    if not types_path.exists():
        raise FileNotFoundError(f"Archivo de clasificaciones ausente: {types_path}")
    if not entities_path.exists():
        raise FileNotFoundError(f"Archivo de mapeos de entidades ausente: {entities_path}")

    glossary_data = json.loads(glossary_path.read_text(encoding="utf-8"))
    types_data = json.loads(types_path.read_text(encoding="utf-8"))
    entities_data = json.loads(entities_path.read_text(encoding="utf-8"))

    return glossary_data, types_data, entities_data


def validate_governance_definitions(
    glossary_data: Dict[str, Any],
    types_data: Dict[str, Any],
    entities_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Valida la conformidad formal del glosario, las clasificaciones y los mapeos
    contra los Criterios de Aceptación de la historia US-27.
    """
    report = {
        "glossary_name": glossary_data.get("glossary", {}).get("name"),
        "terms_count": len(glossary_data.get("terms", [])),
        "classifications_count": len(types_data.get("classificationDefs", [])),
        "entities_count": len(entities_data.get("entities", [])),
        "missing_terms": [],
        "missing_classifications": [],
    }

    # 1. Validar términos de Criterio 1 BDD
    present_terms = {t["name"] for t in glossary_data.get("terms", [])}
    missing_terms = REQUIRED_GLOSSARY_TERMS - present_terms
    if missing_terms:
        report["missing_terms"] = list(missing_terms)
        raise ValueError(
            f"Faltan términos obligatorios de negocio en el glosario (US-27 C1): {missing_terms}"
        )

    # 2. Validar clasificaciones de Criterio 2 BDD
    present_classifications = {c["name"] for c in types_data.get("classificationDefs", [])}
    missing_classes = REQUIRED_CLASSIFICATIONS - present_classifications
    if missing_classes:
        report["missing_classifications"] = list(missing_classes)
        raise ValueError(
            f"Faltan clasificaciones normativas en la taxonomía (US-27 C2): {missing_classes}"
        )

    # 3. Validar consistencia de mapeos de entidades
    for entity in entities_data.get("entities", []):
        for c in entity.get("classifications", []):
            if c not in present_classifications:
                raise ValueError(
                    f"Entidad '{entity.get('name')}' referencia una clasificación no declarada: '{c}'"
                )
        for t in entity.get("glossaryTerms", []):
            if t not in present_terms:
                raise ValueError(
                    f"Entidad '{entity.get('name')}' referencia un término de glosario no declarado: '{t}'"
                )

    return report


class AtlasRestClient:
    """Cliente para la REST API v2 de Apache Atlas."""

    def __init__(self, base_url: str, user: str, password: str):
        if requests is None:
            raise RuntimeError("El módulo 'requests' es requerido para interactuar con Apache Atlas.")
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(user, password)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def check_health(self) -> bool:
        """Verifica conectividad y estado del servidor Atlas."""
        try:
            resp = requests.get(
                f"{self.base_url}/api/atlas/admin/status",
                auth=self.auth,
                headers=self.headers,
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False

    def sync_typedefs(self, types_data: Dict[str, Any]) -> None:
        """Registra las definiciones de clasificación (classificationDefs) en Atlas."""
        url = f"{self.base_url}/api/atlas/v2/types/typedefs"
        resp = requests.post(url, auth=self.auth, headers=self.headers, json=types_data, timeout=15)
        if resp.status_code in (200, 201):
            logger.info("✅ Definiciones de clasificación registradas exitosamente en Atlas.")
        elif resp.status_code == 409:
            # Ya existían: intentamos actualización
            put_resp = requests.put(url, auth=self.auth, headers=self.headers, json=types_data, timeout=15)
            put_resp.raise_for_status()
            logger.info("🔄 Definiciones de clasificación actualizadas exitosamente en Atlas.")
        else:
            resp.raise_for_status()

    def sync_glossary(self, glossary_data: Dict[str, Any]) -> None:
        """Crea el glosario empresarial y sus términos asociados."""
        glossary_info = glossary_data.get("glossary", {})
        terms_list = glossary_data.get("terms", [])

        # Crear Glosario principal
        url_glossary = f"{self.base_url}/api/atlas/v2/glossary"
        resp = requests.post(
            url_glossary, auth=self.auth, headers=self.headers, json=glossary_info, timeout=15
        )
        if resp.status_code in (200, 201):
            glossary_guid = resp.json().get("guid")
            logger.info(f"✅ Glosario '{glossary_info['name']}' creado (GUID: {glossary_guid}).")
        else:
            # Buscar si ya existe
            get_resp = requests.get(url_glossary, auth=self.auth, headers=self.headers, timeout=15)
            get_resp.raise_for_status()
            existing = [g for g in get_resp.json() if g.get("name") == glossary_info["name"]]
            if not existing:
                resp.raise_for_status()
            glossary_guid = existing[0]["guid"]
            logger.info(f"ℹ️ Glosario existente encontrado (GUID: {glossary_guid}).")

        # Crear términos bajo el glosario
        url_terms = f"{self.base_url}/api/atlas/v2/glossary/terms"
        for term in terms_list:
            term_payload = {
                "name": term["name"],
                "shortDescription": term["shortDescription"],
                "longDescription": term["longDescription"],
                "anchor": {"glossaryGuid": glossary_guid},
            }
            term_resp = requests.post(
                url_terms, auth=self.auth, headers=self.headers, json=term_payload, timeout=15
            )
            if term_resp.status_code in (200, 201):
                logger.info(f"  + Término registrado: '{term['name']}'")
            elif term_resp.status_code == 409:
                logger.info(f"  = Término ya existente: '{term['name']}'")
            else:
                logger.warning(f"  ! Error creando término '{term['name']}': {term_resp.status_code}")


def main(argv=None) -> int:
    """Punto de entrada de ejecución del script de gobierno."""
    args = parse_args(argv)

    logger.info("=" * 70)
    logger.info("🏛️ SINCRONIZACIÓN DE GOBIERNO EN APACHE ATLAS (US-27)")
    logger.info(f"  Directorio Metadatos: {args.metadata_dir}")
    logger.info(f"  Atlas URL:            {args.atlas_url}")
    logger.info(f"  Modo Dry-Run:         {args.dry_run}")
    logger.info("=" * 70)

    metadata_path = Path(args.metadata_dir)
    glossary_data, types_data, entities_data = load_governance_files(metadata_path)

    # 1. Validación estricta BDD
    report = validate_governance_definitions(glossary_data, types_data, entities_data)
    logger.info("✅ Validación semántica y de contrato superada con éxito:")
    logger.info(f"  • Glosario:         '{report['glossary_name']}' ({report['terms_count']} términos)")
    logger.info(f"  • Clasificaciones:  {report['classifications_count']} taxonomías definidas")
    logger.info(f"  • Entidades:        {report['entities_count']} mapeadas")

    if args.dry_run:
        logger.info("🛡️ Modo dry-run completado sin peticiones remotas.")
        return 0

    # 2. Conexión y sincronización con el servidor Apache Atlas
    client = AtlasRestClient(args.atlas_url, args.user, args.password)
    logger.info(f"Verificando estado de Apache Atlas en {args.atlas_url}...")
    if not client.check_health():
        logger.warning(
            f"⚠️ Apache Atlas no responde en '{args.atlas_url}'. "
            "Asegúrate de levantarlo con: docker compose -f infra/docker-compose.yml -f infra/docker-compose.atlas.yml up -d atlas"
        )
        return 1

    logger.info("Aprovisionando taxonomías y clasificaciones...")
    client.sync_typedefs(types_data)

    logger.info("Aprovisionando glosario empresarial y términos...")
    client.sync_glossary(glossary_data)

    logger.info("🎉 Sincronización de gobierno en Apache Atlas concluida con éxito.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
