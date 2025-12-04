# dataverse_apis/tasks/object_id_resolver.py
from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import Callable, Iterable, List, Dict, Any

# Importa tu función real para llamar a Dataverse:
from ..core.services.dataverse_client import call_dataverse  # ajusta si usas una clase

# --- Modelo simple y extensible ---
@dataclass
class Target:
    entity: str
    ticket_number: str
    file: str = ""
    sheet: str = ""
    column: str = ""
    object_id: str | None = None
    relative_urls: List[str] = field(default_factory=list)  # para la siguiente fase

    @property
    def sharepoint_entity(self) -> str:
        # SharePoint usa "incident" cuando la entidad negocio es "case"
        return "incident" if self.entity.lower() == "case" else self.entity.lower()

def to_targets(items: Iterable[Dict[str, Any]]) -> List[Target]:
    """Convierte una lista de dicts (tu formato actual) a dataclasses Target."""
    out: List[Target] = []
    for d in items:
        out.append(
            Target(
                entity=str(d.get("entity", "")).strip(),
                ticket_number=str(d.get("ticket_number", "")).strip(),
                file=str(d.get("file", "")),
                sheet=str(d.get("sheet", "")),
                column=str(d.get("column", "")),
                object_id=d.get("object_id"),
                relative_urls=list(d.get("relative_urls", [])) if d.get("relative_urls") else [],
            )
        )
    return out

def to_dicts(items: Iterable[Target]) -> List[Dict[str, Any]]:
    """Convierte Targets de vuelta a dicts si tu UI/worker lo necesita."""
    return [asdict(t) for t in items]

# --- Resolver de object_id por entidad (sencillo y explícito) ---
class ObjectIdResolver:
    def __init__(self, dv_call: Callable[[str], Dict[str, Any]] = call_dataverse,
                 logger: Callable[[str], None] | None = None) -> None:
        self.dv_call = dv_call
        self.log = logger or (lambda msg: None)

    def enrich_with_object_ids(self, targets: List[Target]) -> List[Target]:
        """Itera targets y agrega .object_id según la entidad."""
        for t in targets:
            ent = (t.entity or "").lower()
            key = (t.ticket_number or "").strip()

            if not ent or not key:
                self.log("⚠️ Target sin 'entity' o 'ticket_number' — se omite.")
                t.object_id = None
                continue

            endpoint, id_field = self._build_endpoint_and_id_field(ent, key)
            if not endpoint:
                self.log(f"⚠️ Entidad desconocida '{ent}' — se omite.")
                t.object_id = None
                continue

            try:
                self.log(f"🔎 DV query: {endpoint}")
                result = self.dv_call(endpoint) or {}
                items = result.get("value") or []
                if items:
                    first = items[0]
                    t.object_id = first.get(id_field) or first.get(id_field.lower())
                    self.log(f"   ✓ {ent} {key} → {t.object_id}")
                else:
                    t.object_id = None
                    self.log(f"   ⚠️ {ent} {key}: sin resultados.")
            except Exception as e:
                t.object_id = None
                self.log(f"   ❌ Error DV ({ent} {key}): {e}")

        return targets

    # -------------------- helpers internos --------------------
    # @staticmethod
    # def _q(val: str) -> str:
    #     return f"'{(val or '').replace(\"'\", \"''\")}'"

    def _build_endpoint_and_id_field(self, ent: str, key: str) -> tuple[str | None, str | None]:
        # q = self._q
        if ent == "account":
            return (f"accounts?$select=accountid&$filter=accountnumber eq {key}",
                    "accountid")
        if ent == "case":
            return (f"incidents?$select=incidentid&$filter=ticketnumber eq {key}",
                    "incidentid")
        if ent == "ecase":
            return (f"icps_ecases?$select=icps_ecaseid&$filter=icps_name eq {key}",
                    "icps_ecaseid")
        if ent == "inspection":
            return (f"icps_inspections?$select=icps_inspectionid&$filter=icps_name eq {key}",
                    "icps_inspectionid")
        if ent == "investigation":
            return (f"icps_investigations?$select=icps_investigationid&$filter=icps_name eq {key}",
                    "icps_investigationid")
        return (None, None)
