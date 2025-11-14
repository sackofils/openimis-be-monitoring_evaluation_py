from django.utils import timezone
from location.models import Location
import logging

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Types de formulaires Kobo liés au Suivi-Évaluation
# --------------------------------------------------------------------

FORM_TYPES = {
    "FORM_TMU & TMR": "TMU_TMR",
    "FORM_SERE NAFA": "SERE_NAFA",
    "FORM_AGR": "AGR",
    "FORM_SUBVENTION BENEF": "SUBVENTION_BENEF",
    # optionnel : agrégats de plaintes (GRM)
    "GRM_KPI": "GRIEVANCE_KPI",
}


# --------------------------------------------------------------------
# Normalisation de la période (YYYY-Qn)
# --------------------------------------------------------------------

def normalize_period_from_dt(dt):
    """
    Convertit une date en période trimestrielle lisible.
    Exemple : 2025-03-20 -> '2025-Q1'
    """
    if not dt:
        dt = timezone.now()
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{q}"


# --------------------------------------------------------------------
# Extraction de l'identifiant bénéficiaire depuis Kobo
# --------------------------------------------------------------------

def extract_beneficiary_id(kobo_row: dict):
    """
    Essaie plusieurs clés possibles pour récupérer le bénéficiaire.
    """
    return (
        kobo_row.get("beneficiary_uuid")
        or kobo_row.get("id_rsu")
        or kobo_row.get("beneficiary/id")
        or kobo_row.get("group_beneficiary/id")
    )


# --------------------------------------------------------------------
# Extraction et résolution de la localisation
# --------------------------------------------------------------------

def extract_location_id(kobo_row: dict):
    """
    Recherche d’un code ou nom de localité dans les données Kobo.
    Essaie plusieurs champs typiques ('region_code', 'region', etc.).
    """
    loc_code = (
        kobo_row.get("group_geo/region_code")
        or kobo_row.get("group_geo/region")
        or kobo_row.get("group_geo/group_prefecture/region_code")
        or kobo_row.get("region")
        or kobo_row.get("location_id")
    )

    if not loc_code:
        return None

    # Recherche d'abord par code, puis par nom
    loc = Location.objects.filter(code__iexact=str(loc_code)).first()
    if not loc:
        loc = Location.objects.filter(name__iexact=str(loc_code)).first()

    if loc:
        logger.debug(f"[Kobo Mapping] Localité trouvée : {loc} (code={loc_code})")
        return loc.id

    logger.warning(f"[Kobo Mapping] Aucune localité trouvée pour '{loc_code}'")
    return None
