from datetime import date
from typing import Optional, Dict, Callable

from django.db.models import Count, Sum, Q
from django.db import transaction
from django.utils.translation import gettext_lazy as _

from .models import Indicator, IndicatorValue

# --- Import des modules CoreMIS ---
try:
    from payroll.models import Payment, Payroll
except Exception:
    Payment, Payroll = None, None

try:
    from individual.models import Individual, Household
except Exception:
    Individual, Household = None, None

try:
    from social_protection.models import BenefitProgram, Enrollment
except Exception:
    BenefitProgram, Enrollment = None, None

try:
    from grievance_social_protection.models import Ticket
except Exception:
    Ticket = None


# ================================
#  REGISTRE DE FORMULES
# ================================
FormulaFunc = Callable[[Indicator, date, date, Optional[dict]], Dict]
_FORMULAS: Dict[str, FormulaFunc] = {}


def register_formula(code: str):
    def _wrap(fn: FormulaFunc):
        _FORMULAS[code] = fn
        return fn
    return _wrap


# ================================
#  EXEMPLES DE FORMULES
# ================================

@register_formula("IND1")
def compute_IND1(indicator: Indicator, period_start: date, period_end: date, ctx=None) -> Dict:
    """
    IND1 : Système d'information de gestion conçu et opérationnel
    -> Ici : renvoie OUI/NON selon l'état du système (CoreMIS déployé)
    """
    qualitative = "OUI"
    return {"qualitative_value": qualitative, "source": "CoreMIS configuration"}


@register_formula("IND2")
def compute_IND2(indicator: Indicator, period_start: date, period_end: date, ctx=None) -> Dict:
    """
    IND2 : Bénéficiaires ayant reçu des transferts monétaires pendant la période
    Source : Module payroll / HouseholdTransfer
    """
    if not Payment:
        return {"value": None, "source": "Module payroll non disponible"}

    # Comptage unique des ménages payés
    total = (
        Payment.objects.filter(payment_date__range=[period_start, period_end])
        .exclude(status__in=["cancelled", "failed"])
        .values("household_id")
        .distinct()
        .count()
    )
    return {"value": float(total), "source": "Payroll / Payment"}


@register_formula("IND3")
def compute_IND3(indicator: Indicator, period_start: date, period_end: date, ctx=None) -> Dict:
    """
    IND3 : Nombre total de bénéficiaires enregistrés dans CoreMIS
    Source : Module individual
    """
    if not Individual:
        return {"value": None, "source": "Module individual non disponible"}

    total = Individual.objects.filter(validity_to__isnull=True).count()
    return {"value": float(total), "source": "Individual"}


@register_formula("IND4")
def compute_IND4(indicator: Indicator, period_start: date, period_end: date, ctx=None) -> Dict:
    """
    IND4 : Nombre de programmes de prestation sociale actifs
    Source : Module social_protection
    """
    if not BenefitProgram:
        return {"value": None, "source": "Module social_protection non disponible"}

    total = BenefitProgram.objects.filter(is_active=True).count()
    return {"value": float(total), "source": "Social Protection / BenefitProgram"}


@register_formula("IND5")
def compute_IND5(indicator: Indicator, period_start: date, period_end: date, ctx=None) -> Dict:
    """
    IND5 : Nombre de plaintes enregistrées sur la période
    Source : Module grievance_social_protection
    """
    if not Ticket:
        return {"value": None, "source": "Module grievance_social_protection non disponible"}

    total = Ticket.objects.filter(
        date_created__range=[period_start, period_end]
    ).count()

    return {"value": float(total), "source": "Grievance Social Protection / Ticket"}


# ================================
#  DISPATCHER GÉNÉRIQUE
# ================================

def compute_indicator_value(indicator: Indicator, period_start: date, period_end: date, ctx: Optional[dict] = None) -> Dict:
    """
    Exécute la formule associée à un indicateur.
    """
    fn = _FORMULAS.get(indicator.code)
    if not fn:
        return {"value": None, "source": "Aucune formule enregistrée"}
    return fn(indicator, period_start, period_end, ctx)


@transaction.atomic
def calculate_indicators_for_period(period_start: date, period_end: date, ctx: Optional[dict] = None) -> int:
    """
    Calcule tous les indicateurs automatiques actifs pour la période donnée.
    """
    count = 0
    indicators = Indicator.objects.filter(is_automatic=True, is_active=True)

    for ind in indicators:
        defaults = compute_indicator_value(ind, period_start, period_end, ctx)
        IndicatorValue.objects.update_or_create(
            indicator=ind,
            period_start=period_start,
            period_end=period_end,
            defaults=defaults,
        )
        count += 1

    return count
