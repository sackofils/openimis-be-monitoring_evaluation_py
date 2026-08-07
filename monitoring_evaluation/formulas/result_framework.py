import json
from datetime import datetime, timedelta

from grievance_social_protection.models import Ticket
from individual.models import Individual
from payroll.models import BenefitConsumption, BenefitConsumptionStatus
from social_protection.models import Beneficiary, BenefitPlan, BeneficiaryStatus

from ..formula_utils import save_indicator_value as _save_value

def prepare_sla(instance):
    SLA_DAYS = 21
    WARN_WINDOW = 3

    # Parse du JSON
    json_ext = getattr(instance, "json_ext", {}) or {}
    if isinstance(json_ext, str):
        try:
            json_ext = json.loads(json_ext)
        except Exception:
            json_ext = {}

    # Récupération de la date de soumission
    submitted_at = json_ext.get("submitted_at") or instance.date_created
    if not submitted_at:
        return None

    try:
        submitted_dt = (
            datetime.fromisoformat(submitted_at)
            if isinstance(submitted_at, str)
            else submitted_at
        )
    except Exception:
        return None

    # Calcul date d’échéance
    due_date = submitted_dt + timedelta(days=SLA_DAYS)
    today = datetime.now()

    # Calcul du délai restant
    delta = (due_date - today).days

    if delta < 0:
        sla_state = "En depassement"
    elif delta <= WARN_WINDOW:
        sla_state = "En alerte"
    else:
        sla_state = "Dans les délais"

    return {
        "submitted_at": submitted_dt.isoformat(),
        "due_date": due_date.isoformat(),
        "days_remaining": delta,
        "sla_state": sla_state,
    }

def calc_IRI_012(indicator, start, end):
    """
    IRI.012 – % plaintes traitées dans les délais SLA
    """

    tickets = Ticket.objects.filter()

    total_received = tickets.count()
    if total_received == 0:
        return

    treated = tickets.filter(
        status__in=[
            Ticket.TicketStatus.RESOLVED,
            Ticket.TicketStatus.CLOSED
        ]
    )

    treated_on_time = 0
    for ticket in treated:
        sla = prepare_sla(ticket)
        if sla and sla["sla_state"] == "Dans les délais":
            treated_on_time += 1

    value = round((treated_on_time / total_received) * 100, 2)
    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=value,
        source="Grievance / Social Protection"
    )


def calc_ODP_002(indicator, start, end):
    """
    ODP – Nombre de bénéficiaires TMU ayant reçu un paiement (cumulatif)
    """

    # 1. Identifier les plans TMU (Composante 1)
    tmu_plans = BenefitPlan.objects.filter(
        code__icontains="TMU"
    )

    # 2. Bénéficiaires actifs de ces plans
    beneficiaries = Beneficiary.objects.filter(
        benefit_plan__in=tmu_plans,
        status=BeneficiaryStatus.ACTIVE
    )

    # 3. Paiements effectivement reçus dans la période
    paid_benefits = BenefitConsumption.objects.filter(
        individual__in=[b.individual for b in beneficiaries],
        status__in=[
            BenefitConsumptionStatus.ACCEPTED,
            BenefitConsumptionStatus.RECONCILED
        ],
        date_due__range=(start, end)
    )

    # 4. Nombre distinct de bénéficiaires payés
    count = paid_benefits.values(
        "individual_id"
    ).distinct().count()

    # 5. Enregistrement (cumulatif)
    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=count,
        source="Payroll / Social Protection"
    )


def calc_ODP_003(indicator, start, end):
    """
    ODP_TMU_002 – % femmes bénéficiaires des TMU
    """

    # 1. Identifier les plans TMU (Composante 1)
    tmu_plans = BenefitPlan.objects.filter(
        code__icontains="TMU"
    )

    # 2. Bénéficiaires actifs de ces plans
    beneficiaries = Beneficiary.objects.filter(
        benefit_plan__in=tmu_plans,
        status=BeneficiaryStatus.ACTIVE
    )

    # 3. Paiements effectivement reçus dans la période
    paid_benefits = BenefitConsumption.objects.filter(
        individual__in=[b.individual for b in beneficiaries],
        status__in=[
            BenefitConsumptionStatus.ACCEPTED,
            BenefitConsumptionStatus.RECONCILED
        ],
        date_due__range=(start, end)
    )

    # 4. Dénominateur : total bénéficiaires payés TMU
    total_paid = paid_benefits.values(
        "individual_id"
    ).distinct().count()

    if total_paid == 0:
        value = 0
    else:
        # 5. Numérateur : femmes bénéficiaires payées
        women_paid = paid_benefits.filter(
            individual__json_ext__sexe_bp="F"
        ).values(
            "benefit__individual_id"
        ).distinct().count()

        value = round((women_paid / total_paid) * 100, 2)

    # 6. Sauvegarde de la valeur
    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=value,
        gender="F",
        source="Payroll / Social Protection"
    )


def calc_ODP_004(indicator, start, end):
    """
    ODP / Composante 2
    Nombre total de ménages ayant reçu des transferts monétaires réguliers (TMR)

    Indicateur cumulatif :
    - Comptage des bénéficiaires uniques
    - Ayant effectivement reçu au moins un paiement TMR sur la période
    """

    # 1. Identifier les bénéficiaires TMR actifs
    beneficiaries_qs = Beneficiary.objects.filter(
        benefit_plan__code="TMR",
        status=BeneficiaryStatus.ACTIVE,
        is_deleted=False,
    ).values_list("individual_id", flat=True)

    if not beneficiaries_qs.exists():
        _save_value(indicator, start, end, 0)
        return

    # 2. Vérifier qu’ils ont reçu au moins un paiement sur la période
    paid_individuals = (
        BenefitConsumption.objects.filter(
            individual_id__in=beneficiaries_qs,
            date_due__range=(start, end),
            status__in=[
                BenefitConsumptionStatus.ACCEPTED,
                BenefitConsumptionStatus.RECONCILED,
            ],
            is_deleted=False,
        )
        .values("individual_id")
        .distinct()
        .count()
    )

    # 3. Sauvegarde (cumulatif)
    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=paid_individuals,
        source="Payroll / Social Protection",
    )


def calc_ODP_005(indicator, start, end):
    """
    ODP_005 – % de femmes bénéficiaires des transferts monétaires réguliers (TMR)

    Formule :
    (Femmes bénéficiaires TMR ayant reçu un paiement / Total bénéficiaires TMR payés) * 100
    """

    # 1. Bénéficiaires TMR actifs
    beneficiaries_qs = Beneficiary.objects.filter(
        benefit_plan__code="TMR",
        status=BeneficiaryStatus.ACTIVE,
        is_deleted=False,
    ).values_list("individual_id", flat=True)

    if not beneficiaries_qs.exists():
        _save_value(indicator, start, end, 0)
        return

    # 2. Bénéficiaires ayant effectivement reçu un TMR sur la période
    paid_individuals_qs = BenefitConsumption.objects.filter(
        individual_id__in=beneficiaries_qs,
        date_due__range=(start, end),
        status__in=[
            BenefitConsumptionStatus.ACCEPTED,
            BenefitConsumptionStatus.RECONCILED,
        ],
        is_deleted=False,
    ).values("individual_id").distinct()

    total_paid = paid_individuals_qs.count()

    if total_paid == 0:
        _save_value(indicator, start, end, 0)
        return

    # 3. Femmes parmi les bénéficiaires payés
    women_paid = Individual.objects.filter(
        id__in=paid_individuals_qs,
        json_ext__sexe_bp="F"
    ).count()

    # 4. Pourcentage
    percentage = round((women_paid / total_paid) * 100, 2)

    # 5. Sauvegarde
    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=percentage,
        gender="F",
        source="Payroll / Social Protection",
    )


def calc_ODP_006(indicator, start, end):
    """
    ODP_006 – Nombre total de bénéficiaires directs et indirects des filets sociaux

    Directs   : bénéficiaires enregistrés
    Indirects : membres du ménage (n_membres - 1)
    """

    # 1. Bénéficiaires directs actifs
    beneficiaries_qs = Beneficiary.objects.filter(
        status=BeneficiaryStatus.ACTIVE,
        is_deleted=False,
        date_created__lte=end,  # cumulatif jusqu'à la période
    ).select_related("individual")

    if not beneficiaries_qs.exists():
        _save_value(indicator, start, end, 0)
        return

    total_direct = beneficiaries_qs.count()

    # 2. Calcul des bénéficiaires indirects
    total_indirect = 0

    for ben in beneficiaries_qs:
        individual = ben.individual
        household_size = individual.json_ext.get("n_membres", 1)

        try:
            household_size = int(household_size)
        except (TypeError, ValueError):
            household_size = 1

        # On enlève le bénéficiaire direct lui-même
        indirect = max(household_size - 1, 0)
        total_indirect += indirect

    # 3. Total global
    total_beneficiaries = total_direct + total_indirect

    # 4. Sauvegarde
    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=total_beneficiaries,
        source="Social Protection / Individual",
    )
