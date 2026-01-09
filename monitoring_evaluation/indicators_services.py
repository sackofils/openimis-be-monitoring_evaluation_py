import json
import logging
from datetime import datetime, timedelta
from django.apps import apps
from django.db.models import Sum, Q
from django.db import transaction
from .models import Indicator, IndicatorValue, MonitoringSubmission, MonitoringLog
from payroll.models import Payroll, PayrollBenefitConsumption, BenefitConsumption, BenefitConsumptionStatus
from social_protection.models import Beneficiary, BenefitPlan, BeneficiaryStatus
from grievance_social_protection.models import Ticket


logger = logging.getLogger(__name__)


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

# --------------------------------------------------------------------
# Utilitaire d’enregistrement de valeur
# --------------------------------------------------------------------

def _save_value(indicator, start, end, value, region=None, gender=None, source="SYSTEM"):
    """
    Crée ou met à jour une valeur d’indicateur pour une période donnée.
    """
    indicatorvalue = IndicatorValue.objects.filter(
        indicator=indicator,
        period_start=start,
        period_end=end,
        region_code=region,
        gender=gender,
    ).first()

    if not indicatorvalue:
        indicatorvalue = IndicatorValue(
            indicator=indicator,
            period_start=start,
            period_end=end,
            region_code=region,
            gender=gender,
            value=value,
            source=source,
            validated=True,
        )
        indicatorvalue.save(username="Admin")
        logger.info(
            f"[ME] {indicator.code}: créé ({start}→{end}) = {value}"
        )
        return

    # Détection de changement réel
    has_change = False

    if indicatorvalue.value != value:
        indicatorvalue.value = value
        has_change = True

    if indicatorvalue.source != source:
        indicatorvalue.source = source
        has_change = True

    if not indicatorvalue.validated:
        indicatorvalue.validated = True
        has_change = True

    if has_change:
        indicatorvalue.save(username="Admin")
        logger.info(
            f"[ME] {indicator.code}: mis à jour ({start}→{end}) = {value}"
        )
    else:
        logger.debug(
            f"[ME] {indicator.code}: aucune modification ({start}→{end})"
        )


def _safe_percent(num, den):
    return round((num / den) * 100, 2) if den else 0.0


def compute_indicator_from_datasource(indicator, start, end):
    ds = indicator.data_source
    if not ds or not ds.is_active:
        return

    Model = apps.get_model(ds.module, ds.model)

    qs = Model.objects.filter(**{
        f"{ds.date_field}__range": (start, end)
    })

    if ds.filters:
        qs = qs.filter(**ds.filters)

    if ds.aggregation == "COUNT":
        value = qs.count()

    elif ds.aggregation == "COUNT_DISTINCT":
        value = qs.values(ds.distinct_field).distinct().count()

    elif ds.aggregation == "SUM":
        value = qs.aggregate(
            total=Sum(ds.value_field)
        )["total"] or 0

    elif ds.aggregation == "PERCENT":
        num = qs.filter(**(ds.numerator_filters or {})) \
                .values(ds.distinct_field).distinct().count()

        den = qs.filter(**(ds.denominator_filters or {})) \
                .values(ds.distinct_field).distinct().count()

        value = _safe_percent(num, den)

    else:
        raise ValueError(f"Aggregation inconnue: {ds.aggregation}")

    _save_value(indicator, start, end, value)

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

def calc_PIP_011(indicator, start, end):
    """
    PIP_011 – Nombre de bénéficiaires enregistrés au PIP

    Règle :
    - Comptage des bénéficiaires uniques
    - Ayant une fiche d’enregistrement valide
    - Source : FICHE_ENREG_BENEFICIAIRE
    - Indicateur cumulatif
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_ENREG_BENEFICIAIRE"
        #submitted_at__lte=end,   # cumulatif jusqu'à la période
    )

    if not qs.exists():
        _save_value(indicator, start, end, 0, source="Fiche d’enregistrement")
        return

    # --- Cas 1 : bénéficiaire relationnel ---
    # --- Cas 2 : bénéficiaire dans le JSON Kobo ---
    count = (
        qs.exclude(
            json_ext__groupe_ben__groupe_ajoute_preload__code_menage__isnull=True
        )
        .values(
            "json_ext__groupe_ben__groupe_ajoute_preload__code_menage"
        )
        .distinct()
        .count()
    )

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=count,
        source="Fiche d’enregistrement des bénéficiaires",
    )

def calc_PIP_013(indicator, start, end):
    """
    ODP_SERE_001 – Nombre de Sèrès Nafa mis en place

    Source : Fiche de constitution des Sèrès Nafa
    Formulaire : CONSTITUTION_SERE_NAFA
    Indicateur cumulatif
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="CONSTITUTION_SERE_NAFA",
        submitted_at__lte=end,   # cumulatif jusqu'à la période
    )

    if not qs.exists():
        _save_value(
            indicator=indicator,
            start=start,
            end=end,
            value=0,
            source="Fiche de constitution des Sèrès Nafa",
        )
        return

    # Identifiant unique du Sèrè Nafa (code_sere)
    count = (
        qs
        .distinct()
        .count()
    )

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=count,
        source="Fiche de constitution des Sèrès Nafa",
    )

def calc_PIP_014(indicator, start, end):
    """
    ODP_SERE_002 – Taux de Sèrè Nafa fonctionnant de manière satisfaisante

    Source : Fiche de suivi des Sèrès Nafa
    Fréquence : Mensuelle
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA"
        # submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(indicator, start, end, 0, source="Fiche de suivi des Sèrès Nafa")
        return

    # -------------------------------
    # DÉNOMINATEUR : Sèrè Nafa suivis
    # -------------------------------
    total_sere = (
        qs
        .distinct()
        .count()
    )

    if total_sere == 0:
        _save_value(indicator, start, end, 0, source="Fiche de suivi des Sèrès Nafa")
        return

    # ------------------------------------------------
    # NUMÉRATEUR : Sèrè Nafa fonctionnant correctement
    # ------------------------------------------------
    functioning_sere = (
        qs.filter(
            # Critère 1 : règlement intérieur respecté
            json_ext__reglement_sere__reglementInterieur__icontains=["Oui"],
            # Critère 2 : participation effective
            json_ext__groupe_presence__nbre_homme__gt=0,
        )
        .distinct()
        .count()
    )

    # -------------------------------
    # TAUX (%)
    # -------------------------------
    taux = round((functioning_sere / total_sere) * 100, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=taux,
        source="Fiche de suivi des Sèrès Nafa",
    )

def calc_PIP_015(indicator, start, end):
    """
    ODP_SERE_003 – Épargne moyenne collectée par membre des Sèrès Nafa

    Source : Fiche de suivi des Sèrès Nafa
    Fréquence : Mensuelle
    Unité : GNF
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA"
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(indicator, start, end, 0, source="Fiche de suivi des Sèrès Nafa")
        return

    total_epargne = 0
    total_membres = 0

    for sub in qs:
        epargne = sub.json_ext.get("groupe_epargne", {}).get(
            "montant_total_epargne", 0
        )
        membres = sub.json_ext.get(
            "groupe_identite", {}
        ).get(
            "groupe_ajoute_preload", {}
        ).get("sere_nbre", 0)

        try:
            epargne = float(epargne)
        except (TypeError, ValueError):
            epargne = 0

        try:
            membres = int(float(membres))
        except (TypeError, ValueError):
            membres = 0

        if membres > 0:
            total_epargne += epargne
            total_membres += membres

    if total_membres == 0:
        value = 0
    else:
        value = round(total_epargne / total_membres, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=value,
        source="Fiche de suivi des Sèrès Nafa",
    )

def calc_PIP_016(indicator, start, end):
    """
    ODP_SERE_004 – Épargne cumulée par Sèrè Nafa

    Formule :
    Épargne cumulée =
    (Nombre de membres × 80 %) ×
    Montant d’une part par semaine ×
    Nombre de semaines (cycle de 9 mois = 36)

    Fréquence : Trimestrielle
    Source : Fiche de suivi des Sèrès Nafa
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA"
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(
            indicator=indicator,
            start=start,
            end=end,
            value=0,
            source="Fiche de suivi des Sèrès Nafa",
        )
        return

    TOTAL_WEEKS = 36
    PARTICIPATION_RATE = 0.8

    total_epargne_cumulee = 0

    for sub in qs:
        # Nombre de membres du Sèrè Nafa
        membres = sub.json_ext.get(
            "groupe_identite", {}
        ).get(
            "groupe_ajoute_preload", {}
        ).get("sere_nbre", 0)

        # Montant d'une part par semaine
        valeur_part = sub.json_ext.get(
            "groupe_epargne", {}
        ).get("valeur_epargne", 0)

        try:
            membres = int(float(membres))
        except (TypeError, ValueError):
            membres = 0

        try:
            valeur_part = float(valeur_part)
        except (TypeError, ValueError):
            valeur_part = 0

        if membres <= 0 or valeur_part <= 0:
            continue

        epargne_groupe = (
            membres
            * PARTICIPATION_RATE
            * valeur_part
            * TOTAL_WEEKS
        )

        total_epargne_cumulee += epargne_groupe

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=round(total_epargne_cumulee, 2),
        source="Fiche de suivi des Sèrès Nafa",
    )

def calc_PIP_017(indicator, start, end):
    """
    ODP_SERE_005 – Montant du crédit accordé aux membres des Sèrès Nafa

    Formule :
    Montant du crédit = Montant total de l’épargne × 1,5

    Fréquence : Mensuelle
    Source : Fiche de suivi des Sèrès Nafa
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA"
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(
            indicator=indicator,
            start=start,
            end=end,
            value=0,
            source="Fiche de suivi des Sèrès Nafa",
        )
        return

    CREDIT_MULTIPLIER = 1.5
    total_credit = 0

    for sub in qs:
        montant_epargne = sub.json_ext.get(
            "groupe_epargne", {}
        ).get("montant_total_epargne", 0)

        try:
            montant_epargne = float(montant_epargne)
        except (TypeError, ValueError):
            montant_epargne = 0

        if montant_epargne <= 0:
            continue

        credit_groupe = montant_epargne * CREDIT_MULTIPLIER
        total_credit += credit_groupe

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=round(total_credit, 2),
        source="Fiche de suivi des Sèrès Nafa",
    )


def calc_PIP_018(indicator, start, end):
    """
    ODP_SERE_006 – Taux de bénéficiaires ayant contracté un crédit
    au moins une fois au cours du cycle de 9 mois

    Fréquence : Trimestrielle
    Source : Fiche de suivi des Sèrès Nafa
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA"
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(indicator, start, end, 0, source="Fiche de suivi des Sèrès Nafa")
        return

    total_beneficiaires = 0
    beneficiaires_ayant_credit = 0

    for sub in qs:
        # Nombre total de membres
        membres = sub.json_ext.get(
            "groupe_identite", {}
        ).get(
            "groupe_ajoute_preload", {}
        ).get("sere_nbre", 0)

        # Nombre de crédits en cours
        nb_credits = sub.json_ext.get(
            "groupe_epargne", {}
        ).get("nb_credit_en_cours", 0)

        try:
            membres = int(float(membres))
        except (TypeError, ValueError):
            membres = 0

        try:
            nb_credits = int(float(nb_credits))
        except (TypeError, ValueError):
            nb_credits = 0

        if membres <= 0:
            continue

        total_beneficiaires += membres

        # Si au moins un crédit existe → au moins un bénéficiaire a eu accès
        if nb_credits > 0:
            beneficiaires_ayant_credit += membres

    if total_beneficiaires == 0:
        value = 0
    else:
        value = round((beneficiaires_ayant_credit / total_beneficiaires) * 100, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=value,
        source="Fiche de suivi des Sèrès Nafa",
    )


FORMULAS = {
    "IRI_012": calc_IRI_012,
    "ODP_002": calc_ODP_002,
    "ODP_003": calc_ODP_003,
    "ODP_004": calc_ODP_004,
    "ODP_005": calc_ODP_005,
    "ODP_006": calc_ODP_006,
    "PIP_11": calc_PIP_011,
    "PIP_13": calc_PIP_013,
    "PIP_14": calc_PIP_014,
    "PIP_15": calc_PIP_015,
    "PIP_16": calc_PIP_016,
    "PIP_17": calc_PIP_017,
    "PIP_18": calc_PIP_018,
}

@transaction.atomic
def calculate_me_indicators_for_period(start, end, user=None):
    indicators = Indicator.objects.filter(
        is_active=True,
        method='AUTOMATIQUE',
        data_source__is_active=True
    )

    computed = 0
    errors = []

    for ind in indicators:
        try:
            compute_indicator_from_datasource(ind, start, end)
            computed += 1
        except Exception as e:
            msg = f"{ind.code}: {e}"
            errors.append(msg)
            logger.error(f"[ME] {msg}")

    indicators = Indicator.objects.filter(is_active=True, method='AUTOMATIQUE', formula__isnull=False)
    for ind in indicators:
        try:
            fn = FORMULAS.get(ind.formula)
            if fn:
                fn(ind, start, end)
                computed += 1
        except Exception as e:
            msg = f"{ind.code}: {e}"
            errors.append(msg)
            logger.error(f"[ME] {msg}")


    monitoring = MonitoringLog(
        period_start=start,
        period_end=end,
        indicators_count=computed,
        success=(len(errors) == 0),
        error_details="\n".join(errors) if errors else None,
        executed_by=user,
    )
    monitoring.save(user=user)

    return computed
