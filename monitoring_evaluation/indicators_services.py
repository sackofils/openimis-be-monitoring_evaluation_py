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
    PIP_014 – Taux de Sèrè Nafa fonctionnant de manière satisfaisante

    Définition stricte d’un Sèrè Nafa "satisfaisant" :

    1. applicationReglementInterieur = Oui
    2. montant_total_epargne > 0
    3. taux_remboursement >= 70%

    Où :
        taux_remboursement =
            montant_rembourcement / montant_fond_credis

    Formule de l’indicateur :
        (Nombre groupes satisfaisants / Nombre groupes suivis) × 100

    Source : FICHE_SUIVI_SERE_NAFA
    Fréquence : Mensuelle
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA",
        #submitted_at__range=(start, end),
    )

    # Ensemble des groupes distincts suivis
    groupes_total = set()

    # Ensemble des groupes satisfaisants
    groupes_satisfaisants = set()

    for sub in qs:
        data = sub.json_ext

        # ==============================
        # 1. Identification du groupe
        # ==============================
        groupe_identite = data.get("groupe_identite", {})
        preload = groupe_identite.get("groupe_ajoute_preload", {})

        code_sere = preload.get("sequence_code_sere")

        if not code_sere:
            continue

        code_sere = str(code_sere).strip().upper()
        groupes_total.add(code_sere)

        # ==============================
        # 2. Critère 1 – Règlement appliqué
        # ==============================
        reglement = data.get("reglement_sere", {})
        application = reglement.get("applicationReglementInterieur", [])

        if isinstance(application, str):
            application = [application]

        reglement_ok = "Oui" in application

        # ==============================
        # 3. Critère 2 – Épargne positive
        # ==============================
        groupe_epargne = data.get("groupe_epargne", {})

        montant_total = groupe_epargne.get("montant_total_epargne", 0)

        try:
            montant_total = float(montant_total)
        except (TypeError, ValueError):
            montant_total = 0

        epargne_ok = montant_total > 0

        # ==============================
        # 4. Critère 3 – Taux remboursement >= 70%
        # ==============================
        montant_remboursement = groupe_epargne.get("montant_rembourcement", 0)
        montant_fond_credis = groupe_epargne.get("montant_fond_credis", 0)

        try:
            montant_remboursement = float(montant_remboursement)
        except (TypeError, ValueError):
            montant_remboursement = 0

        try:
            montant_fond_credis = float(montant_fond_credis)
        except (TypeError, ValueError):
            montant_fond_credis = 0

        if montant_fond_credis > 0:
            taux_remboursement = montant_remboursement / montant_fond_credis
        else:
            taux_remboursement = 0

        remboursement_ok = taux_remboursement >= 0.70

        # ==============================
        # 5. Condition finale stricte
        # ==============================
        if reglement_ok and epargne_ok and remboursement_ok:
            groupes_satisfaisants.add(code_sere)

    total = len(groupes_total)
    satisfaisants = len(groupes_satisfaisants)

    if total == 0:
        taux = 0
    else:
        taux = round((satisfaisants / total) * 100, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=taux,
        source="Fiche de suivi des Sèrè Nafa",
    )


def calc_PIP_015(indicator, start, end):
    """
    PIP_015 – Épargne moyenne collectée par membre des Sèrès Nafa

    Définition :
        Épargne moyenne = Somme totale épargne / Somme totale membres

    Où :
        montant_total_epargne → groupe_epargne.montant_total_epargne
        nombre_part → groupe_epargne.nombre_part

    Important :
        - On agrège toutes les épargnes
        - On agrège tous les membres
        - On calcule ensuite une moyenne globale pondérée

    Source : FICHE_SUIVI_SERE_NAFA
    Fréquence : Mensuelle
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA",
        # submitted_at__range=(start, end),
    )

    total_epargne = 0
    total_membres = 0

    for sub in qs:
        data = sub.json_ext
        groupe_epargne = data.get("groupe_epargne", {})

        montant_total = groupe_epargne.get("montant_total_epargne", 0)
        nombre_part = groupe_epargne.get("nombre_part", 0)

        # Conversion sécurisée
        try:
            montant_total = float(montant_total)
        except (TypeError, ValueError):
            montant_total = 0

        try:
            nombre_part = float(nombre_part)
        except (TypeError, ValueError):
            nombre_part = 0

        # On ne compte que les groupes valides
        if montant_total > 0 and nombre_part > 0:
            total_epargne += montant_total
            total_membres += nombre_part

    if total_membres == 0:
        moyenne = 0
    else:
        moyenne = round(total_epargne / total_membres, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=moyenne,
        source="Fiche de suivi des Sèrès Nafa",
    )


def calc_PIP_016(indicator, start, end):
    """
    PIP_016 – Épargne cumulée par Sèrè Nafa

    Définition :
        Somme des montants d’épargne réellement déclarés
        par les groupes Sèrè Nafa sur la période.

    Formule :
        PIP_016 = Somme(montant_total_epargne)

    Source : FICHE_SUIVI_SERE_NAFA
    Fréquence : Trimestrielle
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SUIVI_SERE_NAFA",
        # submitted_at__range=(start, end),
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

    total_epargne = 0

    for sub in qs:
        montant = (
            sub.json_ext
            .get("groupe_epargne", {})
            .get("montant_total_epargne", 0)
        )

        try:
            montant = float(montant)
        except (TypeError, ValueError):
            montant = 0

        if montant > 0:
            total_epargne += montant

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=round(total_epargne, 2),
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

def calc_PIP_025(indicator, start, end):
    """
    PIP_025 – Taux de bénéficiaires ayant diversifié
    ses moyens de subsistance

    Source : Fiche de suivi des sessions de coaching individuel
    Fréquence : Mensuelle

    Formule :
    (Nombre distinct bénéficiaires avec revenus_diversifie = Oui)
    /
    (Nombre distinct bénéficiaires total)
    × 100
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SESSIONS_COACHING_INDIVIDUEL",
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(
            indicator=indicator,
            start=start,
            end=end,
            value=0,
            source="Fiche de suivi des sessions de coaching individuel",
        )
        return

    total_beneficiaries = set()
    diversified_beneficiaries = set()

    for sub in qs:
        suivi_ind = sub.json_ext.get("suiviIndividuel", {})
        suivi_tech = sub.json_ext.get("suiviTechniqueProductive", {})

        code_menage = suivi_ind.get("codeMenage")

        if not code_menage:
            continue

        code_menage = str(code_menage).strip().upper()

        total_beneficiaries.add(code_menage)

        revenus_diversifie = suivi_tech.get("revenus_diversifie", [])

        if isinstance(revenus_diversifie, str):
            revenus_diversifie = [revenus_diversifie]

        if "Oui" in revenus_diversifie:
            diversified_beneficiaries.add(code_menage)

    total = len(total_beneficiaries)
    diversified = len(diversified_beneficiaries)

    if total == 0:
        taux = 0
    else:
        taux = round((diversified / total) * 100, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=taux,
        source="Fiche de suivi des sessions de coaching individuel",
    )


def calc_PIP_026(indicator, start, end):
    """
    PIP_026 – Nombre de bénéficiaires ayant bénéficié
    de séances de coaching individuel

    Fréquence : Mensuel
    Source : Fiche de suivi des sessions de coaching individuel

    Logique :
    Compte le nombre DISTINCT de codeMenage
    sur la période.
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SESSIONS_COACHING_INDIVIDUEL",
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(
            indicator=indicator,
            start=start,
            end=end,
            value=0,
            source="Fiche de suivi des sessions de coaching individuel",
        )
        return

    unique_beneficiaries = set()

    for sub in qs:
        suivi = sub.json_ext.get("suiviIndividuel", {})

        code_menage = suivi.get("codeMenage")

        if not code_menage:
            continue

        # Nettoyage robuste
        code_menage = str(code_menage).strip().upper()

        if code_menage:
            unique_beneficiaries.add(code_menage)

    total = len(unique_beneficiaries)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=total,
        source="Fiche de suivi des sessions de coaching individuel",
    )

def calc_PIP_029(indicator, start, end):
    """
    PIP_029 – Nombre de bénéficiaires ayant développé
    ou renforcé une AGR

    Source : Fiche de suivi des sessions de coaching individuel
    Fréquence : Mensuelle

    Logique :
    Compte le nombre DISTINCT de codeMenage
    ayant agr_creer = Oui OU agr_existant = Oui
    """

    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SESSIONS_COACHING_INDIVIDUEL",
        #submitted_at__range=(start, end),
    )

    if not qs.exists():
        _save_value(
            indicator=indicator,
            start=start,
            end=end,
            value=0,
            source="Fiche de suivi des sessions de coaching individuel",
        )
        return

    unique_beneficiaries = set()

    for sub in qs:
        suivi_ind = sub.json_ext.get("suiviIndividuel", {})
        suivi_tech = sub.json_ext.get("suiviTechniqueProductive", {})

        code_menage = suivi_ind.get("codeMenage")

        if not code_menage:
            continue

        # Nettoyage
        code_menage = str(code_menage).strip().upper()

        agr_creer = suivi_tech.get("agr_creer", [])
        agr_existant = suivi_tech.get("agr_existant", [])

        # Normalisation (au cas où ce n’est pas une liste)
        if isinstance(agr_creer, str):
            agr_creer = [agr_creer]

        if isinstance(agr_existant, str):
            agr_existant = [agr_existant]

        condition = (
            "Oui" in agr_creer
            or "Oui" in agr_existant
        )

        if condition:
            unique_beneficiaries.add(code_menage)

    total = len(unique_beneficiaries)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=total,
        source="Fiche de suivi des sessions de coaching individuel",
    )

def calc_PIP_028(indicator, start, end):
    """
    PIP_028 – Taux de bénéficiaires présents
    aux séances de communication de programme

    Formule :
    (Total bénéficiaires présents / Total bénéficiaires prévus) × 100


    qs = MonitoringSubmission.objects.filter(
        form_type="COMMUNICATION_PROGRAMME",
        #submitted_at__range=(start, end),
    )

    total_prevus = 0
    total_presents = 0

    for sub in qs:
        data = sub.json_ext

        prevus = data.get("participants_prevus", 0)
        presents = data.get("participants_total", 0)

        try:
            prevus = float(prevus)
        except (TypeError, ValueError):
            prevus = 0

        try:
            presents = float(presents)
        except (TypeError, ValueError):
            presents = 0

        total_prevus += prevus
        total_presents += presents

    if total_prevus == 0:
        taux = 0
    else:
        taux = round((total_presents / total_prevus) * 100, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=taux,
        source="Communication de Programme et Mobilisation Communautaire",
    )
    """
    pass

def calc_PIP_027(indicator, start, end):
    """
    PIP_027 – Taux de bénéficiaires satisfaits
    de l’accompagnement reçu

    Formule :
    (Nombre bénéficiaires satisfaits / Nombre bénéficiaires ayant répondu) × 100


    qs = MonitoringSubmission.objects.filter(
        form_type="FICHE_SESSIONS_COACHING_INDIVIDUEL",
        # submitted_at__range=(start, end),
    )

    total_respondents = set()
    satisfied_beneficiaries = set()

    for sub in qs:
        suivi_ind = sub.json_ext.get("suiviIndividuel", {})
        evaluation = sub.json_ext.get("evaluation", {})

        code_menage = suivi_ind.get("codeMenage")

        if not code_menage:
            continue

        code_menage = str(code_menage).strip().upper()

        satisfaction = evaluation.get("niveau_satisfaction", [])

        if isinstance(satisfaction, str):
            satisfaction = [satisfaction]

        if satisfaction:
            total_respondents.add(code_menage)

            if any(val in ["Satisfait", "Très satisfait", "Oui"] for val in satisfaction):
                satisfied_beneficiaries.add(code_menage)

    total = len(total_respondents)
    satisfied = len(satisfied_beneficiaries)

    if total == 0:
        taux = 0
    else:
        taux = round((satisfied / total) * 100, 2)

    _save_value(
        indicator=indicator,
        start=start,
        end=end,
        value=taux,
        source="Accompagnement – Coaching individuel/groupe",
    )
    """
    pass


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
    "PIP_26": calc_PIP_026,
    "PIP_27": calc_PIP_027,
    "PIP_28": calc_PIP_028,
    "PIP_29": calc_PIP_029,
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
