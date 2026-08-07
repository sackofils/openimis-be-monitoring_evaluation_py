from ..formula_utils import save_indicator_value as _save_value
from ..models import MonitoringSubmission

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
