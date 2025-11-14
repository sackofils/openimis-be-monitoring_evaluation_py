import uuid
from django.db import models
from django.contrib.postgres.indexes import GinIndex
from django.utils.translation import gettext_lazy as _
from django.contrib.postgres.search import SearchVectorField
from core.models import HistoryBusinessModel, HistoryModel, User


class Indicator(HistoryBusinessModel):
    """
    Représente un indicateur du cadre de résultats du projet.
    Exemple : 'Nombre de ménages bénéficiaires des transferts monétaires'.
    """

    class Frequency(models.TextChoices):
        MONTHLY = "M", _("Mensuel")
        QUARTERLY = "T", _("Trimestriel")
        SEMIANNUAL = "S", _("Semestriel")
        ANNUAL = "A", _("Annuel")

    code = models.CharField(_("Code"), max_length=50, unique=True)
    name = models.CharField(_("Nom de l’indicateur"), max_length=255)
    description = models.TextField(_("Description / Définition"), blank=True, null=True)

    unit = models.CharField(_("Unité de mesure"), max_length=50, help_text=_("Ex: Nombre, %, Oui/Non"))
    frequency = models.CharField(
        _("Fréquence de suivi"),
        max_length=1,
        choices=Frequency.choices,
        default=Frequency.QUARTERLY,
    )
    disaggregation = models.CharField(
        _("Ventilation"),
        max_length=255,
        blank=True,
        null=True,
        help_text=_("Ex: Par sexe, par région, etc."),
    )

    data_source = models.CharField(_("Source des données"), max_length=255, blank=True, null=True)
    collection_method = models.TextField(_("Méthodologie de collecte"), blank=True, null=True)
    responsible = models.CharField(_("Responsable de la collecte"), max_length=255, blank=True, null=True)
    calculation_method = models.TextField(_("Méthode de calcul"), blank=True, null=True)
    quality_review = models.TextField(_("Processus d’examen de la qualité"), blank=True, null=True)
    budget_notes = models.TextField(_("Budget collecte/analyse"), blank=True, null=True)
    formula_key = models.CharField(
        _("Clé de calcul automatique"), max_length=50, blank=True, null=True,
        help_text=_("Identifiant logique de la formule (ex : ODP_002, IRI_001)")
    )

    is_automatic = models.BooleanField(
        _("Calcul automatique"),
        default=False,
        help_text=_("Indique si l’indicateur est calculé automatiquement à partir des données sources"),
    )
    is_active = models.BooleanField(_("Actif"), default=True)

    # Traçabilité
    created_by = models.ForeignKey(
        User, related_name="me_indicator_created", null=True, blank=True, on_delete=models.SET_NULL
    )
    updated_by = models.ForeignKey(
        User, related_name="me_indicator_updated", null=True, blank=True, on_delete=models.SET_NULL
    )

    class Meta:
        verbose_name = _("Indicateur de suivi-évaluation")
        verbose_name_plural = _("Indicateurs de suivi-évaluation")
        ordering = ("code",)

    def __str__(self):
        return f"{self.code} - {self.name}"


class IndicatorValue(HistoryModel):
    """
    Valeurs observées ou calculées pour un indicateur donné, sur une période donnée.
    Exemple : IND2 = 1520 ménages payés au T2 2025 dans la région de Sikasso.
    """

    indicator = models.ForeignKey(
        Indicator,
        on_delete=models.CASCADE,
        related_name="values",
        verbose_name=_("Indicateur"),
    )

    # Période concernée
    period_start = models.DateField(_("Début de période"))
    period_end = models.DateField(_("Fin de période"))

    # Dimensions de ventilation
    region_code = models.CharField(_("Code Région"), max_length=50, blank=True, null=True)
    gender = models.CharField(_("Sexe"), max_length=10, blank=True, null=True)

    # Valeurs
    value = models.FloatField(_("Valeur numérique"), null=True, blank=True)
    qualitative_value = models.CharField(_("Valeur qualitative"), max_length=100, blank=True, null=True)

    source = models.CharField(_("Source de données"), max_length=255, blank=True, null=True)
    validated = models.BooleanField(_("Validé"), default=False)

    # Traçabilité
    validated_by = models.ForeignKey(
        User, related_name="me_indicator_value_validated", null=True, blank=True, on_delete=models.SET_NULL
    )

    class Meta:
        verbose_name = _("Valeur d’indicateur")
        verbose_name_plural = _("Valeurs d’indicateur")
        constraints = [
            models.UniqueConstraint(
                fields=["indicator", "period_start", "period_end", "region_code", "gender"],
                name="unique_indicator_value_period_region_gender",
            )
        ]

    def __str__(self):
        val = self.value if self.value is not None else self.qualitative_value
        return f"{self.indicator.code} [{self.period_start}→{self.period_end}] = {val}"


class MonitoringLog(HistoryModel):
    """
    Journal des recalculs d'indicateurs pour le suivi-évaluation.
    Chaque entrée correspond à une exécution de calculate_indicators_for_period.
    """

    period_start = models.DateField(_("Début de période"))
    period_end = models.DateField(_("Fin de période"))
    executed_at = models.DateTimeField(_("Date d’exécution"), auto_now_add=True)
    executed_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="me_logs_executed",
        verbose_name=_("Exécuté par"),
    )

    indicators_count = models.PositiveIntegerField(_("Nombre d’indicateurs recalculés"), default=0)
    success = models.BooleanField(_("Succès"), default=True)
    error_details = models.TextField(_("Détails d’erreurs"), blank=True, null=True)

    class Meta:
        verbose_name = _("Journal de recalcul d’indicateurs")
        verbose_name_plural = _("Journaux de recalcul d’indicateurs")
        ordering = ("-executed_at",)

    def __str__(self):
        status = "success" if self.success else "failed"
        return f"{status} {self.period_start} → {self.period_end} ({self.indicators_count} indicateurs)"

class MonitoringSubmission(HistoryModel):
    FORM_TYPES = [
        ("TMU_TMR", "Transferts Monétaires (Urgence/Régulier)"),
        ("SERE_NAFA", "Sensibilisation à la Résilience (SERE NAFA)"),
        ("AGR", "Activité Génératrice de Revenus"),
        ("SUBVENTION_BENEF", "Subvention Bénéficiaires"),
    ]
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    form_type = models.CharField(max_length=50, choices=FORM_TYPES)
    submission_uuid = models.CharField(max_length=255, unique=True, null=True, blank=True)  # _uuid Kobo
    submitted_at = models.DateTimeField(null=True, blank=True)

    # Références “pivot” analytiques
    beneficiary = models.ForeignKey("individual.Individual", on_delete=models.SET_NULL, null=True, blank=True)
    location = models.ForeignKey("location.Location", on_delete=models.SET_NULL, null=True, blank=True)
    # project = models.ForeignKey("core.Project", on_delete=models.SET_NULL, null=True, blank=True)
    enumerator = models.ForeignKey("core.User", on_delete=models.SET_NULL, null=True, blank=True)

    # Période libre (YYYY, YYYY-Qn, YYYY-MM, YYYY-Wnn), à normaliser côté sync
    period = models.CharField(max_length=20, null=True, blank=True)

    # Données natives Kobo (clé→valeur)
    json_ext = models.JSONField(default=dict, blank=True)

    # Index texte optionnel (si vous voulez chercher par mot-clé)
    search_vector = SearchVectorField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["form_type"]),
            models.Index(fields=["period"]),
            models.Index(fields=["beneficiary"]),
            models.Index(fields=["location"]),
            GinIndex(fields=["json_ext"]),
        ]
        ordering = ["-submitted_at", "-created_at"]

    def __str__(self):
        return f"{self.form_type} | {self.submission_uuid or self.id}"

