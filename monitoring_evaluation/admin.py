from django.contrib import admin, messages
from django.utils.translation import gettext_lazy as _
from datetime import date
from django_json_widget.widgets import JSONEditorWidget
from django.db import models

from .models import Indicator, IndicatorValue, MonitoringLog, IndicatorDataSource
from .indicators_services import calculate_me_indicators_for_period

@admin.register(IndicatorDataSource)
class IndicatorDataSourceAdmin(admin.ModelAdmin):
    list_display = (
        "indicator",
        "module",
        "model",
        "aggregation",
        "date_field",
        "is_active",
    )

    list_filter = (
        "aggregation",
        "module",
        "is_active",
        "indicator__category",
    )

    search_fields = (
        "indicator__code",
        "indicator__name",
        "module",
        "model",
    )

    autocomplete_fields = ("indicator",)

    readonly_fields = ()

    fieldsets = (
        ("Indicateur", {
            "fields": ("indicator", "is_active")
        }),
        ("Source de données", {
            "fields": ("module", "model", "date_field")
        }),
        ("Méthode de calcul", {
            "fields": (
                "aggregation",
                "value_field",
                "distinct_field",
            )
        }),
        ("Filtres globaux", {
            "fields": ("filters",),
            "description": "Filtres Django ORM (JSON)"
        }),
        ("Calcul de pourcentage", {
            "fields": (
                "numerator_filters",
                "denominator_filters",
            ),
            "description": "Uniquement si aggregation = PERCENT"
        }),
    )

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)



@admin.action(description=_("Recalculer les indicateurs automatiques"))
def recalculate_indicators(modeladmin, request, queryset):
    """
    Action admin pour recalculer les indicateurs automatiques.
    - Si aucun indicateur n'est sélectionné : tous les indicateurs automatiques actifs.
    - Sinon, uniquement ceux sélectionnés.
    """
    user = request.user
    today = date.today()
    start_month = ((today.month - 1) // 3) * 3 + 1
    start = date(today.year, start_month, 1)
    end = today

    if queryset.exists():
        indicators = queryset.filter(is_automatic=True, is_active=True)
    else:
        indicators = Indicator.objects.filter(is_automatic=True, is_active=True)

    if not indicators.exists():
        messages.warning(request, _("Aucun indicateur automatique à recalculer."))
        return

    count = 0
    errors = []

    for ind in indicators:
        try:
            defaults = ind.compute_value(start, end, user=user) if hasattr(ind, "compute_value") \
                else calculate_me_indicators_for_period(start, end, ctx={"user": user})
            count += 1
        except Exception as e:
            errors.append(f"{ind.code}: {e}")

    MonitoringLog.objects.create(
        period_start=start,
        period_end=end,
        executed_by=user,
        indicators_count=count,
        success=len(errors) == 0,
        error_details="\n".join(errors) if errors else None,
    )

    if errors:
        messages.error(
            request,
            _("Certains indicateurs ont échoué : ") + ", ".join(errors),
        )
    else:
        messages.success(
            request,
            _(f"{count} indicateurs recalculés avec succès pour la période {start} → {end}."),
        )


@admin.register(Indicator)
class IndicatorAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "unit", "frequency", "is_automatic", "is_active")
    list_filter = ("frequency", "is_automatic", "is_active")
    search_fields = ("code", "name")
    actions = [recalculate_indicators]

    def datasource_link(self, obj):
        if hasattr(obj, "data_source"):
            return f"{obj.data_source.aggregation}"
        return "Non configuré"

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)


@admin.register(IndicatorValue)
class IndicatorValueAdmin(admin.ModelAdmin):
    list_display = (
        "indicator",
        "period_start",
        "period_end",
        "region_code",
        "gender",
        "value",
        "validated",
    )
    list_filter = ("indicator", "region_code", "validated")
    search_fields = ("indicator__code", "indicator__name")

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)


@admin.register(MonitoringLog)
class MonitoringLogAdmin(admin.ModelAdmin):
    list_display = (
        "executed_at",
        "period_start",
        "period_end",
        "executed_by",
        "indicators_count",
        "success",
    )
    list_filter = ("success", "executed_by")
    search_fields = ("executed_by__username",)
    readonly_fields = ("executed_at", "error_details")
