from django.contrib import admin, messages
from django.utils.translation import gettext_lazy as _
from datetime import date

from .models import Indicator, IndicatorValue, MonitoringLog
from .services import calculate_indicators_for_period


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
                else calculate_indicators_for_period(start, end, ctx={"user": user})
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
