import graphene
import graphene_django_optimizer as gql_optimizer
from django.utils.translation import gettext_lazy as _

from core.schema import OrderedDjangoFilterConnectionField
from django.contrib.auth.models import AnonymousUser
from core.utils import append_validity_filter
from django.core.exceptions import PermissionDenied
import graphene_django_optimizer as gql_optimizer

from .apps import MODULE_NAME

# Import des queries du module
from .gql_queries import *

# Import des mutations du module
from .gql_mutations import (
    CreateIndicatorMutation,
    UpdateIndicatorMutation,
    CreateManualIndicatorValueMutation,
    UpdateManualIndicatorValueMutation,
    DeleteManualIndicatorValueMutation,
    ValidateManualIndicatorValueMutation,
    RecalculateIndicatorsMutation,
    DeleteIndicatorMutation,
    DuplicateIndicatorMutation
)

class Query(graphene.ObjectType):
    """
    Déclaration des requêtes GraphQL du module Suivi-Évaluation
    """

    # === Requêtes principales ===
    indicators = OrderedDjangoFilterConnectionField(
        IndicatorGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        # is_active=graphene.Boolean(),
        search=graphene.String(description="Recherche plein texte (nom, code, description)"),
    )

    indicator_values = OrderedDjangoFilterConnectionField(
        IndicatorValueGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        #indicator_code=graphene.String(),
        #region_code=graphene.String(),
        #gender=graphene.String(),
        #validated=graphene.Boolean(),
        #start_date=graphene.String(),
        #end_date=graphene.String(),
    )

    monitoring_logs = OrderedDjangoFilterConnectionField(
        MonitoringLogGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        #success=graphene.Boolean(),
    )

    # === Résolveurs ===

    def resolve_indicators(self, info, **kwargs):
        user = info.context.user
        if not user.is_authenticated:
            raise PermissionDenied(_("Vous devez être connecté."))

        qs = Indicator.objects.all()

        # filtre "actif"
        if "is_active" in kwargs:
            qs = qs.filter(is_active=kwargs["is_active"])

        qs = qs.filter(is_deleted=False)

        # recherche plein texte
        search = kwargs.get("search")
        if search:
            qs = qs.filter(
                models.Q(name__icontains=search)
                | models.Q(description__icontains=search)
                | models.Q(code__icontains=search)
            )

        return gql_optimizer.query(qs.order_by("code"), info)

    def resolve_indicator_values(self, info, **kwargs):
        user = info.context.user
        if not user.is_authenticated:
            raise PermissionDenied(_("Authentification requise."))

        qs = IndicatorValue.objects.select_related("indicator")

        qs = qs.filter(is_deleted=False)

        if "indicator_code" in kwargs:
            qs = qs.filter(indicator__code=kwargs["indicator_code"])

        if "region_code" in kwargs:
            qs = qs.filter(region_code=kwargs["region_code"])

        if "gender" in kwargs:
            qs = qs.filter(gender=kwargs["gender"])

        if "validated" in kwargs:
            qs = qs.filter(validated=kwargs["validated"])

        if "start_date" in kwargs:
            qs = qs.filter(period_start__gte=kwargs["start_date"])

        if "end_date" in kwargs:
            qs = qs.filter(period_end__lte=kwargs["end_date"])

        return gql_optimizer.query(qs.order_by("-period_start", "indicator__code"), info)

    def resolve_monitoring_logs(self, info, **kwargs):
        user = info.context.user
        if not user.is_authenticated:
            raise PermissionDenied(_("Authentification requise."))

        qs = MonitoringLog.objects.select_related("executed_by")
        if "success" in kwargs:
            qs = qs.filter(success=kwargs["success"])
        return qs.order_by("-executed_at")


class Mutation(graphene.ObjectType):
    """
    Mutations GraphQL du module Suivi-Évaluation
    """
    create_indicator = CreateIndicatorMutation.Field()
    update_indicator = UpdateIndicatorMutation.Field()
    delete_indicator = DeleteIndicatorMutation.Field()
    duplicate_indicator = DuplicateIndicatorMutation.Field()
    create_manual_indicator_value = CreateManualIndicatorValueMutation.Field()
    update_manual_indicator_value = UpdateManualIndicatorValueMutation.Field()
    delete_manual_indicator_value = DeleteManualIndicatorValueMutation.Field()
    validate_manual_indicator_value = ValidateManualIndicatorValueMutation.Field()
    recalculate_indicators = RecalculateIndicatorsMutation.Field()
