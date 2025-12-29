import graphene
from graphene_django import DjangoObjectType
from core import ExtendedConnection
from .models import Indicator, IndicatorValue, MonitoringLog



class IndicatorGQLType(DjangoObjectType):
    """
    Type GraphQL pour Indicator
    """

    # Champs GraphQL personnalisés
    values = graphene.List(
        lambda: IndicatorValueGQLType,
        description="Liste complète des valeurs de l’indicateur"
    )

    value = graphene.Field(
        lambda: IndicatorValueGQLType,
        description="Dernière valeur de l’indicateur"
    )

    last_value = graphene.Float(
        description="Dernière valeur numérique de l’indicateur"
    )

    category = graphene.String(description="Catégorie de l’indicateur")

    last_updated_date = graphene.String(description="Date de la ddernière mise à jour")

    # ---------- RESOLVERS ----------
    def resolve_values(self, info):
        return (
            self.values.filter(is_deleted=False)
            .order_by("-period_end", "-period_start")
        )

    def resolve_value(self, info):
        last_value = self.values.filter(is_deleted=False).order_by("-period_end", "-period_start").first()
        return last_value

    def resolve_last_value(self, info):
        v = self.values.filter(is_deleted=False).order_by("-period_end", "-period_start").first()
        return v.value if v else None

    def resolve_last_updated_date(self, info):
        v = self.values.filter(is_deleted=False).order_by("-period_end", "-period_start").first()
        return v.date_updated if v else None

    # ---------- META (toujours tout en bas !) ----------
    class Meta:
        model = Indicator
        interfaces = (graphene.relay.Node,)
        connection_class = ExtendedConnection
        filter_fields = {
            "id": ["exact"],
            "code": ["exact", "icontains"],
            "type": ["exact", "icontains"],
            "category": ["exact"],
            "method": ["exact", "icontains"],
            "status": ["exact", "icontains"],
            "module": ["exact", "icontains"],
            "name": ["icontains"],
            "frequency": ["exact"],
            "is_active": ["exact"],
        }


class IndicatorValueGQLType(DjangoObjectType):
    """
    Type GraphQL pour IndicatorValue
    conforme à Relay + ExtendedConnection
    """

    # Champ calculé pour affichage FE
    period = graphene.String(description="Période formatée : YYYY-MM-DD → YYYY-MM-DD")
    display_value = graphene.String()

    class Meta:
        model = IndicatorValue
        interfaces = (graphene.relay.Node,)
        connection_class = ExtendedConnection

        # EXPLICITE : les noms exposés à GraphQL seront en camelCase
        fields = (
            "id",
            "indicator",
            "period_start",
            "period_end",
            "region_code",
            "gender",
            "value",
            "qualitative_value",
            "source",
            "validated",
            "validated_by",
        )

        filter_fields = {
            "id": ["exact"],
            "indicator__code": ["exact", "icontains"],
            "region_code": ["exact", "icontains"],
            "gender": ["exact"],
            "validated": ["exact"],
            "period_start": ["gte", "lte"],
            "period_end": ["gte", "lte"],
        }

    # ---------- RESOLVERS ----------
    def resolve_period(self, info):
        """Retourne une période lisible pour le FE."""
        if not self.period_start or not self.period_end:
            return ""
        return f"{self.period_start} → {self.period_end}"

    def resolve_display_value(self, info):
        return (
            str(self.value)
            if self.value is not None
            else self.qualitative_value or "-"
        )


class MonitoringLogGQLType(DjangoObjectType):
    class Meta:
        model = MonitoringLog
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "period_start": ["exact", "gte", "lte"],
            "period_end": ["exact", "gte", "lte"],
            "executed_by__username": ["icontains"],
            "success": ["exact"],
        }
        connection_class = ExtendedConnection
