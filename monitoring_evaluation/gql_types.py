import graphene
from graphene_django import DjangoObjectType
from core import ExtendedConnection
from .models import Indicator, IndicatorValue


class IndicatorGQLType(DjangoObjectType):
    class Meta:
        model = Indicator
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "uuid": ["exact"],
            "code": ["exact", "icontains"],
            "name": ["icontains"],
            "frequency": ["exact"],
            "is_active": ["exact"],
        }
        connection_class = ExtendedConnection


class IndicatorValueGQLType(DjangoObjectType):
    indicator = graphene.Field(IndicatorGQLType)

    class Meta:
        model = IndicatorValue
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "indicator__code": ["exact", "icontains"],
            "region_code": ["exact", "icontains"],
            "gender": ["exact"],
            "validated": ["exact"],
            "period_start": ["gte", "lte"],
            "period_end": ["gte", "lte"],
        }
        connection_class = ExtendedConnection


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
