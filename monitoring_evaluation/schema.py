import graphene
import graphene_django_optimizer as gql_optimizer
from django.utils.translation import gettext_lazy as _

from core.schema import OrderedDjangoFilterConnectionField
from django.contrib.auth.models import AnonymousUser
from core.utils import append_validity_filter

# Import des queries du module
from .gql_queries import Query as MonitoringEvaluationQuery

# Import des mutations du module
from .gql_mutations import (
    CreateIndicatorMutation,
    CreateIndicatorValueMutation,
    RecalculateIndicatorsMutation,
)


class Mutation(graphene.ObjectType):
    """
    Mutations GraphQL du module Suivi-Évaluation
    """

    # Gestion des indicateurs
    create_indicator = CreateIndicatorMutation.Field()

    # Gestion des valeurs d’indicateurs
    create_indicator_value = CreateIndicatorValueMutation.Field()

    # Recalcul automatique des indicateurs
    recalculate_indicators = RecalculateIndicatorsMutation.Field()


class Query(MonitoringEvaluationQuery, graphene.ObjectType):
    """
    Requêtes GraphQL du module Suivi-Évaluation
    """
    pass


# Schéma GraphQL complet du module
schema = graphene.Schema(query=Query, mutation=Mutation)
