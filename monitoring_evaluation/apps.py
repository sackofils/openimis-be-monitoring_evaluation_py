from django.apps import AppConfig

MODULE_NAME = "monitoring_evaluation"

DEFAULT_CFG = {
    # Permissions associées aux queries GraphQL
    "gql_query_indicators_perms": ["128001"],

    # Permissions associées aux mutations GraphQL
    "gql_mutation_indicators_add_perms": ["128002"],
    "gql_mutation_indicators_update_perms": ["128003"],
    "gql_mutation_indicators_delete_perms": ["128004"],
    "gql_mutation_indicators_duplicate_perms": ["128005"],
    "gql_mutation_indicators_recalculate_perms": ["128006"],

    "gql_mutation_indicators_add_value_perms": ["128007"],
    "gql_mutation_indicators_edit_value_perms": ["128008"],
    "gql_mutation_indicators_delete_value_perms": ["128009"],
    "gql_mutation_indicators_validate_value_perms": ["128010"],
}


class MonitoringEvaluationConfig(AppConfig):
    name = MODULE_NAME
    verbose_name = "Monitoring Evaluation"

    # Déclaration des permissions configurables dynamiquement
    gql_query_indicators_perms = []

    gql_mutation_indicators_add_perms = []
    gql_mutation_indicators_update_perms = []
    gql_mutation_indicators_delete_perms = []
    gql_mutation_indicators_duplicate_perms = []
    gql_mutation_indicators_recalculate_perms = []

    gql_mutation_indicators_add_value_perms = []
    gql_mutation_indicators_edit_value_perms = []
    gql_mutation_indicators_delete_value_perms = []
    gql_mutation_indicators_validate_value_perms = []

    def __load_config(self, cfg):
        """
        Charge dynamiquement les permissions définies dans la configuration du module.
        """
        for field in cfg:
            if hasattr(MonitoringEvaluationConfig, field):
                setattr(MonitoringEvaluationConfig, field, cfg[field])

    def ready(self):
        """
        Appelé à l'initialisation de l'application.
        Enregistre la configuration du module dans ModuleConfiguration.
        """
        from core.models import ModuleConfiguration
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self.__load_config(cfg)
