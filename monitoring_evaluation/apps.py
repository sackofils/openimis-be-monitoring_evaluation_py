from django.apps import AppConfig

MODULE_NAME = "monitoring_evaluation"

DEFAULT_CFG = {
    # Permissions associées aux queries GraphQL
    "gql_query_forms_perms": ["121805"],
    "gql_query_tokens_perms": ["121806"],

    # Permissions associées aux mutations GraphQL
    "gql_mutation_forms_add_perms": ["121807"],
    "gql_mutation_forms_update_perms": ["121808"],
    "gql_mutation_forms_delete_perms": ["121809"],

    "gql_mutation_tokens_add_perms": ["121809"],
    "gql_mutation_tokens_update_perms": ["121810"],
    "gql_mutation_tokens_delete_perms": ["121811"],
}


class MonitoringEvaluationConfig(AppConfig):
    name = MODULE_NAME
    verbose_name = "Monitoring Evaluation"

    # Déclaration des permissions configurables dynamiquement
    gql_query_forms_perms = []
    gql_query_tokens_perms = []

    gql_mutation_forms_add_perms = []
    gql_mutation_forms_update_perms = []
    gql_mutation_forms_delete_perms = []

    gql_mutation_tokens_add_perms = []
    gql_mutation_tokens_update_perms = []
    gql_mutation_tokens_delete_perms = []

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
