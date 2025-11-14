"""
Déclaration des tâches planifiées du module Suivi-Évaluation (Monitoring & Evaluation)
Ces tâches sont automatiquement importées par core/apscheduler_config.py
"""

JOBS = [
    # Synchronisation Kobo → MonitoringSubmission
    {
        "method": "be_monitoring_evaluation.tasks.run_kobo_sync_job",
        "kwargs": {"username": "admin"},
        "id": "me_kobo_sync",
        "trigger": "cron",
        "hour": "2",           # chaque jour à 02h00
        "minute": "0",
        "replace_existing": True,
    },

    # Recalcul automatique des indicateurs Kobo (ODP/IRI)
    {
        "method": "be_monitoring_evaluation.tasks.run_recalculate_indicators_job",
        "kwargs": {"username": "admin"},
        "id": "me_recalc_job",
        "trigger": "cron",
        "hour": "3",           # chaque jour à 03h00 (après sync Kobo)
        "minute": "0",
        "replace_existing": True,
    },
]
