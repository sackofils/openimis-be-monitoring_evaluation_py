from django.core.management.base import BaseCommand
from monitoring_evaluation.tasks import run_kobo_sync_job

class Command(BaseCommand):
    help = "Synchronise les formulaires Kobo vers MonitoringSubmission"

    def add_arguments(self, parser):
        parser.add_argument("--username", default="admin")

    def handle(self, *args, **options):
        total = run_kobo_sync_job(username=options["username"])
        self.stdout.write(self.style.SUCCESS(f"Sync terminé : {total} enregistrements (tous formulaires)."))
