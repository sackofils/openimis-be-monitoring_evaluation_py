from datetime import datetime
from django.core.management.base import BaseCommand
from django.utils.dateparse import parse_date
from monitoring_evaluation.services.indicators import calculate_indicators_for_period

class Command(BaseCommand):
    help = "Recalcule les indicateurs entre 2 dates (YYYY-MM-DD)"

    def add_arguments(self, parser):
        parser.add_argument("start")
        parser.add_argument("end")

    def handle(self, *args, **options):
        start = parse_date(options["start"])
        end = parse_date(options["end"])
        if not start or not end:
            self.stderr.write(self.style.ERROR("Dates invalides. Format attendu: YYYY-MM-DD YYYY-MM-DD"))
            return
        count = calculate_indicators_for_period(start, end)
        self.stdout.write(self.style.SUCCESS(f"Recalcul terminé : {count} indicateurs."))
