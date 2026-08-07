from django.core.management.base import BaseCommand, CommandError
from core.models import User
from django.utils.dateparse import parse_date
from monitoring_evaluation.indicators_services import calculate_me_indicators_for_period

class Command(BaseCommand):
    help = "Recalcule les indicateurs entre 2 dates (YYYY-MM-DD)"

    def add_arguments(self, parser):
        parser.add_argument("start")
        parser.add_argument("end")
        parser.add_argument("--username", default="Admin")

    def handle(self, *args, **options):
        start = parse_date(options["start"])
        end = parse_date(options["end"])
        if not start or not end:
            self.stderr.write(self.style.ERROR("Dates invalides. Format attendu: YYYY-MM-DD YYYY-MM-DD"))
            return
        try:
            user = User.objects.get(username=options["username"])
        except User.DoesNotExist as error:
            raise CommandError(
                f"Utilisateur introuvable: {options['username']}"
            ) from error
        count = calculate_me_indicators_for_period(start, end, user=user)
        self.stdout.write(self.style.SUCCESS(f"Recalcul terminé : {count} indicateurs."))
