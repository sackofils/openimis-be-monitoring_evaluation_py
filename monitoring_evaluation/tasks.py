import logging
from datetime import timedelta
from django.utils import timezone
from django.db import transaction

# --- Models internes ---
from .models import User
from .indicators_services import calculate_me_indicators_for_period

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Calcul automatique des indicateurs Kobo (ODP/IRI)
# --------------------------------------------------------------------

def current_quarter_dates():
    """
    Détermine automatiquement la période trimestrielle en cours.
    """
    today = timezone.now().date()
    q_start_month = ((today.month - 1) // 3) * 3 + 1
    start = today.replace(month=q_start_month, day=1)
    end = start + timedelta(days=90)  # approx 3 mois
    return start, end


def run_recalculate_indicators_job(username="Admin"):
    """
    Recalcule tous les indicateurs automatiques Kobo (ODP/IRI)
    pour la période trimestrielle en cours.
    """
    user = User.objects.get(username=username)
    start, end = current_quarter_dates()
    count = calculate_me_indicators_for_period(start, end, user=user)
    logger.info(f"[Recalc Job] {count} indicateurs Kobo recalculés ({start} → {end}).")
    return count
