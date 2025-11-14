from datetime import timedelta
import logging
from django.utils import timezone
from django.db import transaction

# --- Imports Kobo ---
from kobo_connect.synchronizer import KoboClient, _parse_ts, _get_token, _get_uid, _save_log
from kobo_connect.models import KoboForm

# --- Models internes ---
from .models import MonitoringSubmission, User
from .kobo_mapping import FORM_TYPES, extract_beneficiary_id, extract_location_id, normalize_period_from_dt
from .indicators_services import calculate_me_indicators_for_period

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Récupération des soumissions Kobo (API KPI v2)
# --------------------------------------------------------------------

def fetch_kobo_rows(form_name: str, since: timezone.datetime = None) -> list[dict]:
    """
    Récupère les soumissions Kobo d’un formulaire donné via l’API Kobo v2.
    Utilise le client standard de kobo_connect/synchronizer.
    Si 'since' est fourni, ne récupère que les soumissions plus récentes.
    """
    try:
        # Trouver le KoboForm correspondant
        kobo_form = KoboForm.objects.filter(name__icontains=form_name).first()
        if not kobo_form:
            logger.warning(f"[Kobo] Aucun KoboForm trouvé pour {form_name}")
            return []

        # Construire le client Kobo
        token = _get_token(kobo_form)
        uid = _get_uid(kobo_form)
        base_url = (token.url_kobo or "").strip().rstrip("/")
        client = KoboClient(base_url=base_url, token=token.api_key)

        rows = []
        for row in client.iter_submissions(uid):
            sub_ts = _parse_ts(row.get("_submission_time") or row.get("end") or row.get("start"))
            if since and sub_ts and sub_ts <= since:
                continue
            rows.append(row)

        logger.info(f"[Kobo] {form_name}: {len(rows)} soumissions récupérées depuis Kobo.")
        return rows

    except Exception as e:
        logger.exception(f"[Kobo] Erreur fetch_kobo_rows({form_name}): {e}")
        return []


# --------------------------------------------------------------------
# Synchronisation d’un formulaire Kobo vers MonitoringSubmission
# --------------------------------------------------------------------

def sync_one_form(form_name: str, run_user: User, since: timezone.datetime = None) -> int:
    """
    Synchronise un formulaire Kobo donné (TMU, SERE NAFA, AGR, etc.)
    vers la table MonitoringSubmission.
    Si 'since' est fourni, ne synchronise que les soumissions récentes.
    """
    form_type = FORM_TYPES.get(form_name)
    if not form_type:
        logger.warning(f"[Sync] Form type inconnu pour {form_name}")
        return 0

    rows = fetch_kobo_rows(form_name, since)
    count = 0

    with transaction.atomic():
        for r in rows:
            try:
                sub_uuid = r.get("_uuid") or r.get("_id")
                submitted_at = r.get("_submission_time")

                # Conversion ISO8601 → datetime Django
                if isinstance(submitted_at, str):
                    submitted_at = timezone.make_aware(
                        timezone.datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
                    )
                else:
                    submitted_at = timezone.now()

                # Calcul de la période trimestrielle
                period_label = normalize_period_from_dt(submitted_at)

                # Création ou mise à jour de la soumission
                obj, _created = MonitoringSubmission.objects.update_or_create(
                    submission_uuid=sub_uuid,
                    defaults=dict(
                        form_type=form_type,
                        submitted_at=submitted_at,
                        period=period_label,
                        beneficiary_id=extract_beneficiary_id(r),
                        location_id=extract_location_id(r),
                        json_ext=r,
                    ),
                )

                # Sauvegarde avec traçabilité (HistoryModel)
                obj.save(user=run_user)
                count += 1

            except Exception as inner:
                logger.exception(f"[Sync] Erreur sur une soumission {form_name}: {inner}")

    logger.info(f"[Sync] {form_name}: {count} soumissions synchronisées.")
    return count


# --------------------------------------------------------------------
# Synchronisation de tous les formulaires Kobo de suivi-évaluation
# --------------------------------------------------------------------

def run_kobo_sync_job(username="admin"):
    """
    Synchronise tous les formulaires Kobo liés au module Suivi-Évaluation :
    FORM_TMU & TMR, FORM_SERE NAFA, FORM_AGR, FORM_SUBVENTION BENEF
    """
    user = User.objects.get(username=username)
    total = 0
    since = timezone.now() - timedelta(days=1)  # synchronisation des dernières 24h

    for form_name in ("FORM_TMU & TMR", "FORM_SERE NAFA", "FORM_AGR", "FORM_SUBVENTION BENEF"):
        count = sync_one_form(form_name, user, since)
        total += count

        # Journalisation KoboSyncLog
        try:
            kobo_form = KoboForm.objects.filter(name__icontains=form_name).first()
            if kobo_form:
                _save_log(
                    kobo_form,
                    user,
                    status="success",
                    action="sync_me",
                    message=f"{form_name}: {count} soumissions synchronisées (since {since})",
                )
        except Exception as e:
            logger.warning(f"[Kobo Log] Erreur enregistrement log {form_name}: {e}")

    logger.info(f"[Kobo Sync Job] Total: {total} soumissions synchronisées (toutes sources).")
    return total


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


def run_recalculate_indicators_job(username="admin"):
    """
    Recalcule tous les indicateurs automatiques Kobo (ODP/IRI)
    pour la période trimestrielle en cours.
    """
    user = User.objects.get(username=username)
    start, end = current_quarter_dates()
    count = calculate_me_indicators_for_period(start, end, user=user)
    logger.info(f"[Recalc Job] {count} indicateurs Kobo recalculés ({start} → {end}).")
    return count
