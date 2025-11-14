import logging
from datetime import timedelta
from django.db import transaction
from django.utils import timezone

from kobo_connect.synchronizer import (
    KoboClient,
    _build_choice_resolver,
    _build_mapping,
    _get_token,
    _get_uid,
    _parse_ts,
    _save_log,
)

from kobo_connect.models import KoboForm
from be_monitoring_evaluation.models import MonitoringSubmission
from location.models import Location
from core.models import User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Synchronisation d’un KoboForm vers MonitoringSubmission
# ---------------------------------------------------------

def start_me_sync(kobo_form: KoboForm, user=None, since=None, dry_run=False):
    """Synchronise un formulaire Kobo (TMU/TMR, SERE NAFA, etc.) vers MonitoringSubmission."""

    token = _get_token(kobo_form)
    base_url = (token.url_kobo or "").strip().rstrip("/")
    uid = _get_uid(kobo_form)

    client = KoboClient(base_url=base_url, token=token.api_key)

    # Résolution des labels (pour select_one/multiple)
    try:
        resolve_label = _build_choice_resolver(client, uid, lang="fr")
    except Exception as e:
        logger.warning("[Kobo Label] désactivé : %s", e)
        resolve_label = None

    direct_map, extras_map = _build_mapping(kobo_form)

    _since = since or getattr(kobo_form, "last_sync_date", None)
    if _since:
        _since = _since - timedelta(minutes=1)

    _save_log(kobo_form, user, "success", "start", f"Début sync ME UID={uid}")

    created = updated = skipped = failed = 0
    max_submission_ts = getattr(kobo_form, "last_sync_date", None)

    try:
        for row in client.iter_submissions(uid):
            sub_ts = _parse_ts(row.get("_submission_time") or row.get("end") or row.get("start"))
            if _since and sub_ts and sub_ts <= _since:
                skipped += 1
                continue

            if sub_ts and (max_submission_ts is None or sub_ts > max_submission_ts):
                max_submission_ts = sub_ts

            sub_uuid = row.get("_uuid") or row.get("meta/instanceID")

            try:
                with transaction.atomic():
                    # Vérifier si la soumission existe déjà
                    sub = MonitoringSubmission.objects.filter(submission_uuid=sub_uuid).first()
                    is_create = sub is None
                    if is_create:
                        sub = MonitoringSubmission()

                    # Champs de base
                    sub.submission_uuid = sub_uuid
                    sub.submitted_at = sub_ts
                    sub.form_type = getattr(kobo_form, "form_type", "UNKNOWN")

                    # Détermination de la localité
                    loc_code = row.get("group_geo/group_prefecture/region_code") or row.get("group_geo/region")
                    if loc_code:
                        loc = Location.objects.filter(code=loc_code).first() or Location.objects.filter(name__iexact=loc_code).first()
                        if loc:
                            sub.location = loc

                    # Affectation JSON brut
                    sub.json_ext = row
                    sub.period = f"{sub_ts.year}-Q{((sub_ts.month - 1)//3) + 1}"

                    if dry_run:
                        logger.info("[Dry-Run] Soumission simulée (create/update): %s", sub.submission_uuid)
                    else:
                        sub.save(user=user)
                        if is_create:
                            created += 1
                        else:
                            updated += 1
            except Exception as inner:
                failed += 1
                logger.exception("[ME Sync] Erreur sur une soumission: %s", inner)
                _save_log(kobo_form, user, "failed", "row_error", str(inner), row)

    except Exception as e:
        logger.exception("[ME Sync] Erreur pendant la récupération des soumissions")
        _save_log(kobo_form, user, "failed", "error", str(e))
        return

    # Mettre à jour la date de dernière sync
    setattr(kobo_form, "last_sync_date", max_submission_ts or timezone.now())
    try:
        kobo_form.save(user=user)
    except TypeError:
        kobo_form.save()

    _save_log(
        kobo_form,
        user,
        "success",
        "end",
        f"Terminé : created={created}, updated={updated}, skipped={skipped}, failed={failed}",
        {"created": created, "updated": updated, "skipped": skipped, "failed": failed},
    )
