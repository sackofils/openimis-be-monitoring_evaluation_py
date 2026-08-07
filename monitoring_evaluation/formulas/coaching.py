import unicodedata

from ..formula_utils import save_indicator_value
from ..models import MonitoringSubmission


COACHING_FORM = "FICHE_SESSIONS_COACHING_INDIVIDUEL"
COMMUNICATION_FORM = "COMMUNICATION_PROGRAMME"
COACHING_SOURCE = "Fiche de suivi des sessions de coaching individuel"


def _submissions(form_type, start, end):
    return MonitoringSubmission.objects.filter(
        form_type=form_type,
        submitted_at__date__range=(start, end),
    )


def _household_code(payload):
    value = (payload.get("suiviIndividuel") or {}).get("codeMenage")
    return str(value).strip().upper() if value else None


def _values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return value
    return [value]


def _normalized(value):
    text = unicodedata.normalize("NFKD", str(value))
    return "".join(char for char in text if not unicodedata.combining(char)).strip().casefold()


def _contains(value, accepted):
    accepted = {_normalized(item) for item in accepted}
    return any(_normalized(item) in accepted for item in _values(value))


def _number(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def calc_PIP_026(indicator, start, end):
    beneficiaries = {
        code
        for submission in _submissions(COACHING_FORM, start, end)
        if (code := _household_code(submission.json_ext or {}))
    }
    save_indicator_value(
        indicator,
        start,
        end,
        len(beneficiaries),
        source=COACHING_SOURCE,
    )


def calc_PIP_027(indicator, start, end):
    respondents = set()
    satisfied = set()

    for submission in _submissions(COACHING_FORM, start, end):
        payload = submission.json_ext or {}
        code = _household_code(payload)
        satisfaction = (payload.get("evaluation") or {}).get("niveau_satisfaction")
        if not code or not _values(satisfaction):
            continue
        respondents.add(code)
        if _contains(satisfaction, {"Satisfait", "Tres satisfait", "Oui"}):
            satisfied.add(code)

    value = round((len(satisfied) / len(respondents)) * 100, 2) if respondents else 0.0
    save_indicator_value(
        indicator,
        start,
        end,
        value,
        source="Accompagnement - coaching individuel/groupe",
    )


def calc_PIP_028(indicator, start, end):
    expected = 0.0
    present = 0.0
    for submission in _submissions(COMMUNICATION_FORM, start, end):
        payload = submission.json_ext or {}
        expected += _number(payload.get("participants_prevus"))
        present += _number(payload.get("participants_total"))

    value = round((present / expected) * 100, 2) if expected else 0.0
    save_indicator_value(
        indicator,
        start,
        end,
        value,
        source="Communication de Programme et Mobilisation Communautaire",
    )


def calc_PIP_029(indicator, start, end):
    beneficiaries = set()
    for submission in _submissions(COACHING_FORM, start, end):
        payload = submission.json_ext or {}
        code = _household_code(payload)
        productive = payload.get("suiviTechniqueProductive") or {}
        has_activity = _contains(productive.get("agr_creer"), {"Oui"}) or _contains(
            productive.get("agr_existant"), {"Oui"}
        )
        if code and has_activity:
            beneficiaries.add(code)

    save_indicator_value(
        indicator,
        start,
        end,
        len(beneficiaries),
        source=COACHING_SOURCE,
    )
