from django.test import SimpleTestCase, TestCase

from core.models import User
from core.test_helpers import create_test_interactive_user
from monitoring_evaluation.gql_mutations import DuplicateIndicatorMutation
from monitoring_evaluation.models import Indicator


class DuplicateIndicatorMutationTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = create_test_interactive_user(username="me_duplicate_user")

    def test_duplicate_indicator_keeps_original_category(self):
        indicator = Indicator(
            code="RF001",
            name="Result framework indicator",
            description="Test",
            category=Indicator.CATEGORY_RF,
            type="QUANTITATIVE",
            unit="NOMBRE",
            frequency="MENSUEL",
            target=10,
            module="individual",
            method="MANUEL",
            calculation_method="manual input",
            status="ACTIVE",
            is_automatic=False,
            is_active=True,
        )
        indicator.save(user=self.user)

        DuplicateIndicatorMutation._mutate(
            self.user,
            id=str(indicator.id),
            new_code="RF001_COPY",
            client_mutation_id="dup-test-1",
            client_mutation_label="duplicate indicator",
        )

        duplicated = Indicator.objects.get(code="RF001_COPY")
        self.assertEqual(duplicated.category, Indicator.CATEGORY_RF)


from datetime import date, datetime, timezone
from types import SimpleNamespace

from django.core.exceptions import PermissionDenied

from monitoring_evaluation.apps import MonitoringEvaluationConfig
from monitoring_evaluation.formulas.coaching import (
    calc_PIP_026,
    calc_PIP_027,
    calc_PIP_028,
    calc_PIP_029,
)
from monitoring_evaluation.gql_mutations import CreateIndicatorMutation
from monitoring_evaluation.models import IndicatorValue, MonitoringSubmission


class MonitoringPermissionTestCase(SimpleTestCase):
    def test_indicator_creation_requires_configured_permission(self):
        denied_user = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            has_perms=lambda permissions: False,
        )
        with self.assertRaises(PermissionDenied):
            CreateIndicatorMutation._validate_mutation(denied_user)

        allowed_user = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            has_perms=lambda permissions: (
                permissions
                == MonitoringEvaluationConfig.gql_mutation_indicators_add_perms
            ),
        )
        CreateIndicatorMutation._validate_mutation(allowed_user)


class CoachingFormulaTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = (
            User.objects.filter(username='Admin').first()
            or create_test_interactive_user(username='Admin')
        )
        cls.start = date(2026, 1, 1)
        cls.end = date(2026, 1, 31)

        cls.coaching = MonitoringSubmission(
            form_type="FICHE_SESSIONS_COACHING_INDIVIDUEL",
            submitted_at=datetime(2026, 1, 15, 10, tzinfo=timezone.utc),
            json_ext={
                "suiviIndividuel": {"codeMenage": " hh-01 "},
                "evaluation": {"niveau_satisfaction": "Tres satisfait"},
                "suiviTechniqueProductive": {"agr_creer": "Oui"},
            },
        )
        cls.coaching.save(user=cls.user)

        outside_period = MonitoringSubmission(
            form_type="FICHE_SESSIONS_COACHING_INDIVIDUEL",
            submitted_at=datetime(2026, 2, 1, 10, tzinfo=timezone.utc),
            json_ext={"suiviIndividuel": {"codeMenage": "HH-02"}},
        )
        outside_period.save(user=cls.user)

        communication = MonitoringSubmission(
            form_type="COMMUNICATION_PROGRAMME",
            submitted_at=datetime(2026, 1, 20, 10, tzinfo=timezone.utc),
            json_ext={"participants_prevus": 10, "participants_total": 8},
        )
        communication.save(user=cls.user)

    def _indicator(self, code):
        indicator = Indicator(
            code=code,
            name=code,
            type="QUANTITATIVE",
            unit="NOMBRE",
            frequency="MENSUEL",
            method="AUTOMATIQUE",
            status="ACTIVE",
            is_automatic=True,
            is_active=True,
        )
        indicator.save(user=self.user)
        return indicator

    def _computed_value(self, indicator):
        return IndicatorValue.objects.get(
            indicator=indicator,
            period_start=self.start,
            period_end=self.end,
        ).value

    def test_coaching_formulas_use_only_requested_period(self):
        expected = (
            (calc_PIP_026, 1),
            (calc_PIP_027, 100.0),
            (calc_PIP_028, 80.0),
            (calc_PIP_029, 1),
        )
        for index, (formula, value) in enumerate(expected):
            indicator = self._indicator(f"TEST_PIP_{index}")
            formula(indicator, self.start, self.end)
            self.assertEqual(self._computed_value(indicator), value)

from unittest.mock import patch


class CoachingFormulaUnitTestCase(SimpleTestCase):
    start = date(2026, 1, 1)
    end = date(2026, 1, 31)
    indicator = SimpleNamespace(code="PIP_TEST")

    @patch("monitoring_evaluation.formulas.coaching.save_indicator_value")
    @patch("monitoring_evaluation.formulas.coaching._submissions")
    def test_coaching_formulas_compute_expected_values(self, submissions, save):
        coaching = SimpleNamespace(
            json_ext={
                "suiviIndividuel": {"codeMenage": " HH-01 "},
                "evaluation": {"niveau_satisfaction": "Tres satisfait"},
                "suiviTechniqueProductive": {"agr_creer": "Oui"},
            }
        )
        communication = SimpleNamespace(
            json_ext={"participants_prevus": 10, "participants_total": 8}
        )
        submissions.side_effect = (
            [coaching],
            [coaching],
            [communication],
            [coaching],
        )

        expected = (
            (calc_PIP_026, 1),
            (calc_PIP_027, 100.0),
            (calc_PIP_028, 80.0),
            (calc_PIP_029, 1),
        )
        for formula, value in expected:
            formula(self.indicator, self.start, self.end)
            self.assertEqual(save.call_args.args[3], value)