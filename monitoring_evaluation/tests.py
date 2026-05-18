from django.test import TestCase

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
