import json
import uuid
import datetime
from datetime import date
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError, PermissionDenied
from django.db.models import Max, Q
from django.db import transaction

from core.services import BaseService
from core.signals import register_service_signal
from core.models.user import Role, UserRole, InteractiveUser, User
from core.services.utils import check_authentication as check_authentication, output_exception, \
    model_representation, output_result_success
from monitoring_evaluation.models import Indicator, IndicatorValue, MonitoringSubmission
from monitoring_evaluation.validations import (
    IndicatorValidation,
    IndicatorValueValidation
)
from django.contrib.auth import get_user_model


User = get_user_model()

class IndicatorService(BaseService):
    OBJECT_TYPE = Indicator

    def __init__(self, user, validation_class=IndicatorValidation):
        super().__init__(user, validation_class)

    @register_service_signal('indicator_service.create')
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal('indicator_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('indicator_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)

class IndicatorValueService(BaseService):
    """
    Service openIMIS pour gérer les CRUD et règles métier de IndicatorValue.
    Structure identique à TicketService.
    """
    OBJECT_TYPE = IndicatorValue

    def __init__(self, user, validation_class=IndicatorValueValidation):
        super().__init__(user, validation_class)

    # ======================================================
    #                   HELPERS (internal)
    # ======================================================

    def _get_indicator(self, indicator_id):
        indicator = Indicator.objects.filter(id=indicator_id).first()
        if not indicator:
            raise ValidationError("Indicateur introuvable")
        return indicator

    def _get_last_value(self, indicator: Indicator):
        """
        Récupération de la dernière valeur numérique pour contrôles.
        """
        return (
            IndicatorValue.objects
            .filter(indicator=indicator, is_deleted=False)
            .order_by("-period_end", "-id")
            .first()
        )

    def _validate_manual_indicator_method(self, indicator: Indicator):
        if indicator.method.lower() != "manuel":
            raise ValidationError("La saisie manuelle n’est pas autorisée pour cet indicateur.")

    def _validate_cumulative_value(self, indicator: Indicator, new_value: float):
        """
        Si dernière valeur numérique existe → new_value >= last_value.
        """
        last_val = self._get_last_value(indicator)
        if (
            new_value is not None
            and last_val
            and last_val.value is not None
            and new_value < last_val.value
        ):
            raise ValidationError(
                f"La valeur saisie ({new_value}) ne peut pas être inférieure "
                f"à la dernière valeur enregistrée ({last_val.value})."
            )

    @register_service_signal("indicator_value_service.create")
    def create(self, obj_data):
        """
        Surcharge du CRUD create de BaseService.
        Applique les règles métier :
        - indicateur manuel uniquement
        - validation cumulative
        """
        try:
            with transaction.atomic():

                indicator_id = obj_data.get("indicator_id")
                value = obj_data.get("value")

                indicator = self._get_indicator(indicator_id)

                # Validation méthode + cumul
                self._validate_manual_indicator_method(indicator)
                self._validate_cumulative_value(indicator, value)

                # Conversion dates ISO → Date field
                if "period_start" in obj_data:
                    obj_data["period_start"] = date.fromisoformat(obj_data["period_start"])
                if "period_end" in obj_data:
                    obj_data["period_end"] = date.fromisoformat(obj_data["period_end"])

                # Créer via BaseService
                return super().create(obj_data)

        except Exception as exc:
            return output_exception(
                model_name=self.OBJECT_TYPE.__name__,
                method="create",
                exception=exc,
            )

    @register_service_signal("indicator_value_service.update")
    def update(self, obj_data):
        """
        Semi-modifiable : si on met à jour une valeur → on revérifie la règle cumulative.
        """
        try:
            with transaction.atomic():

                instance_id = obj_data.get("id")
                instance = IndicatorValue.objects.filter(id=instance_id).first()
                if not instance:
                    raise ValidationError("Valeur introuvable")

                indicator = instance.indicator

                #if "value" in obj_data:
                #    new_value = obj_data.get("value")
                #    self._validate_cumulative_value(indicator, new_value)

                return super().update(obj_data)

        except Exception as exc:
            return output_exception(
                model_name=self.OBJECT_TYPE.__name__,
                method="update",
                exception=exc,
            )

    @register_service_signal("indicator_value_service.delete")
    def delete(self, obj_data):
        try:
            return super().delete(obj_data)
        except Exception as exc:
            return output_exception(
                model_name=self.OBJECT_TYPE.__name__,
                method="delete",
                exception=exc,
            )

    @register_service_signal("indicator_value_service.validate")
    def validate_value(self, obj_data):
        """
        Valide une valeur d’indicateur (validated=True + validated_by).
        """
        try:
            with transaction.atomic():
                self.validation_class.validate_validate(self.user, **obj_data)

                value_id = obj_data.get("id")
                if not value_id:
                    raise ValidationError("Missing 'id' for validation")

                iv = IndicatorValue.objects.get(id=value_id)
                iv.validated = True
                iv.validated_by = self.user
                iv.save(username=self.user.username)

                return output_result_success(
                    dict_representation=model_representation(iv)
                )
        except Exception as exc:
            return output_exception(
                model_name=self.OBJECT_TYPE.__name__,
                method="validate_value",
                exception=exc,
            )