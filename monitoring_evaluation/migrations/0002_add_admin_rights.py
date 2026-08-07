from django.db import migrations


RIGHTS = [128001, 128002, 128003, 128004, 128005, 128006, 128007, 128008, 128009, 128010]
ADMINISTRATOR_SYSTEM_ROLE = 64


def add_rights(apps, schema_editor):
    Role = apps.get_model("core", "Role")
    RoleRight = apps.get_model("core", "RoleRight")
    role = Role.objects.get(is_system=ADMINISTRATOR_SYSTEM_ROLE)
    for right_id in RIGHTS:
        RoleRight.objects.get_or_create(
            role=role,
            right_id=right_id,
            validity_to=None,
            defaults={"audit_user_id": 1},
        )


def remove_rights(apps, schema_editor):
    RoleRight = apps.get_model("core", "RoleRight")
    RoleRight.objects.filter(
        role__is_system=ADMINISTRATOR_SYSTEM_ROLE,
        right_id__in=RIGHTS,
        validity_to__isnull=True,
    ).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("monitoring_evaluation", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(add_rights, remove_rights),
    ]
