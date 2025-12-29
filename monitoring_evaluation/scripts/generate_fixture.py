"""
Script : generate_fixture.py
Objectif : Générer automatiquement une fixture JSON de mapping Kobo -> MonitoringSubmission
Auteur : ChatGPT (CoreMIS – Monitoring Evaluation)

Fonctionnement :
----------------
Entrée : un fichier JSON Kobo (format API v2) contenant "results": [...]
Sortie : un fichier fixture JSON utilisable par "python manage.py loaddata"

Usage :
    python generate_fixture.py \
        --input my_kobo_response.json \
        --form-name "FORM_COACHING" \
        --koboform-uuid f3d22ca1-231d-41fc-b50e-b39db8d35140 \
        --admin-uuid 00000000-0000-0000-0000-000000000001 \
        --output mapping_form_coaching.json
"""

import argparse
import json
import uuid


# --------------------------------------------------------------------
# Champs Kobo → MonitoringSubmission (mapping automatique)
# --------------------------------------------------------------------
FIXED_MAPPINGS = {
    "_uuid": "submission_uuid",
    "_submission_time": "submitted_at",
    "_submitted_by": "enumerator_username",
    "group_geo/group_district/secteur_code": "location_code",
}

# Tous les autres champs → json_ext.*. automatiquement
# exemple : suiviTechniqueProductive/agr_creer → json_ext.suiviTechniqueProductive.agr_creer


# --------------------------------------------------------------------
# Fonction : récupérer les champs Kobo à partir du JSON "results"
# --------------------------------------------------------------------
def extract_fields_from_kobo_results(results):
    """
    Récupère tous les chemins de champs Kobo dans une liste 'results'.
    On parcourt récursivement chaque dictionnaire, y compris les sous-nœuds.
    """
    all_fields = set()

    def walk(prefix, value):
        if isinstance(value, dict):
            for k, v in value.items():
                path = f"{prefix}/{k}" if prefix else k
                walk(path, v)
        elif isinstance(value, list):
            for item in value:
                walk(prefix, item)
        else:
            all_fields.add(prefix)

    for item in results:
        walk("", item)

    return sorted(all_fields)


# --------------------------------------------------------------------
# Fonction : construction des entrées de fixture pour chaque champ
# --------------------------------------------------------------------
def build_fixture_entries(form_uuid, admin_uuid, kobo_fields, form_name):
    fixture = []

    for kobo_field in kobo_fields:

        # Si champ connu → mapping direct vers MonitoringSubmission
        if kobo_field in FIXED_MAPPINGS:
            grievance_field = FIXED_MAPPINGS[kobo_field]

        # Sinon → json_ext.<path> automatique
        else:
            grievance_field = "json_ext." + kobo_field.replace("/", ".")

        entry = {
            "model": "kobo_connect.kobofieldmapping",
            "pk": str(uuid.uuid4()),
            "fields": {
                "kobo_form": form_uuid,
                "kobo_field": kobo_field,
                "grievance_field": grievance_field,
                "user_created": admin_uuid,
                "user_updated": admin_uuid,
            },
        }
        fixture.append(entry)

    return fixture


# --------------------------------------------------------------------
# Programme principal
# --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Générateur de fixture de mapping Kobo→MonitoringSubmission")
    parser.add_argument("--input", required=True, help="Chemin du fichier JSON Kobo (avec 'results')")
    parser.add_argument("--output", required=True, help="Chemin du fichier de sortie fixture JSON")
    parser.add_argument("--koboform-uuid", required=True, help="UUID de l'objet KoboForm")
    parser.add_argument("--admin-uuid", required=True, help="UUID du user admin")
    parser.add_argument("--form-name", required=True, help="Nom du formulaire (pour logs)")

    args = parser.parse_args()

    print(f"Lecture fichier Kobo : {args.input}")
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "results" not in data:
        raise ValueError("Le fichier JSON ne contient pas 'results' à la racine.")

    results = data["results"]
    print(f"{len(results)} enregistrements Kobo trouvés.")

    print("Extraction des champs Kobo...")
    kobo_fields = extract_fields_from_kobo_results(results)
    print(f"{len(kobo_fields)} champs détectés.")

    print("Génération des entrées de mapping...")
    fixture = build_fixture_entries(
        form_uuid=args.koboform_uuid,
        admin_uuid=args.admin_uuid,
        kobo_fields=kobo_fields,
        form_name=args.form_name,
    )

    print(f"{len(fixture)} lignes générées dans la fixture.")

    print(f"Écriture fichier fixture : {args.output}")
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(fixture, f, indent=2, ensure_ascii=False)

    print("\nFixture générée avec succès !")


if __name__ == "__main__":
    main()

#python generate_fixture.py \
#  --input kobo_coaching.json \
#  --form-name "FORM_COACHING" \
#  --koboform-uuid c1fdaa45-77da-4f44-9d31-eaf12d0b611b \
#  --admin-uuid 00000000-0000-0000-0000-000000000001 \
#  --output mapping_form_coaching.json

# arMrTT8uQHKTs8T9pAKYgW
#
# python generate_fixture.py \
# --input "../data/fiche_enregistrement.json" \
# --form-name "Fiche d'enregistrement des Bénéficiaires" \
# --koboform-uuid "e8e6383c-5036-43e9-945e-f8e3a5cca7e8" \
# --admin-uuid "30415d40-5925-4fb4-9fa0-0ed29dbae460" \
# --output "mapping_form_fiche_enregistrement_beneficiaire.json" \

# Charger dans Django:
# python manage.py loaddata mapping_form_coaching.json
