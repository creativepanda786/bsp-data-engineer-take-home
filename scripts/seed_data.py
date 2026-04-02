"""
PawsFirst Veterinary Network — Synthetic Data Generator

Generates deterministic seed data for the take-home exercise.
Run: uv run python scripts/seed_data.py

Outputs CSVs to data/ directory.
"""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# Deterministic seed for reproducibility
SEED = 42
DATA_DIR = Path(__file__).parent.parent / "data"

# --- Domain Constants ---

AREAS = {
    "Northeast": ["Boston", "Hartford", "Portland ME", "Providence"],
    "Southeast": ["Atlanta", "Charlotte", "Raleigh", "Nashville"],
    "West": ["Denver", "Phoenix", "Salt Lake City", "Riverside"],
}

SERVICE_TYPES = [
    ("WELL", "Wellness Exam", 75.00),
    ("VACC", "Vaccination", 45.00),
    ("DENT", "Dental Cleaning", 250.00),
    ("SURG", "Surgery", 800.00),
    ("XRAY", "X-Ray / Imaging", 150.00),
    ("EMER", "Emergency Visit", 200.00),
    ("FOLL", "Follow-Up Visit", 50.00),
    ("SPEC", "Specialist Consultation", 175.00),
]

SPECIES = ["Dog", "Cat", "Rabbit", "Bird", "Hamster"]
SPECIES_WEIGHTS = [0.50, 0.30, 0.08, 0.07, 0.05]

DOG_BREEDS = [
    "Labrador Retriever", "Golden Retriever", "German Shepherd",
    "Bulldog", "Poodle", "Beagle", "Rottweiler", "Dachshund",
    "Boxer", "Siberian Husky", "Mixed Breed",
]
CAT_BREEDS = [
    "Domestic Shorthair", "Domestic Longhair", "Siamese",
    "Persian", "Maine Coon", "Ragdoll", "Bengal", "Mixed Breed",
]

INSURANCE_PROVIDERS = [
    "PetPlan", "Trupanion", "Nationwide Pet", "ASPCA Pet Health",
    "Embrace", "Figo", "Healthy Paws", None,
]

REFERRAL_SOURCES = [
    "website", "google_search", "vet_referral", "friend_referral",
    "social_media", "walk_in", "shelter_partner", "insurance_directory",
]

FUNNEL_STAGES = ["inquiry", "consultation", "registered", "active", "churned"]
FUNNEL_STAGE_ORDER = {s: i for i, s in enumerate(FUNNEL_STAGES)}

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
    "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
    "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra",
    "Steven", "Ashley", "Andrew", "Dorothy", "Paul", "Kimberly",
    "Joshua", "Emily", "Kenneth", "Donna", "Kevin", "Michelle",
    "Brian", "Carol", "George", "Amanda", "Timothy", "Melissa",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
    "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott",
    "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
]

PET_NAMES = [
    "Max", "Bella", "Charlie", "Luna", "Cooper", "Daisy",
    "Buddy", "Sadie", "Rocky", "Molly", "Bear", "Bailey",
    "Duke", "Maggie", "Tucker", "Sophie", "Jack", "Chloe",
    "Oliver", "Penny", "Leo", "Zoey", "Milo", "Lily",
    "Finn", "Stella", "Zeus", "Rosie", "Gus", "Ruby",
    "Murphy", "Lola", "Louie", "Gracie", "Bentley", "Nala",
    "Teddy", "Willow", "Winston", "Hazel", "Moose", "Piper",
]

VET_SPECIALTIES = [
    "General Practice", "General Practice", "General Practice",
    "General Practice", "Surgery", "Dentistry", "Emergency Medicine",
    "Internal Medicine", "Dermatology", "Oncology",
]

VET_CREDENTIALS = ["DVM", "DVM", "DVM", "VMD", "DVM, DACVS", "DVM, DAVDC"]


def _rng() -> random.Random:
    return random.Random(SEED)


def _date_range(start: date, end: date) -> list[date]:
    """Generate list of dates from start to end inclusive."""
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


# --- Generators ---


def generate_clinics(rng: random.Random) -> list[dict]:
    """Generate 12 clinics across 3 areas."""
    clinics = []
    clinic_id = 1000

    for area, cities in AREAS.items():
        for city in cities:
            clinic_id += 1
            opened = date(2020, 1, 1) + timedelta(days=rng.randint(0, 365))

            # PLANTED: renamed clinic
            if city == "Riverside":
                clinics.append({
                    "clinic_id": clinic_id,
                    "clinic_name": "Riverside",
                    "city": "Riverside",
                    "state": "CA",
                    "area": area,
                    "opened_date": opened.isoformat(),
                    "closed_date": None,
                    "is_current": False,
                    "renamed_to": "Riverside East",
                    "updated_at": "2024-06-15T00:00:00",
                })
                clinics.append({
                    "clinic_id": clinic_id,
                    "clinic_name": "Riverside East",
                    "city": "Riverside",
                    "state": "CA",
                    "area": area,
                    "opened_date": opened.isoformat(),
                    "closed_date": None,
                    "is_current": True,
                    "renamed_to": None,
                    "updated_at": "2024-06-15T00:00:00",
                })
            elif city == "Portland ME":
                clinics.append({
                    "clinic_id": clinic_id,
                    "clinic_name": f"PawsFirst {city}",
                    "city": "Portland",
                    "state": "ME",
                    "area": area,
                    "opened_date": opened.isoformat(),
                    "closed_date": "2025-03-01",
                    "is_current": True,
                    "renamed_to": None,
                    "updated_at": opened.isoformat() + "T00:00:00",
                })
            else:
                state_map = {
                    "Boston": "MA", "Hartford": "CT", "Providence": "RI",
                    "Atlanta": "GA", "Charlotte": "NC", "Raleigh": "NC",
                    "Nashville": "TN", "Denver": "CO", "Phoenix": "AZ",
                    "Salt Lake City": "UT",
                }
                clinics.append({
                    "clinic_id": clinic_id,
                    "clinic_name": f"PawsFirst {city}",
                    "city": city,
                    "state": state_map.get(city, "XX"),
                    "area": area,
                    "opened_date": opened.isoformat(),
                    "closed_date": None,
                    "is_current": True,
                    "renamed_to": None,
                    "updated_at": opened.isoformat() + "T00:00:00",
                })

    return clinics


def generate_providers(rng: random.Random, clinics: list[dict]) -> list[dict]:
    """Generate ~40 veterinary providers."""
    providers = []
    provider_id = 2000
    active_clinics = [c for c in clinics if c["is_current"]]

    for clinic in active_clinics:
        n_providers = rng.randint(2, 4)
        for _ in range(n_providers):
            provider_id += 1
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            hire_date = date.fromisoformat(clinic["opened_date"]) + timedelta(
                days=rng.randint(0, 180)
            )
            providers.append({
                "provider_id": provider_id,
                "first_name": first,
                "last_name": last,
                "full_name": f"Dr. {first} {last}",
                "credentials": rng.choice(VET_CREDENTIALS),
                "specialty": rng.choice(VET_SPECIALTIES),
                "clinic_id": clinic["clinic_id"],
                "hire_date": hire_date.isoformat(),
                "termination_date": None,
                "is_active": True,
                "updated_at": hire_date.isoformat() + "T00:00:00",
            })

    multi_clinic = rng.sample(providers, min(5, len(providers)))
    for p in multi_clinic:
        other_clinics = [
            c for c in active_clinics if c["clinic_id"] != p["clinic_id"]
        ]
        if other_clinics:
            second = rng.choice(other_clinics)
            providers.append({
                **p,
                "provider_id": p["provider_id"],
                "clinic_id": second["clinic_id"],
                "updated_at": p["updated_at"],
            })

    return providers


def generate_owners(rng: random.Random) -> list[dict]:
    """Generate 1,500 pet owners."""
    owners = []
    for i in range(1, 1501):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        owners.append({
            "owner_id": 3000 + i,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@email.com",
            "phone": f"{rng.randint(200, 999)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}",
            "city": rng.choice(
                [c for area_cities in AREAS.values() for c in area_cities]
            ),
            "state": "XX",
            "created_at": (
                date(2020, 1, 1) + timedelta(days=rng.randint(0, 900))
            ).isoformat() + "T00:00:00",
            "updated_at": (
                date(2020, 1, 1) + timedelta(days=rng.randint(0, 1800))
            ).isoformat() + "T00:00:00",
        })
    return owners


def generate_patients(
    rng: random.Random,
    owners: list[dict],
    clinics: list[dict],
) -> list[dict]:
    """Generate 2,000 patients (animals) with ~5% planted duplicates."""
    patients = []
    patient_id = 5000
    active_clinic_ids = [c["clinic_id"] for c in clinics if c["is_current"]]

    for i in range(1, 1901):
        patient_id += 1
        owner = rng.choice(owners)
        species = rng.choices(SPECIES, weights=SPECIES_WEIGHTS, k=1)[0]

        if species == "Dog":
            breed = rng.choice(DOG_BREEDS)
        elif species == "Cat":
            breed = rng.choice(CAT_BREEDS)
        else:
            breed = None

        name = rng.choice(PET_NAMES)
        birth_date = date(2015, 1, 1) + timedelta(days=rng.randint(0, 3650))
        reg_date = max(
            birth_date + timedelta(days=rng.randint(30, 365)),
            date(2020, 6, 1),
        )
        clinic_id = rng.choice(active_clinic_ids)
        insurance = rng.choice(INSURANCE_PROVIDERS)

        patients.append({
            "patient_id": patient_id,
            "owner_id": owner["owner_id"],
            "patient_name": name,
            "species": species,
            "breed": breed,
            "date_of_birth": birth_date.isoformat(),
            "sex": rng.choice(["M", "F", "M/N", "F/S"]),
            "weight_lbs": round(rng.uniform(2, 120), 1) if species in ("Dog", "Cat") else None,
            "clinic_id": clinic_id,
            "insurance_provider": insurance,
            "registration_date": reg_date.isoformat(),
            "is_active": True,
            "created_at": reg_date.isoformat() + "T00:00:00",
            "updated_at": reg_date.isoformat() + "T00:00:00",
        })

    # PLANTED: duplicate patients
    for _ in range(100):
        original = rng.choice(patients[:1900])
        patient_id += 1

        other_clinics = [c for c in active_clinic_ids if c != original["clinic_id"]]
        dup_clinic = rng.choice(other_clinics) if other_clinics else original["clinic_id"]

        owner = next(o for o in owners if o["owner_id"] == original["owner_id"])
        variant_first = owner["first_name"]
        variant_last = owner["last_name"]
        typo_type = rng.choice(["swap_chars", "drop_char", "add_char"])
        if typo_type == "swap_chars" and len(variant_last) > 2:
            idx = rng.randint(0, len(variant_last) - 2)
            variant_last = (
                variant_last[:idx]
                + variant_last[idx + 1]
                + variant_last[idx]
                + variant_last[idx + 2:]
            )
        elif typo_type == "drop_char" and len(variant_first) > 2:
            idx = rng.randint(1, len(variant_first) - 1)
            variant_first = variant_first[:idx] + variant_first[idx + 1:]
        else:
            variant_last = variant_last + rng.choice(["s", "e", ""])

        dup_owner_id = 3000 + len(owners) + 1
        owners.append({
            "owner_id": dup_owner_id,
            "first_name": variant_first,
            "last_name": variant_last,
            "email": f"{variant_first.lower()}.{variant_last.lower()}{rng.randint(1, 999)}@email.com",
            "phone": owner["phone"],
            "city": owner["city"],
            "state": owner["state"],
            "created_at": original["created_at"],
            "updated_at": original["updated_at"],
        })

        dup_reg_date = date.fromisoformat(original["registration_date"]) + timedelta(
            days=rng.randint(1, 90)
        )
        patients.append({
            "patient_id": patient_id,
            "owner_id": dup_owner_id,
            "patient_name": original["patient_name"],
            "species": original["species"],
            "breed": original["breed"],
            "date_of_birth": original["date_of_birth"],
            "sex": original["sex"],
            "weight_lbs": original["weight_lbs"],
            "clinic_id": dup_clinic,
            "insurance_provider": original["insurance_provider"],
            "registration_date": dup_reg_date.isoformat(),
            "is_active": True,
            "created_at": dup_reg_date.isoformat() + "T00:00:00",
            "updated_at": dup_reg_date.isoformat() + "T00:00:00",
        })

    return patients


def generate_appointments(
    rng: random.Random,
    patients: list[dict],
    providers: list[dict],
    clinics: list[dict],
) -> list[dict]:
    """Generate ~15,000 appointments over 18 months."""
    appointments = []
    appt_id = 10000

    providers_by_clinic: dict[int, list[dict]] = {}
    for p in providers:
        providers_by_clinic.setdefault(p["clinic_id"], []).append(p)

    clinic_opened: dict[int, date] = {}
    for c in clinics:
        if c["is_current"]:
            clinic_opened[c["clinic_id"]] = date.fromisoformat(c["opened_date"])

    start_date = date(2024, 1, 1)
    end_date = date(2025, 6, 30)
    all_dates = _date_range(start_date, end_date)
    weekdays = [d for d in all_dates if d.weekday() < 5]

    for patient in patients:
        n_appts = rng.choices([0, 1, 2, 3, 4, 5, 6, 8, 10], weights=[
            0.05, 0.15, 0.20, 0.20, 0.15, 0.10, 0.07, 0.05, 0.03
        ], k=1)[0]

        for _ in range(n_appts):
            appt_id += 1
            appt_date = rng.choice(weekdays)
            clinic_id = patient["clinic_id"]
            clinic_providers = providers_by_clinic.get(clinic_id, [])

            # PLANTED: null providers
            if rng.random() < 0.03:
                provider_id = None
            elif clinic_providers:
                provider_id = rng.choice(clinic_providers)["provider_id"]
            else:
                provider_id = None

            service = rng.choice(SERVICE_TYPES)
            status = rng.choices(
                ["scheduled", "completed", "cancelled", "no_show"],
                weights=[0.10, 0.70, 0.12, 0.08],
                k=1,
            )[0]

            duration = rng.choice([15, 30, 30, 45, 60, 60, 90]) if status == "completed" else None
            scheduled_at = datetime(
                appt_date.year, appt_date.month, appt_date.day,
                rng.randint(8, 17), rng.choice([0, 15, 30, 45]),
            )
            created_at = scheduled_at - timedelta(days=rng.randint(1, 30))

            # PLANTED: late arrivals
            if rng.random() < 0.05:
                created_at = scheduled_at + timedelta(days=rng.randint(3, 7))

            # PLANTED: impossible dates
            if rng.random() < 0.02 and clinic_id in clinic_opened:
                days_before = rng.randint(30, 180)
                appt_date = clinic_opened[clinic_id] - timedelta(days=days_before)
                scheduled_at = datetime(
                    appt_date.year, appt_date.month, appt_date.day,
                    rng.randint(8, 17), 0,
                )

            updated_at = None
            if status == "completed" and rng.random() < 0.7:
                updated_at = (scheduled_at + timedelta(hours=rng.randint(1, 48))).isoformat()
            elif status in ("cancelled", "no_show"):
                updated_at = (scheduled_at - timedelta(hours=rng.randint(1, 24))).isoformat()

            appointments.append({
                "appointment_id": appt_id,
                "patient_id": patient["patient_id"],
                "provider_id": provider_id,
                "clinic_id": clinic_id,
                "service_code": service[0],
                "service_name": service[1],
                "service_fee": service[2],
                "appointment_date": appt_date.isoformat(),
                "scheduled_at": scheduled_at.isoformat(),
                "status": status,
                "duration_minutes": duration,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at,
            })

    return appointments


def generate_referrals(
    rng: random.Random,
    patients: list[dict],
    clinics: list[dict],
) -> list[dict]:
    """Generate ~800 referrals with funnel stages and planted impossible transitions."""
    referrals = []
    referral_id = 20000
    active_clinic_ids = [c["clinic_id"] for c in clinics if c["is_current"]]

    referred_patients = rng.sample(patients, min(800, len(patients)))

    for patient in referred_patients:
        referral_id += 1
        source = rng.choice(REFERRAL_SOURCES)
        clinic_id = patient["clinic_id"]

        max_stage_idx = rng.choices(
            range(len(FUNNEL_STAGES)),
            weights=[0.15, 0.20, 0.15, 0.35, 0.15],
            k=1,
        )[0]

        stages = []
        base_date = date.fromisoformat(patient["registration_date"]) - timedelta(
            days=rng.randint(7, 60)
        )

        for stage_idx in range(max_stage_idx + 1):
            stage_entered = base_date + timedelta(days=rng.randint(1, 14))
            stage_exited = stage_entered + timedelta(days=rng.randint(1, 30))
            base_date = stage_exited

            stages.append({
                "referral_id": referral_id,
                "patient_id": patient["patient_id"],
                "clinic_id": clinic_id,
                "referral_source": source,
                "stage": FUNNEL_STAGES[stage_idx],
                "stage_entered_at": stage_entered.isoformat() + "T00:00:00",
                "stage_exited_at": stage_exited.isoformat() + "T00:00:00",
                "created_at": stage_entered.isoformat() + "T00:00:00",
                "updated_at": stage_exited.isoformat() + "T00:00:00",
            })

        current_stage = FUNNEL_STAGES[max_stage_idx]

        stages.append({
            "referral_id": referral_id,
            "patient_id": patient["patient_id"],
            "clinic_id": clinic_id,
            "referral_source": source,
            "stage": current_stage,
            "stage_entered_at": (base_date + timedelta(days=1)).isoformat() + "T00:00:00",
            "stage_exited_at": None,
            "created_at": (base_date + timedelta(days=1)).isoformat() + "T00:00:00",
            "updated_at": (base_date + timedelta(days=1)).isoformat() + "T00:00:00",
        })

        referrals.extend(stages)

    # PLANTED: backwards transitions
    n_backwards = int(len(referrals) * 0.10)
    backwards_indices = rng.sample(range(len(referrals)), min(n_backwards, len(referrals)))
    for idx in backwards_indices:
        current_stage = referrals[idx]["stage"]
        current_order = FUNNEL_STAGE_ORDER[current_stage]
        if current_order > 0:
            backwards_stage = FUNNEL_STAGES[rng.randint(0, current_order - 1)]
            referrals[idx]["stage"] = backwards_stage

    return referrals


def generate_invoices(
    rng: random.Random,
    appointments: list[dict],
) -> list[dict]:
    """Generate invoices tied to completed appointments."""
    invoices = []
    invoice_id = 30000

    completed = [a for a in appointments if a["status"] == "completed"]

    for appt in completed:
        invoice_id += 1
        base_fee = appt["service_fee"]
        amount = round(base_fee * rng.uniform(0.9, 1.2), 2)

        payment_type = rng.choices(
            ["insurance", "self_pay"],
            weights=[0.65, 0.35],
            k=1,
        )[0]

        insurance_paid = round(amount * rng.uniform(0.6, 0.9), 2) if payment_type == "insurance" else 0.0
        patient_paid = round(amount - insurance_paid, 2)

        invoice_date = date.fromisoformat(appt["appointment_date"])
        paid_date = invoice_date + timedelta(days=rng.randint(0, 45))

        invoices.append({
            "invoice_id": invoice_id,
            "appointment_id": appt["appointment_id"],
            "patient_id": appt["patient_id"],
            "clinic_id": appt["clinic_id"],
            "service_code": appt["service_code"],
            "amount": amount,
            "payment_type": payment_type,
            "insurance_paid": insurance_paid,
            "patient_paid": patient_paid,
            "invoice_date": invoice_date.isoformat(),
            "paid_date": paid_date.isoformat(),
            "status": rng.choices(
                ["paid", "partial", "outstanding"],
                weights=[0.80, 0.10, 0.10],
                k=1,
            )[0],
            "created_at": invoice_date.isoformat() + "T00:00:00",
            "updated_at": paid_date.isoformat() + "T00:00:00",
        })

    return invoices


def generate_budget_targets(
    rng: random.Random,
    clinics: list[dict],
) -> list[dict]:
    """Generate monthly budget targets: 12 clinics x 12 months."""
    targets = []
    active_clinics = [c for c in clinics if c["is_current"]]

    for clinic in active_clinics:
        base_revenue = rng.uniform(30000, 80000)
        base_appts = rng.randint(80, 200)

        for month in range(1, 13):
            seasonal = 1.0 + 0.1 * rng.uniform(-1, 1)
            targets.append({
                "clinic_id": clinic["clinic_id"],
                "clinic_name": clinic["clinic_name"],
                "year": 2024,
                "month": month,
                "target_revenue": round(base_revenue * seasonal, 2),
                "target_appointments": int(base_appts * seasonal),
                "target_new_patients": rng.randint(8, 25),
            })

    return targets


def write_csv(filename: str, data: list[dict]) -> int:
    """Write list of dicts to CSV. Returns row count."""
    if not data:
        return 0

    filepath = DATA_DIR / filename
    fieldnames = list(data[0].keys())

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    return len(data)


def main() -> None:
    """Generate all seed data."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rng = _rng()

    print("Generating PawsFirst Veterinary Network seed data...")
    print(f"Output directory: {DATA_DIR}")
    print(f"Random seed: {SEED}")
    print()

    clinics = generate_clinics(rng)
    count = write_csv("clinics.csv", clinics)
    print(f"  clinics.csv: {count} rows")

    providers = generate_providers(rng, clinics)
    count = write_csv("providers.csv", providers)
    print(f"  providers.csv: {count} rows")

    owners = generate_owners(rng)

    patients = generate_patients(rng, owners, clinics)
    count = write_csv("patients.csv", patients)
    print(f"  patients.csv: {count} rows")

    count = write_csv("owners.csv", owners)
    print(f"  owners.csv: {count} rows")

    appointments = generate_appointments(rng, patients, providers, clinics)
    count = write_csv("appointments.csv", appointments)
    print(f"  appointments.csv: {count} rows")

    referrals = generate_referrals(rng, patients, clinics)
    count = write_csv("referrals.csv", referrals)
    print(f"  referrals.csv: {count} rows")

    invoices = generate_invoices(rng, appointments)
    count = write_csv("invoices.csv", invoices)
    print(f"  invoices.csv: {count} rows")

    budget_targets = generate_budget_targets(rng, clinics)
    count = write_csv("budget_targets.csv", budget_targets)
    print(f"  budget_targets.csv: {count} rows")

    print()
    print("Done! Seed data generated successfully.")


if __name__ == "__main__":
    main()
