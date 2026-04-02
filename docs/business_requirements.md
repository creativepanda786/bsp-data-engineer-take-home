# PawsFirst Veterinary Network — Business Requirements

These requirements come from PawsFirst stakeholders. Your job is to design and build the gold layer of the data warehouse to answer these questions.

**You are not expected to complete all six.** Prioritize depth over breadth — a well-modeled, well-tested solution for 3-4 requirements is better than a shallow attempt at all 6.

---

## Requirement 1: Clinic Appointment Volume (Clinic Operations Director)

> "I need weekly appointment volume and completion rates by clinic, with the ability to filter by region and area. Show me which clinics are underperforming."

**Key metrics:**
- Total appointments per week per clinic
- Completion rate (completed / total)
- Cancellation rate
- No-show rate
- Ability to filter/group by area and region

**Expected grain:** One row per clinic per week.

---

## Requirement 2: Referral Funnel Analysis (CEO)

> "Give me a referral funnel — how many inquiries, consultations, registrations, and active patients by month and by clinic. I want to see conversion rates between stages and median days between each stage."

**Key metrics:**
- Count of referrals at each funnel stage per month per clinic
- Conversion rate between consecutive stages
- Median days spent in each stage
- Breakdown by referral source

**Note:** Be thoughtful about how you handle the stage transition data. Not all transitions in the source data are valid.

---

## Requirement 3: Revenue vs Budget (Finance Lead)

> "I need revenue by clinic by month, split by insurance vs self-pay. Include the ability to compare actuals against budget targets."

**Key metrics:**
- Total revenue per clinic per month
- Revenue split: insurance-paid vs patient-paid (self-pay)
- Budget target comparison (actual vs target, variance)
- Collection rate (paid / invoiced)

**Reference data:** Budget targets are provided in `budget_targets.csv`.

---

## Requirement 4: Provider Utilization (Medical Director)

> "I want to see provider utilization — appointments per provider per week, broken out by service type. Flag providers below a minimum threshold."

**Key metrics:**
- Appointments per provider per week
- Breakdown by service type (wellness, surgery, dental, etc.)
- Total minutes per provider per week
- Flag: providers with < 15 appointments per week

---

## Requirement 5: Duplicate Patient Detection (Operations Analyst)

> "We keep getting duplicate patient records when owners register at multiple clinics. I need a view that identifies likely duplicates for manual review."

**Matching signals to consider:**
- Same patient name + same species + same date of birth
- Same owner phone number across different owner records
- Same patient name + similar owner last name (fuzzy match)

**Expected output:** A view showing pairs of likely-duplicate patient records with a reason for the match.

---

## Requirement 6: Patient Retention Cohort (VP of Growth)

> "Show me patient retention — of patients who had their first appointment in a given month, what percentage returned within 30, 60, and 90 days? Break it down by clinic and referral source."

**Key metrics:**
- Cohort: patients by their first-appointment month
- Retention at 30, 60, 90 days (percentage who had a subsequent appointment)
- Breakdown by clinic and referral source
- Cohort size (number of new patients per month)
