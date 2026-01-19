# credit-abs-oltp-to-mart

Synthetic credit OLTP → dbt analytics mart pipeline to compute ABS-like credit risk metrics
 (DPD / NPL / Roll-rate / Cure-rate / Vintage).

# What this project does
1) Generates synthetic data in PostgreSQL (credit_oltp schema)  
2) Builds dbt staging + marts in PostgreSQL (credit_mart schema)  
3) Exposes analytics-friendly views for reporting / BI

# Tech stack
- PostgreSQL (OLTP + Analytics schemas)
- dbt (models + tests)
- (Optional) Airflow (orchestration)

# Architecture (simple)
- Source / OLTP:credit_oltp. (loan_contract, repayment_schedule, repayment_payment, arrears_dpd_status,
    write_off_and_recovery, collections_case)
- dbt staging:credit_mart.stg (clean/standardize types & keys)
- dbt marts: credit_mart.fct (metrics & aggregates)

# Examples
- DPD buckets: 1–30, 31–60, 61–90, 90+ days past due  
- NPL ratio:npl_exposure / total_exposure
- Roll-rate: transition rate between delinquency buckets month-to-month
- Cure rate: delinquent → performing recovery rate
- Vintage / MOB: cohort delinquency performance by origination month



Application Table - Data Quality Audit (Deutsch)
[ok ] Frage 1: Chronologische Logik Problem: application_date (2016) < created_at (2026). 
select * from application  where created_at >application_date ;
select count(*) as FUTURE_DATE from application where created_at >application_date ; (6000)
Lösung: Realistische Zeitverzögerung implementieren. 
created_at wird zufällig zwischen 3 Stunden (für Bestandskunden) und 72 Stunden (maximale Bearbeitungszeit) nach dem application_date gesetzt.

[ ] Frage 2: Prüfung auf zukünftige Daten Problem: Zeilen, in denen application_date > CURRENT_DATE ist. 
Lösung: Die Daten im Generator-Code auf das aktuelle Tagesdatum begrenzen.

[ ] Frage 3: Batch-Insert Duplikate Problem: Alle created_at Werte liegen in der gleichen Sekunde. 
Lösung: Für jede Zeile einen unterschiedlichen (randomisierten) Zeitstempel erstellen.

[ ] Frage 4: ID-Reihenfolge (ID Gaps) Problem: Prüfung auf Lücken in der Spalte application_id. Lösung: Überprüfung, ob der Spaltentyp "Sequence" oder "Identity" korrekt funktioniert.
## Chronologische Datenlogik (Chronological Logic)

Um die Datenrealität zu erhöhen, wurde eine spezifische Zeitlogik für die Tabellen `application` und `borrower` implementiert. 


- Das Erstellungsdatum created_at muss immer **vor** dem offiziellen Antragsdatum application_dateliegen.
- Es wurde ein zufälliger Zeitabstand von 3 bis 72 Stunden zwischen der Systemerfassung und dem formalen Antrag definiert.
- Dies verhindert, dass synthetische Daten durch Standard-Datenbankwerte wie now() unrealistisch wirken.

Code-Implementierung:

python
# Logik zur Generierung realistischer Zeitstempel
for _ in range(cfg.n_applications):
    # 1. Zufälliges Antragsdatum wählen
    app_dt = fake.date_between_dates(date_start=cfg.start_date_min, date_end=cfg.start_date_max)
    app_dt_full = datetime.combine(app_dt, time(random.randint(0, 23), random.randint(0, 59)))

    # 2. Created_at berechnen (3 bis 72 Stunden vor application_date)
    random_gap = random.randint(3, 72)
    created_at = app_dt_full - timedelta(hours=random_gap)

    rows.append((app_dt_full, created_at))





