from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from faker import Faker

try:
    from psycopg2.extras import execute_values  # type: ignore
except Exception:
    execute_values = None  # fallback to slow inserts


# ----------------------------
# Config
# ----------------------------
@dataclass
class OLTPSynthConfig:
    schema: str = "credit_oltp"

    # volume
    n_borrowers: int = 2000
    n_applications: int = 3000
    n_loans: int = 1500

    # realism controls
    start_date_min: date = date(2015, 1, 1)
    start_date_max: date = date.today()  # origination upper bound
    max_term_months: int = 72

    # ids - make them >= 5 digits
    min_borrower_id: int = 10000
    min_application_id: int = 100000000

    # behavior distributions
    p_variable_rate: float = 0.35
    p_direct_debit: float = 0.55
    p_late_installment: float = 0.18          # some installments become late
    p_partial_payment: float = 0.10
    p_default: float = 0.03                   # loans that default eventually
    p_forbearance: float = 0.05

    # rates / fees
    annual_rate_min: float = 0.03
    annual_rate_max: float = 0.22
    penalty_rate_annual: float = 0.12
    late_fee_amount_min: float = 5.0
    late_fee_amount_max: float = 40.0

    # snapshots (to avoid huge data)
    build_daily_snapshots: bool = True
    snapshot_days_per_loan: int = 180         # daily snapshots only for first N days from origination
    commit_every: int = 2000

    faker_locale: str = "de_DE"
    seed: int = 42


# ----------------------------
# Helpers
# ----------------------------
def _month_add(d: date, months: int) -> date:
    # simple month add keeping day-of-month if possible
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    # clamp day
    last_day = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    day = min(d.day, last_day)
    return date(y, m, day)


def _bucket_from_dpd(dpd: int) -> str:
    if dpd <= 0:
        return "0"
    if dpd <= 30:
        return "1-30"
    if dpd <= 60:
        return "31-60"
    if dpd <= 90:
        return "61-90"
    return ">90"


def _annuity_payment(principal: float, annual_rate: float, term_months: int) -> float:
    r = annual_rate / 12.0
    if r <= 0:
        return principal / term_months
    return principal * (r * (1 + r) ** term_months) / ((1 + r) ** term_months - 1)


def _daily_rate(annual_rate: float, day_count_base: int = 365) -> float:
    return annual_rate / float(day_count_base)


def _ensure_sequences(cur, cfg: OLTPSynthConfig):
    # make borrower/application ids start from desired minimums (best effort)
    # identity sequences in PG usually named: <table>_<col>_seq
    # If already larger, do nothing.
    seqs = [
        (f"{cfg.schema}.borrower_borrower_id_seq", cfg.min_borrower_id),
        (f"{cfg.schema}.application_application_id_seq", cfg.min_application_id),
    ]
    for seq_name, minv in seqs:
        try:
            cur.execute(f"SELECT last_value FROM {seq_name};")
            lastv = int(cur.fetchone()[0])
            if lastv < minv:
                cur.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH {minv};")
        except Exception:
            # sequence name might differ if table created differently; ignore
            pass


def _exec_values(cur, sql: str, rows: List[Tuple], page_size: int = 1000, fetch_returning: bool = False) -> List[Tuple]:
    if not rows:
        return []
    if execute_values is None:
        # fallback slow
        out = []
        for r in rows:
            cur.execute(sql, r)
            if fetch_returning:
                out.append(cur.fetchone())
        return out

    tpl = "(" + ",".join(["%s"] * len(rows[0])) + ")"
    # sql must contain VALUES %s
    if fetch_returning:
      return list(execute_values(cur, sql, rows,
     template=tpl, page_size=page_size, fetch=True) or 
     [])
    else:
        execute_values(cur, sql, rows, template=tpl,
     page_size=page_size, fetch=False)
    return []

# ----------------------------
# Main runner
# ----------------------------
def run_pg_credit_oltp_synth(postgres_conn_id: str, cfg: Optional[OLTPSynthConfig] = None) -> Dict[str, int]:
    cfg = cfg or OLTPSynthConfig()
    random.seed(cfg.seed)
    fake = Faker(cfg.faker_locale)
    Faker.seed(cfg.seed)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    # best effort set sequences
    _ensure_sequences(cur, cfg)
    conn.commit()

    # 1) borrowers
    borrower_ids = _insert_borrowers(cur, cfg, fake)
    conn.commit()

    # 2) applications
    application_ids = _insert_applications(cur, cfg, fake)
    conn.commit()

    # 3) loans + (optional) variable rate schedule + disbursement
    loan_rows = _insert_loans(cur, cfg, fake, borrower_ids, application_ids)
    conn.commit()

    loan_ids = [r[0] for r in loan_rows]
    loan_meta = {r[0]: r for r in loan_rows}  # loan_id -> tuple with metadata

    _insert_disbursements(cur, cfg, fake, loan_meta)
    _insert_interest_rate_schedule(cur, cfg, fake, loan_meta)
    conn.commit()

    # 4) repayment schedules
    schedule_map = _insert_repayment_schedules(cur, cfg, loan_meta)
    conn.commit()

    # 5) payments + allocations + fees/penalties + arrears + balance snapshots + other entities
    stats = _insert_events_and_snapshots(cur, cfg, fake, loan_meta, schedule_map)
    conn.commit()

    cur.close()
    conn.close()

    return {
        "borrowers": len(borrower_ids),
        "applications": len(application_ids),
        "loans": len(loan_ids),
        **stats,
    }


# ----------------------------
# Insert blocks
# ----------------------------
def _insert_borrowers(cur, cfg: OLTPSynthConfig, fake):
    sql = f"""
    INSERT INTO {cfg.schema}.borrower (created_at)
    SELECT now()
    FROM generate_series(1, %s)
    RETURNING borrower_id;
    """
    cur.execute(sql, (cfg.n_borrowers,))
    ret = cur.fetchall()
    return [int(r[0]) for r in ret]


def _insert_applications(cur, cfg: OLTPSynthConfig, fake: Faker) -> List[int]:
    # application_date between start_date_min and today
    rows = []
    for _ in range(cfg.n_applications):
        app_dt = fake.date_between_dates(date_start=cfg.start_date_min, date_end=cfg.start_date_max)
        rows.append((app_dt,))
    sql = f"""
    INSERT INTO {cfg.schema}.application (application_date)
    VALUES %s
    RETURNING application_id;
    """
    ret = _exec_values(cur, sql, rows, page_size=1000, fetch_returning=True)
    return [int(r[0]) for r in ret]


def _insert_loans(cur, cfg: OLTPSynthConfig, fake: Faker, borrower_ids: List[int], application_ids: List[int]) -> List[Tuple]:
    currencies = ["EUR", "USD", "GBP", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK"]
    products = ["consumer_loan", "secured_consumer_loan", "home_improvement", "buy_to_let", "auto_loan", "education_loan"]
    day_counts = ["ACT/365", "ACT/360", "30/360"]
    repayment_methods = ["annuity", "linear", "interest_only", "balloon"]
    pay_freqs = ["monthly", "weekly"]

    rows = []
    for _ in range(cfg.n_loans):
        borrower_id = random.choice(borrower_ids)
        application_id = random.choice(application_ids)

        product_type = random.choice(products)
        currency = random.choice(currencies)

        origination = fake.date_between_dates(date_start=cfg.start_date_min, date_end=cfg.start_date_max)
        disb_date = origination + timedelta(days=random.randint(0, 7))
        term = random.randint(6, cfg.max_term_months)
        maturity = _month_add(origination, term)

        principal = round(random.uniform(500, 50000), 2)
        annual_rate = random.uniform(cfg.annual_rate_min, cfg.annual_rate_max)
        rate_type = "variable" if random.random() < cfg.p_variable_rate else "fixed"
        rate_index = "EURIBOR" if rate_type == "variable" else None
        margin = round(random.uniform(0.005, 0.05), 6) if rate_type == "variable" else None

        repayment_method = random.choice(repayment_methods)
        payment_frequency = "monthly"  # keep consistent for schedule generator
        if random.random() < 0.10:
            payment_frequency = random.choice(pay_freqs)

        day_count = random.choice(day_counts)
        grace = 0 if random.random() < 0.85 else random.choice([1, 2, 3])

        pay_day = random.randint(1, 28)
        # installment amount for annuity/linear; keep blank for interest_only/balloon sometimes
        if repayment_method == "annuity":
            inst = _annuity_payment(principal, annual_rate, term)
        elif repayment_method == "linear":
            inst = principal / term + (principal * (annual_rate / 12.0))  # rough
        else:
            inst = None

        apr_eff = annual_rate + (random.uniform(0.0, 0.03))  # rough
        status = "active"

        rows.append((
            application_id, borrower_id,
            product_type, currency,
            origination, disb_date, maturity,
            principal, principal,
            term,
            rate_type, rate_index, margin, round(annual_rate, 6),
            round(apr_eff, 6), day_count,
            payment_frequency, repayment_method,
            round(inst, 2) if inst is not None else None,
            pay_day, grace,
            status
        ))

    sql = f"""
    INSERT INTO {cfg.schema}.loan_contract (
      application_id, borrower_id,
      product_type, currency,
      origination_date, disbursement_date, maturity_date,
      principal_original, principal_current,
      term_months,
      interest_rate_type, interest_rate_index, interest_rate_margin, interest_rate_current,
      apr_effective, day_count_convention,
      payment_frequency, repayment_method,
      installment_amount, payment_day_of_month, grace_period_months,
      status
    )
    VALUES %s
    RETURNING loan_id,
              borrower_id, application_id,
              currency, origination_date, disbursement_date, maturity_date,
              principal_original, term_months,
              interest_rate_type, interest_rate_current,
              repayment_method, payment_frequency, grace_period_months;
    """
    ret = _exec_values(cur, sql, rows, page_size=1000, fetch_returning=True)
    # returned tuple: loan_id + meta fields
    return ret


def _insert_disbursements(cur, cfg: OLTPSynthConfig, fake: Faker, loan_meta: Dict[int, Tuple]):
    methods = ["bank_transfer", "cash", "internal"]
    rows = []
    for loan_id, meta in loan_meta.items():
        # meta: loan_id, borrower_id, application_id, currency, orig, disb, maturity, principal, term, rate_type, rate, repayment_method, payment_frequency, grace
        currency = meta[3]
        disb_date = meta[5]
        principal = float(meta[7])

        rows.append((
            loan_id, 1,
            disb_date, principal, currency,
            random.choice(methods),
            "DE** **** **** **** **** **"  # masked
        ))

    sql = f"""
    INSERT INTO {cfg.schema}.loan_disbursement (
      loan_id, disbursement_seq_no,
      disbursement_date, disbursement_amount, currency,
      disbursement_method,
      payout_account_iban_masked,
      status
    )
    VALUES %s
    """
    # add status column value in rows by extending template: simplest: include status in tuples
    rows2 = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], "settled") for r in rows]
    _exec_values(cur, sql, rows2, page_size=1000, fetch_returning=False)


def _insert_interest_rate_schedule(cur, cfg: OLTPSynthConfig, fake: Faker, loan_meta: Dict[int, Tuple]):
    rows = []
    for loan_id, meta in loan_meta.items():
        rate_type = meta[9]
        if rate_type != "variable":
            continue
        orig = meta[4]
        maturity = meta[6]
        base_rate = float(meta[10])

        # create 1-3 rate change events
        n_events = random.randint(1, 3)
        dates = sorted(
            fake.date_between_dates(date_start=orig, date_end=min(maturity, date.today()))
            for _ in range(n_events)
        )
        for i, eff_from in enumerate(dates):
            eff_to = dates[i + 1] - timedelta(days=1) if i + 1 < len(dates) else None
            nominal = max(0.0, base_rate + random.uniform(-0.02, 0.03))
            rows.append((
                loan_id,
                eff_from, eff_to,
                "variable",
                "EURIBOR", random.choice(["1M", "3M", "6M"]),
                round(random.uniform(0.005, 0.05), 6),
                round(nominal, 6),
                "market"
            ))

    if not rows:
        return

    sql = f"""
    INSERT INTO {cfg.schema}.interest_rate_schedule (
      loan_id,
      effective_from_date, effective_to_date,
      rate_type,
      index_name, index_tenor,
      margin,
      nominal_rate,
      rate_source
    )
    VALUES %s
    """
    _exec_values(cur, sql, rows, page_size=1000, fetch_returning=False)


def _insert_repayment_schedules(cur, cfg: OLTPSynthConfig, loan_meta: Dict[int, Tuple]) -> Dict[int, List[Tuple]]:
    """
    Returns map: loan_id -> list of (installment_no, due_date, principal_due, interest_due, fees_due, total_due)
    """
    schedule_map: Dict[int, List[Tuple]] = {}

    rows = []
    for loan_id, meta in loan_meta.items():
        currency = meta[3]
        orig = meta[4]
        principal = float(meta[7])
        term = int(meta[8])
        rate_type = meta[9]
        annual_rate = float(meta[10])
        repay_method = meta[11]
        grace = int(meta[13])

        # payment start after grace months
        first_due = _month_add(orig, 1 + grace)

        # compute schedule
        bal = principal
        if repay_method == "annuity":
            pmt = _annuity_payment(principal, annual_rate, term)
        elif repay_method == "linear":
            pmt = None
        elif repay_method == "interest_only":
            pmt = None
        else:
            pmt = None

        loan_lines: List[Tuple] = []
        for n in range(1, term + 1):
            due = _month_add(first_due, n - 1)
            monthly_rate = annual_rate / 12.0
            interest = bal * monthly_rate

            if repay_method == "annuity":
                principal_due = max(0.0, pmt - interest)
            elif repay_method == "linear":
                principal_due = principal / term
            elif repay_method == "interest_only":
                principal_due = 0.0
            elif repay_method == "balloon":
                principal_due = 0.0 if n < term else bal
            else:
                principal_due = 0.0

            fees = 0.0
            total = principal_due + interest + fees
            opening = bal
            closing = max(0.0, bal - principal_due)

            loan_lines.append((n, due, round(principal_due, 2), round(interest, 2), round(fees, 2), round(total, 2), round(opening, 2), round(closing, 2)))
            bal = closing

        schedule_map[loan_id] = loan_lines

        for (inst_no, due, pr_due, int_due, fee_due, total_due, opening, closing) in loan_lines:
            rows.append((
                loan_id,
                inst_no, due, currency,
                pr_due, int_due, fee_due, 0.0,
                total_due,
                opening, closing,
                "planned",
                1
            ))

    sql = f"""
    INSERT INTO {cfg.schema}.repayment_schedule (
      loan_id,
      installment_no, due_date, currency,
      principal_due, interest_due, fees_due, penalty_interest_due,
      total_due,
      opening_principal_balance, closing_principal_balance,
      schedule_status,
      schedule_version
    )
    VALUES %s
    """
    _exec_values(cur, sql, rows, page_size=2000, fetch_returning=False)
    return schedule_map


def _insert_events_and_snapshots(cur, cfg: OLTPSynthConfig, fake: Faker, loan_meta: Dict[int, Tuple], schedule_map: Dict[int, List[Tuple]]) -> Dict[str, int]:
    payment_rows = []
    alloc_rows = []
    arrears_rows = []
    fee_rows = []
    penalty_rows = []
    mandate_rows = []
    instr_rows = []
    forbear_rows = []
    case_rows = []
    writeoff_rows = []
    bal_snap_rows = []
    audit_rows = []

    # helpers for quick lookups
    def log(entity_type: str, entity_id: str, event_type: str, notes: str = ""):
        audit_rows.append((entity_type, entity_id, event_type, datetime.utcnow(), "system", "synth", None, None, notes))

    # choose some defaulted loans
    loan_ids = list(loan_meta.keys())
    defaulted_set = set(random.sample(loan_ids, k=max(1, int(len(loan_ids) * cfg.p_default)))) if loan_ids else set()

    # build mandates for some loans
    for loan_id, meta in loan_meta.items():
        borrower_id = int(meta[1])
        if random.random() < cfg.p_direct_debit:
            mandate_ref = f"DD-{loan_id}-{random.randint(1000,9999)}"
            sign_dt = meta[4]  # origination
            mandate_rows.append((
                borrower_id, loan_id,
                mandate_ref, sign_dt,
                "active", "RCUR",
                fake.name(),
                "DE** **** **** **** **** **",
                None,
                "DE98ZZZ00000000000",
                "Demo Bank",
                random.randint(1, 28)
            ))
            log("mandate", mandate_ref, "created", "direct debit mandate")

    # insert mandates now to get mandate_id for instructions (optional)
    mandate_id_by_loan: Dict[int, int] = {}
    if mandate_rows:
        sql_mand = f"""
        INSERT INTO {cfg.schema}.direct_debit_mandate (
          borrower_id, loan_id,
          mandate_reference, mandate_signature_date,
          mandate_status, sequence_type,
          debtor_name,
          debtor_iban_masked,
          debtor_bic,
          creditor_id, creditor_name,
          requested_collection_day
        )
        VALUES %s
        RETURNING mandate_id, loan_id;
        """
        ret = _exec_values(cur, sql_mand, mandate_rows, page_size=1000, fetch_returning=True)
        for mandate_id, loan_id in ret:
            mandate_id_by_loan[int(loan_id)] = int(mandate_id)

    # payments + allocations + arrears / penalties / fees / instructions
    # We'll generate payments per installment with some late/partial behavior.
    for loan_id, meta in loan_meta.items():
        borrower_id = int(meta[1])
        currency = meta[3]
        orig = meta[4]
        maturity = meta[6]
        annual_rate = float(meta[10])

        lines = schedule_map[loan_id]
        outstanding_principal = float(meta[7])

        in_default = loan_id in defaulted_set
        default_at: Optional[date] = None
        arrears_start: Optional[date] = None

        # choose default moment if defaulted: somewhere after 30% of term
        if in_default and lines:
            default_line = random.randint(max(1, int(len(lines) * 0.3)), len(lines))
            default_at = lines[default_line - 1][1] + timedelta(days=random.randint(60, 150))

        # instruction per installment (if mandate exists)
        mandate_id = mandate_id_by_loan.get(loan_id)

        for inst_no, due, pr_due, int_due, fee_due, total_due, opening, closing in lines:
            # stop if default date passed (simulate no further regular payments)
            if default_at and due > default_at:
                break

            # create collection instruction (optional)
            if mandate_id:
                instr_rows.append((
                    loan_id,
                    None,  # schedule_id optional (we don't fetch schedule_id here)
                    mandate_id,
                    f"MSG-{loan_id}-{inst_no}",
                    f"PINF-{loan_id}-{inst_no}",
                    due,
                    float(total_due),
                    currency,
                    "DE** **** **** **** **** **",
                    "DE98ZZZ00000000000",
                    f"E2E-{loan_id}-{inst_no}",
                    f"Installment {inst_no}",
                    "sent"
                ))

            # decide payment behavior
            late = random.random() < cfg.p_late_installment
            partial = (random.random() < cfg.p_partial_payment) and not late

            # if loan is defaulted, increase late probability
            if in_default and default_at and due > (default_at - timedelta(days=120)):
                late = True
                partial = False

            # payment dates: never before due (bank logic)
            if late:
                days_late = random.randint(1, 90)
                pay_date = due + timedelta(days=days_late)
                if arrears_start is None:
                    arrears_start = due + timedelta(days=1)
            else:
                pay_date = due  # on time

            # if defaulted, sometimes skip payment entirely near default date
            if in_default and default_at and pay_date >= default_at:
                # skip
                continue

            amount_received = float(total_due)
            if partial:
                amount_received = round(amount_received * random.uniform(0.3, 0.8), 2)

            # compute penalty & late fee if late
            late_fee = 0.0
            penalty_amt = 0.0
            if late:
                late_fee = round(random.uniform(cfg.late_fee_amount_min, cfg.late_fee_amount_max), 2)
                # penalty accrual on overdue total (rough daily)
                dr = _daily_rate(cfg.penalty_rate_annual, 365)
                penalty_amt = round((float(total_due) * dr * max(1, (pay_date - due).days)), 2)

                # record fee and penalty event
                fee_rows.append((
                    loan_id, "late_fee", due, pay_date, currency, late_fee, None,
                    "assessed", None, None
                ))
                penalty_rows.append((
                    loan_id, due, pay_date,
                    cfg.penalty_rate_annual, currency, penalty_amt,
                    False, None
                ))

            # payment record
            status = "received"
            payment_rows.append((
                loan_id,
                pay_date, pay_date,
                currency,
                amount_received,
                "direct_debit" if mandate_id else random.choice(["bank_transfer", "cash", "card", "internal"]),
                f"EXT-{loan_id}-{inst_no}-{random.randint(100000,999999)}",
                None,
                status,
                None, None
            ))

            # allocation rule: fees+penalty -> interest -> principal
            remaining = amount_received

            alloc_penalty = min(remaining, penalty_amt)
            remaining -= alloc_penalty

            alloc_fees = min(remaining, late_fee)
            remaining -= alloc_fees

            alloc_interest = min(remaining, float(int_due))
            remaining -= alloc_interest

            alloc_principal = min(remaining, float(pr_due))
            remaining -= alloc_principal

            alloc_other = max(0.0, remaining)

            # We will link allocations after payment_id is known (need RETURNING). We'll do that in batch later.

            # arrears daily snapshots (small window)
            # We'll compute dpd status only around due->pay_date window (or unpaid)
            if cfg.build_daily_snapshots:
                # create daily arrears rows from due to min(pay_date, due+snapshot_days_per_loan)
                end = min(pay_date, due + timedelta(days=cfg.snapshot_days_per_loan))
                cur_day = due
                oldest_unpaid = due if late else None
                while cur_day <= end:
                    dpd = max(0, (cur_day - due).days) if late and cur_day > due else 0
                    bucket = _bucket_from_dpd(dpd)
                    past_total = float(total_due) if dpd > 0 else 0.0
                    arrears_rows.append((
                        loan_id,
                        cur_day,
                        dpd,
                        past_total,
                        float(pr_due) if dpd > 0 else 0.0,
                        float(int_due) if dpd > 0 else 0.0,
                        (late_fee + penalty_amt) if dpd > 0 else 0.0,
                        oldest_unpaid,
                        bucket,
                        True if 5 <= dpd <= 30 else False,
                        True if (default_at and cur_day >= default_at) else False,
                        True if dpd > 90 else False,
                        False,
                        None
                    ))
                    cur_day += timedelta(days=1)

            log("loan", str(loan_id), "installment_processed", f"inst={inst_no} due={due} pay={pay_date} late={late}")

        # if defaulted, create collections + writeoff optionally
        if in_default and default_at:
            case_open = default_at + timedelta(days=random.randint(10, 40))
            case_rows.append((
                loan_id,
                case_open,
                random.choice(["agent_1", "agent_2", "legal_team"]),
                random.choice(["soft", "hard", "legal"]),
                case_open + timedelta(days=random.randint(5, 20)),
                case_open + timedelta(days=random.randint(21, 45)),
                random.choice(["promise_to_pay", "no_contact", "legal_notice"]),
                None, None
            ))

    # Insert payments with RETURNING so we can insert allocations
    payment_id_by_idx: List[int] = []
    if payment_rows:
        sql_pay = f"""
        INSERT INTO {cfg.schema}.repayment_payment (
          loan_id,
          payment_date, value_date,
          currency,
          amount_received,
          payment_channel,
          external_reference,
          bank_statement_entry_id,
          status,
          return_reason_code,
          reversal_reference
        )
        VALUES %s
        RETURNING payment_id;
        """
        ret = _exec_values(cur, sql_pay, payment_rows, page_size=2000, fetch_returning=True)
        payment_id_by_idx = [int(r[0]) for r in ret]

        # Build allocations in same order as payment_rows
        # NOTE: we recompute allocations (same logic) because we didn't store them per row.
        alloc_rows = []
        for idx, pay in enumerate(payment_rows):
            loan_id = int(pay[0])
            amount_received = float(pay[4])

            # we don't have exact penalty/fee per installment here; approximate:
            # allocate 5% fees, 15% interest, rest principal (prod-like-ish)
            alloc_fees = round(amount_received * random.uniform(0.00, 0.08), 2)
            alloc_interest = round(amount_received * random.uniform(0.05, 0.25), 2)
            alloc_penalty = round(amount_received * random.uniform(0.00, 0.05), 2)
            alloc_principal = max(0.0, round(amount_received - (alloc_fees + alloc_interest + alloc_penalty), 2))

            alloc_rows.append((
                payment_id_by_idx[idx],
                loan_id,
                alloc_principal,
                alloc_interest,
                alloc_fees,
                alloc_penalty,
                0.0,
                "system"
            ))

        sql_alloc = f"""
        INSERT INTO {cfg.schema}.payment_allocation (
          payment_id, loan_id,
          allocated_principal,
          allocated_interest,
          allocated_fees,
          allocated_penalty_interest,
          allocated_other,
          allocation_rule
        )
        VALUES %s
        """
        _exec_values(cur, sql_alloc, alloc_rows, page_size=2000, fetch_returning=False)

    # arrears
    if arrears_rows:
        sql_arr = f"""
        INSERT INTO {cfg.schema}.arrears_dpd_status (
          loan_id,
          as_of_date,
          days_past_due,
          past_due_amount_total,
          past_due_principal,
          past_due_interest,
          past_due_fees,
          oldest_unpaid_due_date,
          arrears_bucket,
          early_arrears_flag,
          default_flag,
          nonperforming_flag,
          probation_flag,
          cure_date
        )
        VALUES %s
        ON CONFLICT (loan_id, as_of_date) DO NOTHING
        """
        _exec_values(cur, sql_arr, arrears_rows, page_size=5000, fetch_returning=False)

    # fees
    if fee_rows:
        sql_fee = f"""
        INSERT INTO {cfg.schema}.fees_and_charges (
          loan_id,
          fee_type,
          assessed_date,
          due_date,
          currency,
          amount,
          tax_amount,
          status,
          related_payment_id,
          waiver_reason_code
        )
        VALUES %s
        """
        _exec_values(cur, sql_fee, fee_rows, page_size=2000, fetch_returning=False)

    # penalty
    if penalty_rows:
        sql_pen = f"""
        INSERT INTO {cfg.schema}.penalty_interest_events (
          loan_id,
          accrual_from_date,
          accrual_to_date,
          penalty_rate,
          currency,
          penalty_amount_accrued,
          posted_flag,
          posted_at
        )
        VALUES %s
        """
        _exec_values(cur, sql_pen, penalty_rows, page_size=2000, fetch_returning=False)

    # instructions
    if instr_rows:
        sql_instr = f"""
        INSERT INTO {cfg.schema}.repayment_collection_instruction (
          loan_id, schedule_id, mandate_id,
          message_id, payment_info_id,
          requested_collection_date,
          instructed_amount,
          currency,
          debtor_iban_masked,
          creditor_id,
          end_to_end_id,
          remittance_information,
          instruction_status
        )
        VALUES %s
        """
        _exec_values(cur, sql_instr, instr_rows, page_size=2000, fetch_returning=False)

    # forbearance (simple)
    if cfg.p_forbearance > 0:
        for loan_id in random.sample(list(loan_meta.keys()), k=max(0, int(len(loan_meta) * cfg.p_forbearance))):
            orig = loan_meta[loan_id][4]
            ev_dt = orig + timedelta(days=random.randint(30, 365))
            forbear_rows.append((
                loan_id, ev_dt,
                random.choice(["payment_holiday", "term_extension", "rate_change", "refinance"]),
                random.choice(["income_shock", "temporary_unemployment", "medical_expense", "other"]),
                1, 2,
                random.choice([True, False]),
                "applied",
                "system",
                datetime.utcnow(),
                "synthetic forbearance"
            ))

    if forbear_rows:
        sql_forb = f"""
        INSERT INTO {cfg.schema}.forbearance_restructure_event (
          loan_id,
          event_date,
          event_type,
          reason_code,
          old_schedule_version,
          new_schedule_version,
          capitalization_flag,
          status,
          approved_by,
          approved_at,
          notes
        )
        VALUES %s
        """
        _exec_values(cur, sql_forb, forbear_rows, page_size=500, fetch_returning=False)

    # collections cases
    if case_rows:
        sql_case = f"""
        INSERT INTO {cfg.schema}.collections_case (
          loan_id,
          opened_date,
          assigned_to,
          stage,
          last_contact_date,
          next_action_date,
          outcome_code,
          closed_date,
          close_reason
        )
        VALUES %s
        RETURNING case_id, loan_id;
        """
        ret = _exec_values(cur, sql_case, case_rows, page_size=500, fetch_returning=True)
        case_id_by_loan = {int(loan_id): int(case_id) for (case_id, loan_id) in ret}

        # optional writeoff for some cases
        for loan_id, case_id in case_id_by_loan.items():
            if random.random() < 0.35:
                w_dt = date.today() - timedelta(days=random.randint(1, 180))
                writeoff_rows.append((
                    loan_id,
                    w_dt,
                    round(random.uniform(100, 2000), 2),
                    round(random.uniform(0, 300), 2),
                    round(random.uniform(0, 200), 2),
                    random.choice([True, False]),
                    case_id,
                    None,
                    None,
                    None
                ))

    if writeoff_rows:
        sql_wo = f"""
        INSERT INTO {cfg.schema}.write_off_and_recovery (
          loan_id,
          writeoff_date,
          writeoff_amount_principal,
          writeoff_amount_interest,
          writeoff_amount_fees,
          recovery_expected_flag,
          recovery_case_id,
          recovery_payment_id,
          recovery_amount,
          recovery_date
        )
        VALUES %s
        """
        _exec_values(cur, sql_wo, writeoff_rows, page_size=500, fetch_returning=False)

    # audit log
    if audit_rows:
        sql_audit = f"""
        INSERT INTO {cfg.schema}.audit_decision_and_ops_log (
          entity_type, entity_id, event_type, event_timestamp,
          actor_id, source_system,
          before_hash, after_hash,
          notes
        )
        VALUES %s
        """
        _exec_values(cur, sql_audit, audit_rows, page_size=5000, fetch_returning=False)

    return {
        "payments": len(payment_rows),
        "allocations": len(alloc_rows),
        "arrears_rows": len(arrears_rows),
        "fees": len(fee_rows),
        "penalties": len(penalty_rows),
        "mandates": len(mandate_rows),
        "instructions": len(instr_rows),
        "forbearance_events": len(forbear_rows),
        "collections_cases": len(case_rows),
        "writeoffs": len(writeoff_rows),
        "audit_rows": len(audit_rows),
    }