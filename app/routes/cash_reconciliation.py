from flask import Blueprint, render_template, request, flash, redirect, url_for, send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import pandas as pd
import io

# The name "cash_reconciliation" here must match the prefix in url_for
cash_reconciliation_bp = Blueprint("cash_reconciliation", __name__)


@cash_reconciliation_bp.route("/cash-reconciliation", methods=["GET", "POST"])
def reconciliation_view():
    db = SessionLocal()
    try:
        # 1. Check for an active OPEN day
        open_day = db.execute(
            text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()

        if not open_day:
            return redirect(url_for('stock_day.dashboard'))

        s_id = open_day.stock_day_id

        # --- POST: Handling Update Balances ---
        if request.method == "POST":
            boys = db.execute(text("SELECT delivery_boy_id FROM delivery_boys WHERE is_active = 1")).fetchall()

            for b in boys:
                db_id = b.delivery_boy_id
                op = float(request.form.get(f"opening_{db_id}", 0))
                ex = float(request.form.get(f"expected_{db_id}", 0))
                dp = float(request.form.get(f"deposited_{db_id}", 0))

                # Calculate closing balance
                cl = op + ex - dp

                # LOGIC FIX: Status is 'SETTLED' only if closing balance is exactly 0
                new_status = 'SETTLED' if round(cl, 2) == 0 else 'PENDING'

                db.execute(text("""
                    INSERT INTO delivery_cash_balance 
                        (stock_day_id, delivery_boy_id, opening_balance, today_expected, today_deposited, closing_balance, balance_status)
                    VALUES 
                        (:s_id, :db_id, :op, :ex, :dp, :cl, :status)
                    ON DUPLICATE KEY UPDATE 
                        opening_balance = :op, today_expected = :ex, 
                        today_deposited = :dp, closing_balance = :cl, balance_status = :status
                """), {
                    "s_id": s_id,
                    "db_id": db_id,
                    "op": op,
                    "ex": ex,
                    "dp": dp,
                    "cl": cl,
                    "status": new_status
                })

            db.commit()
            flash("Cash balances updated successfully.", "success")
            return redirect(url_for('cash_reconciliation.reconciliation_view'))

        # --- GET: Fetching Data for the Display ---
        query = text("""
            SELECT 
                db.delivery_boy_id, db.name,
                COALESCE((
                    SELECT dcb2.closing_balance 
                    FROM delivery_cash_balance dcb2
                    JOIN stock_days sd ON dcb2.stock_day_id = sd.stock_day_id
                    WHERE dcb2.delivery_boy_id = db.delivery_boy_id 
                      AND sd.status = 'CLOSED'
                    ORDER BY sd.stock_date DESC LIMIT 1
                ), 0) as opening_bal,
                COALESCE(dea.expected_amount, 0) as expected_bal,
                COALESCE(dcd.total_deposited, 0) as deposited_bal,
                COALESCE(dcb.balance_status, 'PENDING') as balance_status
            FROM delivery_boys db
            LEFT JOIN delivery_cash_balance dcb ON db.delivery_boy_id = dcb.delivery_boy_id AND dcb.stock_day_id = :s_id
            LEFT JOIN delivery_expected_amount dea ON db.delivery_boy_id = dea.delivery_boy_id AND dea.stock_day_id = :s_id
            LEFT JOIN delivery_cash_deposit dcd ON db.delivery_boy_id = dcd.delivery_boy_id AND dcd.stock_day_id = :s_id
            WHERE db.is_active = 1
        """)
        results = db.execute(query, {"s_id": s_id}).fetchall()

        # Determine if balances have been updated to control button states
        has_updated = db.execute(text("SELECT COUNT(*) FROM delivery_cash_balance WHERE stock_day_id = :s_id"),
                                 {"s_id": s_id}).scalar() > 0

        return render_template(
            "cash_reconciliation.html",
            stock_date=open_day.stock_date,
            rows=results,
            has_updated=has_updated
        )
    finally:
        db.close()


@cash_reconciliation_bp.route("/day-close")
def day_close():
    db = SessionLocal()
    try:
        db.execute(text("UPDATE stock_days SET status = 'CLOSED' WHERE status = 'OPEN'"))
        db.commit()
        flash("Day closed successfully. Records moved to history.", "success")
        return redirect(url_for('stock_day.dashboard'))
    finally:
        db.close()


@cash_reconciliation_bp.route("/download-stock/<int:day_id>")
def download_stock(day_id):
    db = SessionLocal()
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        query = text("""
            SELECT t.code as Cylinder_Type, s.opening_filled, s.opening_empty, s.item_receipt, s.item_return, s.sales_regular, 
            s.nc_qty, s.dbc_qty, s.closing_filled, s.closing_empty
            FROM daily_stock_summary s 
            JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
            WHERE s.stock_day_id = :id
        """)
        df = pd.read_sql(query, db.bind, params={"id": day_id})
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Stock_Report')
        output.seek(0)
        return send_file(output, download_name=f"Stock_Report_{report_date}.xlsx", as_attachment=True)
    finally:
        db.close()


@cash_reconciliation_bp.route("/download-cash/<int:day_id>")
def download_cash(day_id):
    db = SessionLocal()
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        query = text("""
            SELECT b.name as Delivery_Boy, c.opening_balance, c.today_expected, c.today_deposited, c.closing_balance, c.balance_status
            FROM delivery_cash_balance c 
            JOIN delivery_boys b ON c.delivery_boy_id = b.delivery_boy_id
            WHERE c.stock_day_id = :id
        """)
        df = pd.read_sql(query, db.bind, params={"id": day_id})
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Cash_Report')
        output.seek(0)
        return send_file(output, download_name=f"Cash_Report_{report_date}.xlsx", as_attachment=True)
    finally:
        db.close()