from flask import Blueprint, render_template, request, flash, redirect, url_for, send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import pandas as pd
import io

# PDF Generation Imports
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

cash_reconciliation_bp = Blueprint("cash_reconciliation", __name__)


@cash_reconciliation_bp.route("/cash-reconciliation", methods=["GET", "POST"])
def reconciliation_view():
    db = SessionLocal()
    try:
        # 1. Check for an active OPEN day
        open_day = db.execute(
            text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()

        if not open_day:
            flash("No active OPEN day found. Please open a day first.", "warning")
            return redirect(url_for('stock_day.dashboard'))

        s_id = open_day.stock_day_id

        # --- POST: Handling Update Balances ---
        if request.method == "POST":
            boys = db.execute(text("SELECT delivery_boy_id FROM delivery_boys WHERE is_active = 1")).fetchall()

            for b in boys:
                db_id = b.delivery_boy_id
                # Explicitly cast to float to prevent Decimal/Float TypeErrors
                op = float(request.form.get(f"opening_{db_id}", 0))
                ex = float(request.form.get(f"expected_{db_id}", 0))
                dp = float(request.form.get(f"deposited_{db_id}", 0))

                # Logic: (Opening + Expected) - Deposited
                cl = op + ex - dp

                if round(cl, 2) == 0:
                    new_status = 'SETTLED'
                elif cl < 0:
                    new_status = 'EXCESS'
                else:
                    new_status = 'PENDING'

                db.execute(text("""
                    INSERT INTO delivery_cash_balance 
                        (stock_day_id, delivery_boy_id, opening_balance, today_expected, today_deposited, closing_balance, balance_status)
                    VALUES 
                        (:s_id, :db_id, :op, :ex, :dp, :cl, :status)
                    ON DUPLICATE KEY UPDATE 
                        opening_balance = :op, today_expected = :ex, 
                        today_deposited = :dp, closing_balance = :cl, balance_status = :status
                """), {
                    "s_id": s_id, "db_id": db_id, "op": op, "ex": ex, "dp": dp, "cl": cl, "status": new_status
                })

            db.commit()
            flash("Cash balances updated and categorized successfully.", "success")
            return redirect(url_for('cash_reconciliation.reconciliation_view'))

        # --- GET: Fetching Data for Display ---
        # UPDATED: Deposited Today now aggregates manual deposits AND automatic counter sales revenue
        query = text("""
            SELECT 
                db.delivery_boy_id, db.name,
                /* Fetch Opening Balance: Last CLOSED day's Closing Balance */
                COALESCE((
                    SELECT dcb2.closing_balance 
                    FROM delivery_cash_balance dcb2
                    JOIN stock_days sd ON dcb2.stock_day_id = sd.stock_day_id
                    WHERE dcb2.delivery_boy_id = db.delivery_boy_id 
                      AND sd.status = 'CLOSED'
                    ORDER BY sd.stock_date DESC LIMIT 1
                ), 0) as opening_bal,

                /* Fetch Expected Balance: Use Step 5 finalized data */
                COALESCE(dea.expected_amount, 0) as expected_bal,

                /* AGGREGATED DEPOSIT: Manual Deposits + Automatic Counter Sales Revenue (for Office row) */
                (COALESCE(dcd.total_deposited, 0) + 
                 CASE 
                    WHEN db.name = 'OFFICE' THEN 
                        COALESCE((SELECT SUM(cash_collected + upi_collected) FROM office_counter_sales WHERE stock_day_id = :s_id), 0)
                    ELSE 0 
                 END
                ) as deposited_bal,

                COALESCE(dcb.balance_status, 'PENDING') as balance_status
            FROM delivery_boys db
            LEFT JOIN delivery_cash_balance dcb ON db.delivery_boy_id = dcb.delivery_boy_id AND dcb.stock_day_id = :s_id
            LEFT JOIN delivery_expected_amount dea ON db.delivery_boy_id = dea.delivery_boy_id AND dea.stock_day_id = :s_id
            LEFT JOIN delivery_cash_deposit dcd ON db.delivery_boy_id = dcd.delivery_boy_id AND dcd.stock_day_id = :s_id
            WHERE db.is_active = 1
            ORDER BY db.name = 'OFFICE' DESC, db.name ASC
        """)
        results = db.execute(query, {"s_id": s_id}).fetchall()

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
        # Lock the day so opening balances can carry forward
        db.execute(text("UPDATE stock_days SET status = 'CLOSED' WHERE status = 'OPEN'"))
        db.commit()
        flash("Day closed. All balances carried forward.", "success")
        return redirect(url_for('stock_day.dashboard'))
    finally:
        db.close()


@cash_reconciliation_bp.route("/download-stock/<int:day_id>")
def download_stock(day_id):
    db = SessionLocal()
    file_format = request.args.get('file_format', 'excel')
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
        results = db.execute(query, {"id": day_id}).fetchall()

        if file_format == 'pdf':
            output = io.BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            elements = []
            styles = getSampleStyleSheet()
            elements.append(Paragraph(f"Daily Stock Summary Report", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))

            data = [["Cylinder", "Op Fill", "Op Emp", "In", "Out", "Sale", "NC", "DBC", "Cl Fill", "Cl Emp"]]
            for r in results:
                data.append(
                    [r.Cylinder_Type, r.opening_filled, r.opening_empty, r.item_receipt, r.item_return, r.sales_regular,
                     r.nc_qty, r.dbc_qty, r.closing_filled, r.closing_empty])

            t = Table(data, colWidths=[80, 50, 50, 40, 40, 40, 40, 40, 50, 50])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#212529")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 9)
            ]))
            elements.append(t)
            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"Stock_Report_{report_date}.pdf", mimetype='application/pdf')
        else:
            df = pd.DataFrame([dict(row._mapping) for row in results])
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
    file_format = request.args.get('file_format', 'excel')
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        query = text("""
            SELECT b.name as Delivery_Boy, c.opening_balance as Opening, 
            c.today_expected as Expected, c.today_deposited as Deposited, 
            c.closing_balance as Closing, c.balance_status as Status
            FROM delivery_cash_balance c 
            JOIN delivery_boys b ON c.delivery_boy_id = b.delivery_boy_id
            WHERE c.stock_day_id = :id
            ORDER BY b.name
        """)
        results = db.execute(query, {"id": day_id}).fetchall()

        if file_format == 'pdf':
            output = io.BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            elements = []
            styles = getSampleStyleSheet()
            elements.append(Paragraph(f"Cash Reconciliation Log", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))

            data = [["Delivery Boy", "Opening", "Expected", "Deposited", "Closing", "Status"]]
            for r in results:
                data.append(
                    [r.Delivery_Boy, f"{r.Opening:.2f}", f"{r.Expected:.2f}", f"{r.Deposited:.2f}", f"{r.Closing:.2f}",
                     r.Status])

            t = Table(data, colWidths=[150, 75, 75, 75, 75, 75])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#0d6efd")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(t)
            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"Cash_Report_{report_date}.pdf", mimetype='application/pdf')
        else:
            df = pd.DataFrame([dict(row._mapping) for row in results])
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Cash_Report')
            output.seek(0)
            return send_file(output, download_name=f"Cash_Report_{report_date}.xlsx", as_attachment=True)
    finally:
        db.close()