from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import pandas as pd
import io

# PDF Generation Imports
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

delivery_transactions_bp = Blueprint("delivery_transactions", __name__)


def get_open_day(db):
    return db.execute(text("""
        SELECT stock_day_id, stock_date, delivery_no_movement 
        FROM stock_days WHERE status = 'OPEN' LIMIT 1
    """)).fetchone()


@delivery_transactions_bp.route("/delivery-transactions", methods=["GET", "POST"])
def transactions_view():
    db = SessionLocal()
    try:
        open_day = get_open_day(db)
        if not open_day:
            flash("No active OPEN stock day found.", "danger")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id
        is_finalized = db.execute(text("""
            SELECT COALESCE(MAX(is_reconciled), 0) FROM daily_stock_summary 
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() == 1

        if request.method == "POST":
            if is_finalized:
                flash("Locked: Reconciliation (Step 4) is complete.", "danger")
                return redirect(url_for("delivery_transactions.transactions_view"))

            # --- 1. RESET LOGIC (FIXED) ---
            if "reset_db" in request.form:
                # We ONLY delete delivery_issues.
                # office_counter_sales is kept because it was initialized at Day Start.
                db.execute(text("DELETE FROM delivery_issues WHERE stock_day_id = :s_id"), {"s_id": s_id})

                # We reset the TV Out quantities in the summary as they depend on delivery_issues
                db.execute(text("UPDATE daily_stock_summary SET tv_out_qty = 0 WHERE stock_day_id = :s_id"),
                           {"s_id": s_id})

                # Reset the toggle status
                db.execute(text("UPDATE stock_days SET delivery_no_movement = 0 WHERE stock_day_id = :s_id"),
                           {"s_id": s_id})

                db.commit()
                flash("Delivery transaction records cleared. Office sales were preserved.", "info")
                return redirect(url_for("delivery_transactions.transactions_view"))

            no_mov_checked = 1 if request.form.get("delivery_no_movement") else 0
            db.execute(text("UPDATE stock_days SET delivery_no_movement = :val WHERE stock_day_id = :s_id"),
                       {"val": no_mov_checked, "s_id": s_id})

            # --- 2. NO MOVEMENT LOGIC (FIXED) ---
            if no_mov_checked == 1:
                # If there are no movements, we clear delivery_issues but PROTECT office_counter_sales
                db.execute(text("DELETE FROM delivery_issues WHERE stock_day_id = :s_id"), {"s_id": s_id})
                db.execute(text("UPDATE daily_stock_summary SET tv_out_qty = 0 WHERE stock_day_id = :s_id"),
                           {"s_id": s_id})
            else:
                data_map = {}
                for key, value in request.form.items():
                    if key.startswith("issue_"):
                        qty = int(value or 0)
                        parts = key.split("_")
                        b_id, t_id, cat = parts[1], parts[2], parts[3]
                        if (b_id, t_id) not in data_map:
                            data_map[(b_id, t_id)] = {'r': 0, 'n': 0, 'd': 0, 'tv': 0}
                        mapping = {'REFILL': 'r', 'NC': 'n', 'DBC': 'd', 'TVOUT': 'tv'}
                        if cat in mapping:
                            data_map[(b_id, t_id)][mapping[cat]] = qty

                for (b_id, t_id), q in data_map.items():
                    if t_id and (q['r'] > 0 or q['n'] > 0 or q['d'] > 0 or q['tv'] > 0):
                        # --- 3. UPDATE DELIVERY ISSUES ---
                        db.execute(text("""
                            INSERT INTO delivery_issues 
                                (stock_day_id, delivery_boy_id, cylinder_type_id, regular_qty, nc_qty, dbc_qty, tv_out_qty, delivery_source)
                            VALUES (:s_id, :b_id, :t_id, :r, :n, :d, :tv, 'DELIVERY_BOY')
                            ON DUPLICATE KEY UPDATE 
                                regular_qty = VALUES(regular_qty), nc_qty = VALUES(nc_qty), 
                                dbc_qty = VALUES(dbc_qty), tv_out_qty = VALUES(tv_out_qty)
                        """), {"s_id": s_id, "b_id": b_id, "t_id": t_id, "r": q['r'], "n": q['n'], "d": q['d'],
                               "tv": q['tv']})

                        # --- 4. SYNC WITH OFFICE COUNTER SALES (OFFICE ID 11) ---
                        if str(b_id) == "11":
                            # We update the received columns.
                            # Note: The 'opening' columns were already set during Step 1 (Day Start).
                            db.execute(text("""
                                UPDATE office_counter_sales 
                                SET 
                                    received_refill = :r,
                                    received_nc = :n,
                                    received_dbc = :d
                                WHERE stock_day_id = :s_id AND cylinder_type_id = :t_id
                            """), {"s_id": s_id, "t_id": t_id, "r": q['r'], "n": q['n'], "d": q['d']})

                db.execute(text("""
                    UPDATE daily_stock_summary dss
                    SET tv_out_qty = (
                        SELECT COALESCE(SUM(tv_out_qty), 0) 
                        FROM delivery_issues 
                        WHERE stock_day_id = dss.stock_day_id AND cylinder_type_id = dss.cylinder_type_id
                    ) WHERE stock_day_id = :s_id
                """), {"s_id": s_id})

            db.commit()
            flash("Delivery transactions updated! Office stock rows preserved.", "success")
            return redirect(url_for("delivery_transactions.transactions_view"))

        boys = db.execute(
            text("SELECT delivery_boy_id, name FROM delivery_boys WHERE is_active = 1 ORDER BY name")).fetchall()
        types = db.execute(text("""
            SELECT cylinder_type_id, code as cylinder_type FROM cylinder_types
            ORDER BY CASE code WHEN '14.2KG' THEN 1 WHEN '19KG' THEN 2 WHEN '10KG' THEN 3 WHEN '5KG BLUE' THEN 4 WHEN '5KG RED' THEN 5 ELSE 6 END
        """)).fetchall()
        issues_raw = db.execute(text("SELECT * FROM delivery_issues WHERE stock_day_id = :s_id"),
                                {"s_id": s_id}).fetchall()
        issues = {(r.delivery_boy_id, r.cylinder_type_id): r for r in issues_raw}
        is_saved = (len(issues_raw) > 0 or open_day.delivery_no_movement == 1)

        return render_template("delivery_transactions.html", boys=boys, types=types, issues=issues,
                               is_saved=is_saved, no_movement=open_day.delivery_no_movement,
                               is_finalized=is_finalized, stock_date=open_day.stock_date)
    finally:
        db.close()


@delivery_transactions_bp.route("/download-delivery-log/<int:day_id>")
def download_delivery_log(day_id):
    db = SessionLocal()
    file_format = request.args.get('file_format', 'excel')
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        query = text("""
            SELECT 
                b.name AS Delivery_Boy, 
                t.code AS Cylinder, 
                i.regular_qty AS Refill, 
                i.nc_qty AS NC, 
                i.dbc_qty AS DBC, 
                i.tv_out_qty AS TV_Out,
                (i.regular_qty + i.nc_qty + i.dbc_qty) AS Total_Qty
            FROM delivery_issues i
            JOIN delivery_boys b ON i.delivery_boy_id = b.delivery_boy_id
            JOIN cylinder_types t ON i.cylinder_type_id = t.cylinder_type_id
            WHERE i.stock_day_id = :id
            ORDER BY b.name, t.cylinder_type_id
        """)
        results = db.execute(query, {"id": day_id}).fetchall()

        if not results:
            flash(f"No delivery transaction records found.", "info")
            return redirect(url_for('stock_day.dashboard'))

        if file_format == 'pdf':
            output = io.BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            elements = []
            styles = getSampleStyleSheet()
            elements.append(Paragraph(f"Delivery Issues Log", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))
            data = [["Delivery Boy", "Cylinder", "Refill", "NC", "DBC", "TV Out", "Total"]]
            for row in results:
                data.append([row.Delivery_Boy, row.Cylinder, row.Refill, row.NC, row.DBC, row.TV_Out, row.Total_Qty])
            t = Table(data, colWidths=[130, 75, 45, 45, 45, 45, 45])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#343a40")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
            ]))
            elements.append(t)
            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"Delivery_Transactions_{report_date}.pdf", as_attachment=True,
                             mimetype='application/pdf')
        else:
            df = pd.DataFrame([dict(row._mapping) for row in results])
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Delivery_Issues')
            output.seek(0)
            return send_file(output, download_name=f"Delivery_Transactions_{report_date}.xlsx", as_attachment=True)
    finally:
        db.close()