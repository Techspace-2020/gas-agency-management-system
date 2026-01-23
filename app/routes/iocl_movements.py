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

iocl_movements_bp = Blueprint("iocl_movements", __name__)


@iocl_movements_bp.route("/iocl-movements", methods=["GET", "POST"])
def iocl_view():
    db = SessionLocal()
    try:
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        if not open_day:
            flash("No active OPEN stock day found.", "error")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id
        is_finalized = db.execute(
            text("SELECT COALESCE(MAX(is_reconciled), 0) FROM daily_stock_summary WHERE stock_day_id = :s_id"),
            {"s_id": s_id}).scalar() == 1
        current_no_mov = db.execute(
            text("SELECT COALESCE(MAX(iocl_no_movement), 0) FROM daily_stock_summary WHERE stock_day_id = :s_id"),
            {"s_id": s_id}).scalar() or 0
        step1_done = db.execute(
            text("SELECT COUNT(*) FROM daily_stock_summary WHERE stock_day_id = :s_id AND opening_filled IS NOT NULL"),
            {"s_id": s_id}).scalar() > 0
        is_editable = step1_done and not is_finalized

        if request.method == "POST":
            if not is_editable:
                flash("Entry Locked: This day has been finalized in Step 4.", "danger")
                return redirect(url_for("iocl_movements.iocl_view"))

            no_mov_checked = 1 if request.form.get("no_movement") else 0
            if no_mov_checked == 1:
                db.execute(text(
                    "UPDATE daily_stock_summary SET item_receipt = 0, item_return = 0, iocl_no_movement = 1 WHERE stock_day_id = :s_id"),
                           {"s_id": s_id})
            else:
                for key, value in request.form.items():
                    if key.startswith("receipt_"):
                        c_id = key.split("_")[1]
                        receipt = int(value or 0)
                        ret = int(request.form.get(f"return_{c_id}", 0))
                        db.execute(text(
                            "UPDATE daily_stock_summary SET item_receipt = :receipt, item_return = :ret, iocl_no_movement = 0 WHERE stock_day_id = :s_id AND cylinder_type_id = :c_id"),
                                   {"receipt": receipt, "ret": ret, "s_id": s_id, "c_id": c_id})
            db.commit()
            flash("IOCL Movements updated successfully.", "success")
            return redirect(url_for("iocl_movements.iocl_view"))

        rows = db.execute(text("""
            SELECT ct.cylinder_type_id, ct.code AS cylinder_type, COALESCE(dss.item_receipt, 0) AS item_receipt, COALESCE(dss.item_return, 0) AS item_return
            FROM cylinder_types ct
            JOIN daily_stock_summary dss ON dss.cylinder_type_id = ct.cylinder_type_id
            WHERE dss.stock_day_id = :s_id
            ORDER BY CASE ct.code WHEN '14.2KG' THEN 1 WHEN '19KG' THEN 2 WHEN '10KG' THEN 3 WHEN '5KG BLUE' THEN 4 WHEN '5KG RED' THEN 5 ELSE 6 END
        """), {"s_id": s_id}).fetchall()

        total_received = sum(row.item_receipt for row in rows)
        total_returned = sum(row.item_return for row in rows)
        has_data = (total_received + total_returned) > 0 or current_no_mov == 1

        return render_template("iocl_movements.html", rows=rows, stock_date=open_day.stock_date,
                               no_movement=current_no_mov, has_data=has_data, is_editable=is_editable,
                               step1_done=step1_done, is_finalized=is_finalized, total_received=total_received,
                               total_returned=total_returned)
    finally:
        db.close()


@iocl_movements_bp.route("/iocl-movements/delete", methods=["POST"])
def delete_movements():
    db = SessionLocal()
    try:
        open_day = db.execute(text("SELECT stock_day_id FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if open_day:
            s_id = open_day.stock_day_id
            if db.execute(
                    text("SELECT COALESCE(MAX(is_reconciled), 0) FROM daily_stock_summary WHERE stock_day_id = :s_id"),
                    {"s_id": s_id}).scalar() == 1:
                flash("Locked: Cannot reset finalized records.", "danger")
                return redirect(url_for("iocl_movements.iocl_view"))
            db.execute(text(
                "UPDATE daily_stock_summary SET item_receipt = 0, item_return = 0, iocl_no_movement = 0 WHERE stock_day_id = :s_id"),
                       {"s_id": s_id})
            db.commit()
            flash("Records and flags reset successfully.", "info")
        return redirect(url_for("iocl_movements.iocl_view"))
    finally:
        db.close()


# CORRECTED DOWNLOAD ROUTE SUPPORTING BOTH EXCEL AND PDF
@iocl_movements_bp.route("/download-iocl-log/<int:day_id>")
def download_iocl_log(day_id):
    db = SessionLocal()
    # Capture format from redirect parameters
    file_format = request.args.get('file_format', 'excel')

    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        query = text("""
            SELECT 
                t.code AS Cylinder_Type, 
                s.item_receipt AS Inward_Receipts, 
                s.item_return AS Outward_Returns
            FROM daily_stock_summary s
            JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
            WHERE s.stock_day_id = :id AND (s.item_receipt > 0 OR s.item_return > 0)
        """)
        results = db.execute(query, {"id": day_id}).fetchall()

        if file_format == 'pdf':
            # --- PDF GENERATION ---
            output = io.BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            elements = []
            styles = getSampleStyleSheet()

            # Header Text
            elements.append(Paragraph(f"IOCL Movements Log", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))

            # Table Data Preparation
            data = [["Cylinder Type", "Inward Receipts", "Outward Returns"]]
            for row in results:
                data.append([row.Cylinder_Type, str(row.Inward_Receipts), str(row.Outward_Returns)])

            # Table Styling
            t = Table(data, colWidths=[150, 120, 120])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey])
            ]))
            elements.append(t)

            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"IOCL_Log_{report_date}.pdf", as_attachment=True,
                             mimetype='application/pdf')

        else:
            # --- EXCEL GENERATION ---
            df = pd.read_sql(query, db.bind, params={"id": day_id})
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='IOCL_Movements')
            output.seek(0)
            return send_file(output, download_name=f"IOCL_Log_{report_date}.xlsx", as_attachment=True)

    finally:
        db.close()