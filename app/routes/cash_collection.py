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

# The variable name MUST match main.py line 18: from app.routes.cash_collection import cash_collection_bp
cash_collection_bp = Blueprint("cash_collection", __name__)


@cash_collection_bp.route("/cash-collection", methods=["GET", "POST"])
def collection_view():
    db = SessionLocal()
    try:
        # 1. Check for an active OPEN day
        open_day = db.execute(
            text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")
        ).fetchone()

        if not open_day:
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # 2. Check for Lock
        saved_records = db.execute(
            text("SELECT * FROM delivery_cash_deposit WHERE stock_day_id = :s_id"),
            {"s_id": s_id}
        ).fetchall()
        is_locked = len(saved_records) > 0

        # 3. Handle POST (Saving Deposits or Reset)
        if request.method == "POST":
            # --- RESET LOGIC (Allow reset even if locked) ---
            if "reset_db" in request.form:
                db.execute(text("DELETE FROM delivery_cash_deposit WHERE stock_day_id = :s_id"), {"s_id": s_id})
                db.commit()
                flash("✅ All cash collection records cleared successfully.", "info")
                return redirect(url_for("cash_collection.collection_view"))
            
            # --- NORMAL SAVE LOGIC (Only if not locked) ---
            if not is_locked:
                entities = db.execute(text("SELECT delivery_boy_id FROM delivery_boys")).fetchall()
                for entity in entities:
                    cash = float(request.form.get(f"cash_{entity.delivery_boy_id}") or 0)
                    upi = float(request.form.get(f"upi_{entity.delivery_boy_id}") or 0)
                    db.execute(text("""
                        INSERT INTO delivery_cash_deposit (stock_day_id, delivery_boy_id, cash_amount, upi_amount, total_deposited)
                        VALUES (:s_id, :db_id, :cash, :upi, :total)
                    """), {"s_id": s_id, "db_id": entity.delivery_boy_id, "cash": cash, "upi": upi, "total": cash + upi})
                db.commit()
                flash("✅ Cash collection saved successfully.", "success")
                return redirect(url_for("cash_collection.collection_view"))

        # 4. Fetch Automatic Counter Sales Revenue
        office_counter_data = db.execute(text("""
            SELECT SUM(cash_collected + upi_collected) as counter_total
            FROM office_counter_sales
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).fetchone()

        counter_deposited = float(office_counter_data.counter_total or 0.0)

        # 5. Prepare Display Data
        saved_map = {row.delivery_boy_id: row for row in saved_records}
        display_entities = db.execute(text("SELECT delivery_boy_id, name FROM delivery_boys")).fetchall()

        return render_template("cash_collection.html",
                               stock_date=open_day.stock_date,
                               entities=display_entities,
                               saved_map=saved_map,
                               is_locked=is_locked,
                               counter_deposited=counter_deposited)
    finally:
        db.close()


@cash_collection_bp.route("/download-collection-log/<int:day_id>")
def download_collection_log(day_id):
    db = SessionLocal()
    file_format = request.args.get('file_format', 'excel')
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        query = text("""
            SELECT b.name AS Delivery_Boy, d.cash_amount AS Cash, d.upi_amount AS UPI, d.total_deposited AS Total
            FROM delivery_cash_deposit d
            JOIN delivery_boys b ON d.delivery_boy_id = b.delivery_boy_id
            WHERE d.stock_day_id = :id AND d.total_deposited > 0
            ORDER BY b.name
        """)
        results = db.execute(query, {"id": day_id}).fetchall()

        if not results:
            flash(f"No records found for {report_date}.", "info")
            return redirect(url_for('stock_day.dashboard'))

        if file_format == 'pdf':
            output = io.BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            elements = []
            styles = getSampleStyleSheet()
            elements.append(Paragraph(f"Actual Cash Collection Report", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))
            data = [["Delivery Boy", "Cash", "UPI", "Total"]]
            grand_total = 0
            for row in results:
                data.append([row.Delivery_Boy, f"{row.Cash:.2f}", f"{row.UPI:.2f}", f"{row.Total:.2f}"])
                grand_total += row.Total
            data.append(["GRAND TOTAL", "", "", f"{grand_total:.2f}"])
            t = Table(data, colWidths=[160, 100, 100, 120])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#198754")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            elements.append(t)
            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"Cash_Collection_{report_date}.pdf", mimetype='application/pdf')
        else:
            df = pd.DataFrame([dict(row._mapping) for row in results])
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f"Cash_Collection_{report_date}.xlsx", as_attachment=True)
    finally:
        db.close()