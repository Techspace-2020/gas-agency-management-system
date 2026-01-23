from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import pandas as pd
import io

# PDF Generation Imports
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

office_sales_bp = Blueprint("office_sales", __name__)


@office_sales_bp.route("/office-sales", methods=["GET", "POST"])
def manage_office_sales():
    db = SessionLocal()
    try:
        # 1. Get the current active OPEN stock day
        day = db.execute(text("""
            SELECT stock_day_id, stock_date, office_finalized 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        if not day:
            flash("No Open Day found. Please start a new day first.", "danger")
            return redirect(url_for('stock_day.dashboard'))

        s_id = day.stock_day_id
        is_finalized = (day.office_finalized == 1)

        # 2. Handle Recording a Sale (POST)
        if request.method == "POST":
            if is_finalized:
                flash("Office Sales are LOCKED. Cannot record new sales.", "danger")
                return redirect(url_for('office_sales.manage_office_sales'))

            c_id = request.form.get("cylinder_type_id")
            sale_type = request.form.get("sale_type")
            qty = int(request.form.get("qty_sold", 0))
            cash = float(request.form.get("cash", 0))
            upi = float(request.form.get("upi", 0))

            column_map = {"REFILL": "sold_refill", "NC": "sold_nc", "DBC": "sold_dbc"}
            col_to_update = column_map.get(sale_type)

            if not col_to_update:
                flash("Invalid Sale Type selected.", "danger")
                return redirect(url_for('office_sales.manage_office_sales'))

            db.execute(text(f"""
                UPDATE office_counter_sales 
                SET 
                    {col_to_update} = {col_to_update} + :qty,
                    cash_collected = cash_collected + :cash,
                    upi_collected = upi_collected + :upi
                WHERE stock_day_id = :s_id AND cylinder_type_id = :c_id
            """), {
                "s_id": s_id,
                "c_id": c_id,
                "qty": qty,
                "cash": cash,
                "upi": upi
            })

            db.commit()
            flash(f"Office {sale_type} sale recorded successfully.", "success")
            return redirect(url_for('office_sales.manage_office_sales'))

        # 3. Fetch Pricing Map
        prices_raw = db.execute(text("""
            SELECT 
                cylinder_type_id, 
                refill_amount, 
                (deposit_amount + refill_amount + document_charge + installation_charge + COALESCE(regulator_charge, 0)) as nc_total,
                (deposit_amount + refill_amount + document_charge + installation_charge) as dbc_total
            FROM price_nc_components
        """)).fetchall()

        price_map = {
            p.cylinder_type_id: {
                'REFILL': float(p.refill_amount),
                'NC': float(p.nc_total),
                'DBC': float(p.dbc_total)
            } for p in prices_raw
        }

        # 4. Fetch Inventory Breakdown for UI
        office_stock = db.execute(text("""
            SELECT 
                t.cylinder_type_id, t.code, 
                s.opening_refill, s.received_refill, s.sold_refill, s.closing_refill,
                s.opening_nc, s.received_nc, s.sold_nc, s.closing_nc,
                s.opening_dbc, s.received_dbc, s.sold_dbc, s.closing_dbc
            FROM cylinder_types t
            JOIN office_counter_sales s ON t.cylinder_type_id = s.cylinder_type_id 
            WHERE s.stock_day_id = :s_id
            ORDER BY t.cylinder_type_id
        """), {"s_id": s_id}).fetchall()

        return render_template(
            "office_sales.html",
            office_stock=office_stock,
            stock_date=day.stock_date,
            price_map=price_map,
            is_finalized=is_finalized
        )
    finally:
        db.close()


@office_sales_bp.route("/finalize-office-sales", methods=["POST"])
def finalize_office_sales():
    db = SessionLocal()
    try:
        day = db.execute(text(
            "SELECT stock_day_id FROM stock_days WHERE status = 'OPEN' ORDER BY stock_date DESC LIMIT 1")).fetchone()
        if day:
            db.execute(text("""
                UPDATE stock_days SET office_finalized = 1 WHERE stock_day_id = :s_id
            """), {"s_id": day.stock_day_id})
            db.commit()
            flash("Office Sales have been finalized and frozen.", "success")
        else:
            flash("No active day found to finalize.", "warning")
        return redirect(url_for('office_sales.manage_office_sales'))
    finally:
        db.close()


@office_sales_bp.route("/download-office-report/<int:day_id>")
def download_office_report(day_id):
    db = SessionLocal()
    file_format = request.args.get('file_format', 'excel')
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        # Query for all 14 inventory columns, excluding cash/total_amount as requested
        query = text("""
            SELECT 
                t.code AS Cylinder,
                s.opening_refill AS 'Opn Refill', s.received_refill AS 'Rcv Refill', 
                s.sold_refill AS 'Sold Refill', s.closing_refill AS 'Bal Refill',
                s.opening_nc AS 'Opn NC', s.received_nc AS 'Rcv NC', 
                s.sold_nc AS 'Sold NC', s.closing_nc AS 'Bal NC',
                s.opening_dbc AS 'Opn DBC', s.received_dbc AS 'Rcv DBC', 
                s.sold_dbc AS 'Sold DBC', s.closing_dbc AS 'Bal DBC',
                s.total_office_closing AS 'Total Bal'
            FROM office_counter_sales s
            JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
            WHERE s.stock_day_id = :id
            ORDER BY t.cylinder_type_id
        """)
        results = db.execute(query, {"id": day_id}).fetchall()

        if not results:
            flash("No office records found for this date.", "info")
            return redirect(url_for('stock_day.dashboard'))

        if file_format == 'pdf':
            output = io.BytesIO()
            # Using landscape to accommodate 14 columns comfortably
            doc = SimpleDocTemplate(output, pagesize=landscape(letter))
            elements = []
            styles = getSampleStyleSheet()

            elements.append(Paragraph(f"Office Counter Inventory Report", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))

            # Header row
            data = [[
                "Cylinder", "Opn Ref", "Rcv Ref", "Sld Ref", "Bal Ref",
                "Opn NC", "Rcv NC", "Sld NC", "Bal NC",
                "Opn DBC", "Rcv DBC", "Sld DBC", "Bal DBC", "Total Bal"
            ]]

            # Data rows (No Grand Total added)
            for r in results:
                data.append([
                    r.Cylinder, r[1], r[2], r[3], r[4],
                    r[5], r[6], r[7], r[8],
                    r[9], r[10], r[11], r[12], r[13]
                ])

            # Table configuration for 14 columns
            t = Table(data)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#212529")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
            ]))
            elements.append(t)
            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"Office_Inventory_{report_date}.pdf", mimetype='application/pdf')

        else:
            # Excel Generation (No Grand Total added)
            df = pd.DataFrame([dict(row._mapping) for row in results])

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Office_Inventory')

                workbook = writer.book
                worksheet = writer.sheets['Office_Inventory']
                header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})

                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                worksheet.set_column('A:N', 12)  # Set width for all inventory columns

            output.seek(0)
            return send_file(output, download_name=f"Office_Inventory_{report_date}.xlsx", as_attachment=True)
    finally:
        db.close()