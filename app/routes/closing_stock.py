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

closing_stock_bp = Blueprint("closing_stock", __name__)


@closing_stock_bp.route("/closing-stock", methods=["GET", "POST"])
def closing_view():
    db = SessionLocal()
    try:
        # 1. Fetch the current active OPEN stock day
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date, delivery_no_movement 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        if not open_day:
            flash("No active OPEN stock day found.", "danger")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # 2. PREREQUISITE CHECK
        has_delivery_rows = db.execute(text("""
            SELECT COUNT(*) FROM delivery_issues WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() > 0

        step3_done = has_delivery_rows or (open_day.delivery_no_movement == 1)

        # 3. MASTER LOCK CHECK
        is_finalized = db.execute(text("""
            SELECT MAX(is_reconciled) 
            FROM daily_stock_summary 
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() == 1

        # 4. Fetch Data for Reconciliation Math
        summary_raw = db.execute(text("""
            SELECT s.*, t.code 
            FROM daily_stock_summary s
            JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
            WHERE s.stock_day_id = :s_id
            ORDER BY 
                    CASE t.code
                        WHEN '14.2KG' THEN 1
                        WHEN '19KG' THEN 2
                        WHEN '10KG' THEN 3
                        WHEN '5KG BLUE' THEN 4
                        WHEN '5KG RED' THEN 5
                        ELSE 6
                    END
        """), {"s_id": s_id}).fetchall()

        issues_raw = db.execute(text("""
            SELECT cylinder_type_id, 
                   SUM(regular_qty) as total_reg, 
                   SUM(nc_qty) as total_nc, 
                   SUM(dbc_qty) as total_dbc, 
                   SUM(tv_out_qty) as total_tv
            FROM delivery_issues 
            WHERE stock_day_id = :s_id 
            GROUP BY cylinder_type_id
        """), {"s_id": s_id}).fetchall()

        issues_map = {r.cylinder_type_id: r for r in issues_raw}

        display_data = []
        for s in summary_raw:
            iss = issues_map.get(s.cylinder_type_id)
            reg = iss.total_reg if iss else 0
            nc = iss.total_nc if iss else 0
            dbc = iss.total_dbc if iss else 0
            tv = iss.total_tv if iss else 0

            # Reconciliation Formulas
            calc_filled = (s.opening_filled or 0) + (s.item_receipt or 0) - (reg + nc + dbc)
            calc_empty = (s.opening_empty or 0) + reg + tv - (s.item_return or 0)
            defective = s.defective_empty_vehicle or 0

            # TOTAL STOCK = Filled + Empty + Defective
            total_stock = calc_filled + calc_empty + defective

            display_data.append({
                'cylinder_type_id': s.cylinder_type_id,
                'code': s.code,
                'opening': {'f': s.opening_filled, 'e': s.opening_empty},
                'iocl': {'in': s.item_receipt, 'out': s.item_return},
                'issues': {'reg': reg, 'nc': nc, 'dbc': dbc},
                'tv': tv,
                'defective_v': defective,
                'closing': {'f': calc_filled, 'e': calc_empty},
                'total_stock': total_stock
            })

        # 5. Handle Finalization (POST)
        if request.method == "POST":
            if is_finalized:
                flash("This day is already finalized.", "warning")
                return redirect(url_for("closing_stock.closing_view"))

            if not step3_done:
                flash("Error: Please complete Step 3 before finalizing.", "danger")
                return redirect(url_for("closing_stock.closing_view"))

            for item in display_data:
                db.execute(text("""
                    UPDATE daily_stock_summary 
                    SET closing_filled = :cf, 
                        closing_empty = :ce, 
                        total_stock = :ts,
                        sales_regular = :sr, 
                        nc_qty = :nq, 
                        dbc_qty = :dq, 
                        tv_out_qty = :tvq,
                        defective_empty_vehicle = :dev,
                        is_reconciled = 1
                    WHERE stock_day_id = :s_id AND cylinder_type_id = :ct_id
                """), {
                    "cf": item['closing']['f'],
                    "ce": item['closing']['e'],
                    "ts": item['total_stock'],
                    "sr": item['issues']['reg'],
                    "nq": item['issues']['nc'],
                    "dq": item['issues']['dbc'],
                    "tvq": item['tv'],
                    "dev": item['defective_v'],
                    "s_id": s_id,
                    "ct_id": item['cylinder_type_id']
                })

            db.commit()
            flash(f"Reconciliation successful. Stock locked for {open_day.stock_date}.", "success")
            return redirect(url_for("closing_stock.closing_view"))

        return render_template("closing_stock.html",
                               stock_date=open_day.stock_date,
                               data=display_data,
                               is_finalized=is_finalized,
                               step3_done=step3_done)
    finally:
        db.close()


# UPDATED DOWNLOAD ROUTE SUPPORTING NC, DBC, AND FULL ORDER
@closing_stock_bp.route("/download-stock/<int:day_id>")
def download_stock(day_id):
    db = SessionLocal()
    file_format = request.args.get('file_format', 'excel')
    try:
        day_info = db.execute(text("SELECT stock_date FROM stock_days WHERE stock_day_id = :id"),
                              {"id": day_id}).fetchone()
        report_date = day_info.stock_date if day_info else "Report"

        # Explicit Query for the 13-column Report
        query = text("""
            SELECT 
                t.code AS Cylinder, 
                s.opening_filled AS Open_Filled, 
                s.opening_empty AS Open_Empty, 
                s.item_receipt AS Item_receipt, 
                s.item_return AS Item_return, 
                s.sales_regular AS Sales, 
                s.nc_qty AS NC,
                s.dbc_qty AS DBC,
                s.tv_out_qty AS TV_Out,
                s.defective_empty_vehicle AS Defective,
                s.closing_filled AS Close_Filled, 
                s.closing_empty AS Close_Empty,
                s.total_stock AS Total_Stock
            FROM daily_stock_summary s 
            JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
            WHERE s.stock_day_id = :id
            ORDER BY t.cylinder_type_id
        """)
        results = db.execute(query, {"id": day_id}).fetchall()

        if file_format == 'pdf':
            output = io.BytesIO()
            # Use landscape to fit 13 columns
            doc = SimpleDocTemplate(output, pagesize=landscape(letter))
            elements = []
            styles = getSampleStyleSheet()

            elements.append(Paragraph(f"Daily Stock Summary Report", styles['Title']))
            elements.append(Paragraph(f"Stock Date: {report_date}", styles['Normal']))
            elements.append(Spacer(1, 12))

            # Table Header - 13 Columns
            data = [
                ["Cylinder", "Open_Filled", "Open_Empty", "Item_receipt", "Item_return",
                 "Sales", "NC", "DBC", "TV_Out", "Defective", "Close_Filled", "Close_Empty", "Total_Stock"]
            ]
            for r in results:
                data.append([r.Cylinder, r.Open_Filled, r.Open_Empty, r.Item_receipt, r.Item_return,
                             r.Sales, r.NC, r.DBC, r.TV_Out, r.Defective, r.Close_Filled, r.Close_Empty, r.Total_Stock])

            # Formatting table
            t = Table(data)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#343a40")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey])
            ]))
            elements.append(t)
            doc.build(elements)
            output.seek(0)
            return send_file(output, download_name=f"Stock_Report_{report_date}.pdf", mimetype='application/pdf')

        else:
            # Excel Generation - Ensures ordered columns
            df = pd.DataFrame([dict(row._mapping) for row in results])
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Stock_Report')
            output.seek(0)
            return send_file(output, download_name=f"Stock_Report_{report_date}.xlsx", as_attachment=True)

    finally:
        db.close()