from flask import Blueprint, render_template, request, send_file, flash, redirect, url_for
from flask_login import login_required
from sqlalchemy import text
import pandas as pd
import io
from datetime import datetime
from app.db.session import SessionLocal

reports_bp = Blueprint("reports", __name__)


@reports_bp.route("/reports")
@login_required
def range_reports_view():
    """Renders the date range selection page."""
    return render_template("range_reports.html", today=datetime.now().date().isoformat())


@reports_bp.route("/reports/generate", methods=["POST"])
@login_required
def generate_range_report():
    db = SessionLocal()
    try:
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")
        report_category = request.form.get("report_category")  # 'stock', 'delivery', 'cash'

        if not start_date or not end_date:
            flash("Please select both start and end dates.", "danger")
            return redirect(url_for("reports.range_reports_view"))

        output = io.BytesIO()

        # --- CATEGORY 1: CLOSING STOCK SUMMARY ---
        if report_category == 'stock':
            query = text("""
                SELECT sd.stock_date AS Date, ct.code AS Cylinder, 
                       s.opening_filled, s.opening_empty, s.item_receipt, s.item_return, 
                       s.sales_regular, s.nc_qty, s.dbc_qty, s.tv_out_qty, 
                       s.closing_filled, s.closing_empty, s.total_stock
                FROM daily_stock_summary s
                JOIN stock_days sd ON s.stock_day_id = sd.stock_day_id
                JOIN cylinder_types ct ON s.cylinder_type_id = ct.cylinder_type_id
                WHERE sd.stock_date BETWEEN :start AND :end
                ORDER BY sd.stock_date DESC, ct.cylinder_type_id ASC
            """)
            results = db.execute(query, {"start": start_date, "end": end_date}).fetchall()
            df = pd.DataFrame([dict(row._mapping) for row in results])
            sheet_name = "Stock_Summary"

        # --- CATEGORY 2: DELIVERY ISSUES (BOY-WISE) ---
        elif report_category == 'delivery':
            query = text("""
                SELECT sd.stock_date AS Date, b.name AS Delivery_Boy, t.code AS Cylinder, 
                       i.regular_qty AS Refill, i.nc_qty AS NC, i.dbc_qty AS DBC, i.tv_out_qty AS TV_Out
                FROM delivery_issues i
                JOIN stock_days sd ON i.stock_day_id = sd.stock_day_id
                JOIN delivery_boys b ON i.delivery_boy_id = b.delivery_boy_id
                JOIN cylinder_types t ON i.cylinder_type_id = t.cylinder_type_id
                WHERE sd.stock_date BETWEEN :start AND :end
                ORDER BY sd.stock_date DESC, b.name ASC
            """)
            results = db.execute(query, {"start": start_date, "end": end_date}).fetchall()
            df = pd.DataFrame([dict(row._mapping) for row in results])
            sheet_name = "Delivery_Transactions"

        elif report_category == 'office':
            query = text("""
                SELECT sd.stock_date AS Date, t.code AS Cylinder, 
                       ofc.opening_refill AS Opening_Refill,ofc.received_refill AS Received_Refill, ofc.sold_refill AS Sold_Refill,ofc.closing_refill AS Closing_Refill,
                       ofc.opening_nc AS Opening_NC,ofc.received_nc AS Received_NC, ofc.sold_nc AS Sold_NC,ofc.closing_nc AS Closing_NC,
                       ofc.opening_dbc AS Opening_DBC,ofc.received_dbc AS Received_DBC, ofc.sold_dbc AS Sold_DBC,ofc.closing_dbc AS Closing_DBC,
                       ofc.total_office_closing AS Total_Closing,
                       ofc.cash_collected AS Cash_Collected,ofc.upi_collected AS UPI_Collected, ofc.total_amount AS Total_Collected
                FROM office_counter_sales ofc
                JOIN stock_days sd ON ofc.stock_day_id = sd.stock_day_id
                JOIN cylinder_types t ON ofc.cylinder_type_id = t.cylinder_type_id
                WHERE sd.stock_date BETWEEN :start AND :end
                ORDER BY sd.stock_date DESC, t.cylinder_type_id ASC
            """)
            results = db.execute(query, {"start": start_date, "end": end_date}).fetchall()
            df = pd.DataFrame([dict(row._mapping) for row in results])
            sheet_name = "Office_Sales"

        elif report_category == 'deposit':
            query = text("""
                SELECT sd.stock_date AS Date, b.name AS Delivery_Boy, 
                       dcd.cash_amount AS Cash_Amount, dcd.upi_amount AS UPI_Amount, dcd.total_deposited AS Total_Deposited
                FROM delivery_cash_deposit dcd
                JOIN stock_days sd ON dcd.stock_day_id = sd.stock_day_id
                JOIN delivery_boys b ON dcd.delivery_boy_id = b.delivery_boy_id
                WHERE sd.stock_date BETWEEN :start AND :end
                ORDER BY sd.stock_date DESC, b.name ASC
            """)
            results = db.execute(query, {"start": start_date, "end": end_date}).fetchall()
            df = pd.DataFrame([dict(row._mapping) for row in results])
            sheet_name = "Delivery_Deposits"

        # --- CATEGORY 3: CASH BALANCE TRENDS ---
        elif report_category == 'cash':
            query = text("""
                SELECT sd.stock_date AS Date, b.name AS Delivery_Boy, 
                       c.opening_balance, c.today_expected, c.today_deposited, 
                       c.closing_balance, c.balance_status
                FROM delivery_cash_balance c
                JOIN stock_days sd ON c.stock_day_id = sd.stock_day_id
                JOIN delivery_boys b ON c.delivery_boy_id = b.delivery_boy_id
                WHERE sd.stock_date BETWEEN :start AND :end
                ORDER BY sd.stock_date DESC, b.name ASC
            """)
            results = db.execute(query, {"start": start_date, "end": end_date}).fetchall()
            df = pd.DataFrame([dict(row._mapping) for row in results])
            sheet_name = "Cash_Balances"

        if df.empty:
            flash(f"No records found for the range {start_date} to {end_date}.", "warning")
            return redirect(url_for("reports.range_reports_view"))

        # Write to Excel
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)

        output.seek(0)
        filename = f"{report_category}_report_{start_date}_to_{end_date}.xlsx"
        return send_file(output, download_name=filename, as_attachment=True)

    except Exception as e:
        flash(f"Error generating report: {str(e)}", "danger")
        return redirect(url_for("reports.range_reports_view"))
    finally:
        db.close()