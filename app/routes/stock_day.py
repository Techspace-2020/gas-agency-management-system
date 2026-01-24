from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from sqlalchemy import text
from datetime import date, timedelta, datetime
from app.db.session import SessionLocal

stock_day_bp = Blueprint("stock_day", __name__)


@stock_day_bp.route("/")
@stock_day_bp.route("/dashboard")
@login_required
def dashboard():
    db = SessionLocal()
    try:
        today_val = date.today().isoformat()

        day = db.execute(text("""
            SELECT stock_day_id, stock_date, status, delivery_no_movement 
            FROM stock_days ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        history = db.execute(text("""
            SELECT stock_day_id, stock_date FROM stock_days 
            WHERE status = 'CLOSED' ORDER BY stock_date DESC
        """)).fetchall()

        is_day_closed = (day.status.upper() == 'CLOSED') if day else False

        progress = {
            "opening_stock": False, "iocl_movements": False, "deliveries": False,
            "finalized_stock": False, "expected_cash": False, "cash_collection": False,
            "reconciled_cash": False
        }

        if day and not is_day_closed:
            s_id = day.stock_day_id

            progress["opening_stock"] = db.execute(text("""
                SELECT COUNT(*) FROM daily_stock_summary 
                WHERE stock_day_id = :s_id AND opening_filled IS NOT NULL
            """), {"s_id": s_id}).scalar() > 0

            iocl_status = db.execute(text("""
                SELECT (COALESCE(SUM(item_receipt + item_return), 0) > 0) OR 
                       (MAX(CAST(iocl_no_movement AS UNSIGNED)) = 1)
                FROM daily_stock_summary WHERE stock_day_id = :s_id
            """), {"s_id": s_id}).fetchone()
            progress["iocl_movements"] = bool(iocl_status[0]) and progress["opening_stock"]

            has_delivery_data = db.execute(text("SELECT COUNT(*) FROM delivery_issues WHERE stock_day_id = :s_id"),
                                           {"s_id": s_id}).scalar() > 0
            progress["deliveries"] = (has_delivery_data or day.delivery_no_movement == 1) and progress["iocl_movements"]

            has_finalized = db.execute(
                text("SELECT MAX(is_reconciled) FROM daily_stock_summary WHERE stock_day_id = :s_id"),
                {"s_id": s_id}).scalar() == 1
            progress["finalized_stock"] = has_finalized and progress["deliveries"]

            has_exp = db.execute(text("SELECT COUNT(*) FROM delivery_expected_amount WHERE stock_day_id = :s_id"),
                                 {"s_id": s_id}).scalar() > 0
            progress["expected_cash"] = has_exp and progress["finalized_stock"]

            has_coll = db.execute(text("SELECT COUNT(*) FROM delivery_cash_deposit WHERE stock_day_id = :s_id"),
                                  {"s_id": s_id}).scalar() > 0
            progress["cash_collection"] = has_coll and progress["expected_cash"]

            has_recon = db.execute(text("SELECT COUNT(*) FROM delivery_cash_balance WHERE stock_day_id = :s_id"),
                                   {"s_id": s_id}).scalar() > 0
            progress["reconciled_cash"] = has_recon and progress["cash_collection"]

        return render_template("dashboard.html", day=day, history=history, progress=progress,
                               is_day_closed=is_day_closed, user=current_user, today=today_val)
    finally:
        db.close()


@stock_day_bp.route("/preview-report", methods=["POST"])
@login_required
def preview_report():
    db = SessionLocal()

    try:
        report_type = request.form.get("report_type")
        selected_date_str = request.form.get("selected_date")

        if not report_type or not selected_date_str:
            return jsonify({"error": "Missing report type or date."}), 400
                                         
        try:
            selected_date = datetime.strptime(selected_date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({"error": "Invalid date format."}), 400
                                         
        record = db.execute(text("SELECT stock_day_id, status FROM stock_days WHERE stock_date = :sd"),
                            {"sd": selected_date_str}).fetchone()

        if not record:
            return jsonify({"error": f"Error: No operation records exist for {selected_date_str}."}), 404

        if record.status == 'OPEN' and report_type in ['stock', 'cash', 'delivery_issues', 'actual_cash']:
            return jsonify({"error": "This day is still OPEN. Finalize the day to view Summary Reports."}), 400
                                         
        s_id = record.stock_day_id
        data = []
                                                       
        if report_type == 'actual_cash':
            results = db.execute(text("""
                SELECT 'Office Counter' AS Delivery_Boy, 
                       SUM(cash_collected) AS Cash, 
                       SUM(upi_collected) AS UPI, 
                       SUM(cash_collected + upi_collected) AS Total
                FROM office_counter_sales 
                WHERE stock_day_id = :s_id
                HAVING Total > 0
                UNION ALL
                SELECT b.name AS Delivery_Boy, 
                       d.cash_amount AS Cash, 
                       d.upi_amount AS UPI, 
                       d.total_deposited AS Total
                FROM delivery_cash_deposit d 
                JOIN delivery_boys b ON d.delivery_boy_id = b.delivery_boy_id
                WHERE d.stock_day_id = :s_id AND d.total_deposited > 0
                ORDER BY Delivery_Boy
            """), {"s_id": s_id}).fetchall()
            data = [{"Delivery Boy": r.Delivery_Boy, "Cash": f"{float(r.Cash):.2f}", "UPI": f"{float(r.UPI):.2f}",
                     "Total": f"{float(r.Total):.2f}"} for r in results]

        elif report_type == 'stock':
            results = db.execute(text("""
                SELECT t.code as Cylinder, s.opening_filled, s.opening_empty, s.item_receipt, s.item_return, s.sales_regular, 
                       s.nc_qty, s.dbc_qty, s.tv_out_qty, s.defective_empty_vehicle, s.closing_filled, s.closing_empty, s.total_stock
                FROM daily_stock_summary s JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
                WHERE s.stock_day_id = :s_id ORDER BY t.cylinder_type_id
            """), {"s_id": s_id}).fetchall()
            data = [dict(row._mapping) for row in results]

        elif report_type == 'cash':
            results = db.execute(text("""
                SELECT b.name as Delivery_Boy, c.opening_balance, c.today_expected, c.today_deposited, c.closing_balance, c.balance_status
                FROM delivery_cash_balance c JOIN delivery_boys b ON c.delivery_boy_id = b.delivery_boy_id
                WHERE c.stock_day_id = :s_id ORDER BY b.name
            """), {"s_id": s_id}).fetchall()
            data = [{"Delivery Boy": r.Delivery_Boy, "Opening": r.opening_balance, "Expected": r.today_expected,
                     "Deposited": r.today_deposited, "Closing": r.closing_balance, "Status": r.balance_status} for r in
                    results]

        elif report_type == 'delivery_issues':
            results = db.execute(text("""
                SELECT b.name as Delivery_Boy, t.code as Cylinder, i.regular_qty as Refill, i.nc_qty as NC, i.dbc_qty as DBC, i.tv_out_qty as TV_Out
                FROM delivery_issues i JOIN delivery_boys b ON i.delivery_boy_id = b.delivery_boy_id 
                JOIN cylinder_types t ON i.cylinder_type_id = t.cylinder_type_id
                WHERE i.stock_day_id = :s_id ORDER BY b.name
            """), {"s_id": s_id}).fetchall()
            data = [
                {"Delivery Boy": r.Delivery_Boy, "Cylinder": r.Cylinder, "Refill": r.Refill, "NC": r.NC, "DBC": r.DBC,
                 "TV Out": r.TV_Out} for r in results]

        elif report_type == 'iocl_inward':
            results = db.execute(text("""
                SELECT t.code as Cylinder, s.item_receipt, s.item_return
                FROM daily_stock_summary s JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
                WHERE s.stock_day_id = :s_id AND (s.item_receipt > 0 OR s.item_return > 0) ORDER BY t.cylinder_type_id
            """), {"s_id": s_id}).fetchall()
            data = [{"Cylinder": r.Cylinder, "Receipt": r.item_receipt, "Return": r.item_return} for r in results]

        # --- UPDATED OFFICE SALES PREVIEW ---
        elif report_type == 'office_sales':
            results = db.execute(text("""
                SELECT 
                    t.code AS Cylinder,
                    s.opening_refill, s.received_refill, s.sold_refill, s.closing_refill,
                    s.opening_nc, s.received_nc, s.sold_nc, s.closing_nc,
                    s.opening_dbc, s.received_dbc, s.sold_dbc, s.closing_dbc,
                    s.total_office_closing
                FROM office_counter_sales s
                JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
                WHERE s.stock_day_id = :s_id
                ORDER BY t.cylinder_type_id
            """), {"s_id": s_id}).fetchall()

            # Mapping keys to match the 14-column header in dashboard.html
            data = [{
                "Cylinder": r.Cylinder,
                "Opn Refill": r.opening_refill, "Rcv Refill": r.received_refill,
                "Sold Refill": r.sold_refill, "Bal Refill": r.closing_refill,
                "Opn NC": r.opening_nc, "Rcv NC": r.received_nc,
                "Sold NC": r.sold_nc, "Bal NC": r.closing_nc,
                "Opn DBC": r.opening_dbc, "Rcv DBC": r.received_dbc,
                "Sold DBC": r.sold_dbc, "Bal DBC": r.closing_dbc,
                "Total Bal": r.total_office_closing
            } for r in results]

        if not data:
            return jsonify({"error": "No records found for the selected report type and date."}), 404                                        
        return jsonify({"data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500                                  
    finally:
        db.close()


@stock_day_bp.route("/generate-report", methods=["POST"])
@login_required
def generate_report():
    db = SessionLocal()
    try:
        report_type = request.form.get("report_type")
        selected_date_str = request.form.get("selected_date")
        file_format = request.form.get("file_format", "excel")

        record = db.execute(text("SELECT stock_day_id FROM stock_days WHERE stock_date = :sd"),
                            {"sd": selected_date_str}).fetchone()

        if not record:
            flash(f"Error: No stock day exists for {selected_date_str}.", "danger")
            return redirect(url_for('stock_day.dashboard'))

        report_routes = {
            'stock': 'closing_stock.download_stock',
            'cash': 'cash_reconciliation.download_cash',
            'delivery_issues': 'delivery_transactions.download_delivery_log',
            'iocl_inward': 'iocl_movements.download_iocl_log',
            'actual_cash': 'cash_collection.download_collection_log',
            'office_sales': 'office_sales.download_office_report'
        }

        if report_type in report_routes:
            return redirect(url_for(report_routes[report_type], day_id=record.stock_day_id, file_format=file_format))

        flash("Invalid report type.", "danger")
        return redirect(url_for('stock_day.dashboard'))
    finally:
        db.close()


@stock_day_bp.route("/create-stock-day", methods=["GET", "POST"])
@login_required
def create_new_day():
    db = SessionLocal()
    try:
        today_val = date.today().isoformat()
        last_day = db.execute(
            text("SELECT stock_day_id, stock_date FROM stock_days ORDER BY stock_date DESC LIMIT 1")).fetchone()
        next_available = (last_day.stock_date + timedelta(days=1)).isoformat() if last_day else today_val

        if request.method == "POST":
            selected_date = request.form.get("stock_date")

            # --- Validation Logic Added Here ---
            # Check if a stock day for the selected date already exists
            existing_day = db.execute(
                text("SELECT stock_day_id FROM stock_days WHERE stock_date = :sd"),
                {"sd": selected_date}
            ).fetchone()

            if existing_day:
                # If it exists, flash an error message and redirect back to the creation page
                flash(f"Error: A stock day for {selected_date} already exists.", "error")
                return redirect(url_for('stock_day.create_new_day'))
            # --- End of Validation Logic ---

            res = db.execute(
                text("INSERT INTO stock_days (stock_date, status, delivery_no_movement) VALUES (:sd, 'OPEN', 0)"),
                {"sd": selected_date})
            new_id = res.lastrowid

            # Initializing office stock rows for all types
            # generated columns (closing_xxx) are managed by MySQL
            db.execute(text("""
                INSERT INTO office_counter_sales (
                    stock_day_id, cylinder_type_id, 
                    opening_refill, opening_nc, opening_dbc
                )
                SELECT 
                    :new_id, t.cylinder_type_id,
                    COALESCE(prev.closing_refill, 0), 
                    COALESCE(prev.closing_nc, 0), 
                    COALESCE(prev.closing_dbc, 0)
                FROM cylinder_types t
                LEFT JOIN (
                    SELECT cylinder_type_id, closing_refill, closing_nc, closing_dbc
                    FROM office_counter_sales 
                    WHERE stock_day_id = :last_id
                ) prev ON t.cylinder_type_id = prev.cylinder_type_id
            """), {"new_id": new_id, "last_id": last_day.stock_day_id if last_day else -1})

            db.commit()
            flash(f"New stock day for {selected_date} created successfully.", "success")
            return redirect(url_for('stock_day.dashboard'))

        return render_template("create_stock_day.html", next_available_date=next_available, today=today_val)
    finally:
        db.close()
