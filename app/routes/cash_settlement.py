from flask import Blueprint, render_template, request, flash, redirect, url_for
from sqlalchemy import text
from app.db.session import SessionLocal

# The variable name MUST match main.py line 17: from app.routes.cash_settlement import cash_settlement_bp
cash_settlement_bp = Blueprint("cash_settlement", __name__)


@cash_settlement_bp.route("/cash-settlement", methods=["GET", "POST"])
def cash_view():
    db = SessionLocal()
    is_updated = False
    try:
        # 1. Fetch current active OPEN day
        open_day = db.execute(
            text("SELECT stock_day_id, stock_date, office_finalized FROM stock_days WHERE status = 'OPEN' LIMIT 1")
        ).fetchone()

        if not open_day:
            flash("No active OPEN stock day found.", "danger")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # 2. Master Lock Check: Check if finalized
        existing_record = db.execute(
            text("SELECT 1 FROM delivery_expected_amount WHERE stock_day_id = :s_id LIMIT 1"),
            {"s_id": s_id}
        ).fetchone()

        if existing_record:
            is_updated = True

        # 3. Calculation Logic

        # A. Staff Refill Dues (Excluding Office)
        staff_results = db.execute(text("""
            SELECT db.name AS delivery_boy, di.delivery_boy_id, SUM(di.regular_qty * pnc.refill_amount) AS regular_amt
            FROM delivery_issues di
            JOIN delivery_boys db ON di.delivery_boy_id = db.delivery_boy_id
            JOIN price_nc_components pnc ON di.cylinder_type_id = pnc.cylinder_type_id
            WHERE di.stock_day_id = :s_id AND db.name != 'OFFICE'
            GROUP BY di.delivery_boy_id, db.name
        """), {"s_id": s_id}).fetchall()

        # B. Global Staff Connections (EXCLUDING OFFICE ID 11)
        global_staff_data = db.execute(text("""
            SELECT 
                SUM(di.nc_qty * (pnc.deposit_amount + pnc.refill_amount + pnc.document_charge + pnc.installation_charge + COALESCE(pnc.regulator_charge, 0))) as total_nc_amt,
                SUM(di.dbc_qty * (pnc.deposit_amount + pnc.refill_amount + pnc.document_charge + pnc.installation_charge)) as total_dbc_amt
            FROM delivery_issues di
            JOIN price_nc_components pnc ON di.cylinder_type_id = pnc.cylinder_type_id
            WHERE di.stock_day_id = :s_id AND di.delivery_boy_id != 11
        """), {"s_id": s_id}).fetchone()

        # C. Office Counter Sales
        office_data = db.execute(text("""
            SELECT SUM(cash_collected + upi_collected) as total_cash, SUM(sold_refill + sold_nc + sold_dbc) as total_qty
            FROM office_counter_sales WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).fetchone()

        # Prepare final values
        pure_office_sales = float(office_data.total_cash or 0.0)
        total_nc_staff = float(global_staff_data.total_nc_amt or 0.0)
        total_dbc_staff = float(global_staff_data.total_dbc_amt or 0.0)
        net_global_connection = total_nc_staff + total_dbc_staff

        # Combined Grand Total for Office
        final_office_expected = pure_office_sales + net_global_connection
        total_staff_refill_expected = float(sum(row.regular_amt for row in staff_results)) if staff_results else 0.0

        # 4. Handle Finalization (POST)
        if request.method == "POST" and not is_updated:
            if open_day.office_finalized == 0:
                flash("Please finalize Office Sales first.", "warning")
                return redirect(url_for("cash_settlement.cash_view"))

            # Save Boy Dues
            for row in staff_results:
                db.execute(text(
                    "INSERT INTO delivery_expected_amount (stock_day_id, delivery_boy_id, expected_amount) VALUES (:s_id, :db_id, :amt)"),
                           {"s_id": s_id, "db_id": row.delivery_boy_id, "amt": row.regular_amt})

            # Save Combined Office Total
            office_info = db.execute(
                text("SELECT delivery_boy_id FROM delivery_boys WHERE name = 'OFFICE' LIMIT 1")).fetchone()
            if office_info:
                db.execute(text(
                    "INSERT INTO delivery_expected_amount (stock_day_id, delivery_boy_id, expected_amount) VALUES (:s_id, :db_id, :amt)"),
                           {"s_id": s_id, "db_id": office_info.delivery_boy_id, "amt": final_office_expected})

            db.commit()
            is_updated = True
            flash("Expected cash finalized successfully.", "success")
            return redirect(url_for("cash_settlement.cash_view"))

        return render_template("cash_settlement.html",
                               stock_date=open_day.stock_date,
                               staff_results=staff_results,
                               office_expected=final_office_expected,
                               pure_office_sales=pure_office_sales,
                               is_updated=is_updated,
                               office_finalized=open_day.office_finalized,
                               total_refill=(total_staff_refill_expected + pure_office_sales),
                               total_nc=total_nc_staff,
                               total_dbc=total_dbc_staff,
                               net_office_connection_cash=net_global_connection)
    finally:
        db.close()