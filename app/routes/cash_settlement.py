from flask import Blueprint, render_template, request, flash
from sqlalchemy import text
from app.db.session import SessionLocal

cash_settlement_bp = Blueprint("cash_settlement", __name__)


@cash_settlement_bp.route("/cash-settlement", methods=["GET", "POST"])
def cash_view():
    db = SessionLocal()
    success_message = None
    is_updated = False
    try:
        # 1. Fetch current OPEN day [cite: 38]
        open_day = db.execute(
            text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if not open_day:
            return "No active OPEN stock day found.", 400

        # 2. Check if Expected Cash is already updated for this day [cite: 45-47, 151]
        existing_record = db.execute(text("""
            SELECT 1 FROM delivery_expected_amount 
            WHERE stock_day_id = :s_id LIMIT 1
        """), {"s_id": open_day.stock_day_id}).fetchone()

        if existing_record:
            is_updated = True

        # 3. Perform Calculations (Derived from Delivery Issues & Prices) [cite: 78-118]
        calculation_query = text("""
            SELECT 
                db.name AS delivery_boy,
                di.delivery_boy_id,
                SUM(di.regular_qty * pnc.refill_amount) AS regular_amt,
                SUM(di.nc_qty * (pnc.deposit_amount + pnc.refill_amount + pnc.document_charge + pnc.installation_charge + COALESCE(pnc.regulator_charge, 0))) AS nc_amt,
                SUM(di.dbc_qty * (pnc.deposit_amount + pnc.refill_amount + pnc.document_charge + pnc.installation_charge)) AS dbc_amt,
                SUM(di.tv_out_qty * pnc.deposit_amount) AS tv_refund,
                (
                    SUM(di.regular_qty * pnc.refill_amount) +
                    SUM(di.nc_qty * (pnc.deposit_amount + pnc.refill_amount + pnc.document_charge + pnc.installation_charge + COALESCE(pnc.regulator_charge, 0))) +
                    SUM(di.dbc_qty * (pnc.deposit_amount + pnc.refill_amount + pnc.document_charge + pnc.installation_charge)) -
                    SUM(di.tv_out_qty * pnc.deposit_amount)
                ) AS final_expected
            FROM delivery_issues di
            JOIN delivery_boys db ON di.delivery_boy_id = db.delivery_boy_id
            JOIN price_nc_components pnc ON di.cylinder_type_id = pnc.cylinder_type_id
            WHERE di.stock_day_id = :s_id
            GROUP BY di.delivery_boy_id, db.name
        """)
        results = db.execute(calculation_query, {"s_id": open_day.stock_day_id}).fetchall()

        # 4. Handle Update to Database (POST) - Only if not already updated [cite: 80-120]
        if request.method == "POST" and not is_updated:
            for row in results:
                db.execute(text("""
                    INSERT INTO delivery_expected_amount (stock_day_id, delivery_boy_id, expected_amount)
                    VALUES (:s_id, :db_id, :amt)
                """), {
                    "s_id": open_day.stock_day_id,
                    "db_id": row.delivery_boy_id,
                    "amt": row.final_expected
                })
            db.commit()
            is_updated = True
            success_message = "Expected cash amounts saved successfully!"

        return render_template("cash_settlement.html",
                               stock_date=open_day.stock_date,
                               results=results,
                               success_message=success_message,
                               is_updated=is_updated)
    finally:
        db.close()