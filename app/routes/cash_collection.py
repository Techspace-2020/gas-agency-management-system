from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import text
from app.db.session import SessionLocal

cash_collection_bp = Blueprint("cash_collection", __name__)

@cash_collection_bp.route("/cash-collection", methods=["GET", "POST"])
def collection_view():
    db = SessionLocal()
    try:
        open_day = db.execute(text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if not open_day:
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # Check for Lock (Final Reconciliation or Existing Collection)
        saved_records = db.execute(text("SELECT * FROM delivery_cash_deposit WHERE stock_day_id = :s_id"), {"s_id": s_id}).fetchall()
        is_locked = len(saved_records) > 0

        if request.method == "POST" and not is_locked:
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

        saved_map = {row.delivery_boy_id: row for row in saved_records}
        display_entities = db.execute(text("SELECT delivery_boy_id, name FROM delivery_boys")).fetchall()

        return render_template("cash_collection.html", stock_date=open_day.stock_date,
                               entities=display_entities, saved_map=saved_map, is_locked=is_locked)
    finally:
        db.close()