from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import io
import pandas as pd
from datetime import date, datetime

delivery_boys_bp = Blueprint("delivery_boys", __name__)


@delivery_boys_bp.route("/delivery-boys", methods=["GET", "POST"])
def delivery_boys():
    db = SessionLocal()
    try:
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        if request.method == "POST":
            action = request.form.get("action")

            # --- TOGGLE STATUS WITH BALANCE CHECK ---
            if action == "toggle_status":
                boy_id = request.form.get("boy_id")
                current_status = int(request.form.get("current_status"))

                if current_status == 1:  # Attempting to deactivate
                    # Fetch the absolute latest balance record for this specific boy
                    balance_info = db.execute(text("""
                        SELECT closing_balance, balance_status 
                        FROM delivery_cash_balance 
                        WHERE delivery_boy_id = :id 
                        ORDER BY balance_id DESC LIMIT 1
                    """), {"id": boy_id}).fetchone()

                    if balance_info:
                        has_money_pending = abs(balance_info.closing_balance) > 0.01
                        is_pending_status = (balance_info.balance_status == 'PENDING')

                        if has_money_pending or is_pending_status:
                            flash(
                                f"Cannot deactivate: Outstanding balance of ₹{balance_info.closing_balance:,.2f} found. "
                                f"Settlement Status: {balance_info.balance_status}.", "error")
                            return redirect(url_for("delivery_boys.delivery_boys"))

                new_status = 0 if current_status == 1 else 1
                db.execute(text("UPDATE delivery_boys SET is_active = :status WHERE delivery_boy_id = :id"),
                           {"status": new_status, "id": boy_id})
                db.commit()
                flash("Delivery boy status updated successfully.", "success")
                return redirect(url_for("delivery_boys.delivery_boys"))

            # --- CREATE ACTION ---
            if action == "create":
                name = request.form.get("name", "").strip()
                mobile = request.form.get("mobile", "").strip()
                if not name or not mobile:
                    flash("Name and Mobile are required", "error")
                elif not mobile.isdigit() or len(mobile) != 10:
                    flash("Enter a valid 10-digit mobile number", "error")
                else:
                    existing = db.execute(text("SELECT 1 FROM delivery_boys WHERE name=:n OR mobile=:m"),
                                          {"n": name, "m": mobile}).fetchone()
                    if existing:
                        flash("Delivery boy or mobile already exists", "error")
                    else:
                        db.execute(text("INSERT INTO delivery_boys (name, mobile, is_active) VALUES (:n, :m, 1)"),
                                   {"n": name, "m": mobile})
                        db.commit()
                        flash(f"Delivery boy '{name}' added successfully", "success")
                return redirect(url_for("delivery_boys.delivery_boys"))

        # --- FETCH ALL BOYS WITH UPDATED BALANCE SUBQUERY ---
        results = db.execute(text("""
            SELECT 
                db.delivery_boy_id, 
                db.name, 
                db.mobile, 
                db.is_active,
                COALESCE((
                    SELECT closing_balance 
                    FROM delivery_cash_balance 
                    WHERE delivery_boy_id = db.delivery_boy_id 
                    ORDER BY balance_id DESC LIMIT 1
                ), 0) as current_balance
            FROM delivery_boys db
            ORDER BY db.name
        """)).fetchall()

        return render_template("delivery_boys.html",
                               delivery_boys=results,
                               stock_date=open_day.stock_date if open_day else "No Open Day")
    finally:
        db.close()


@delivery_boys_bp.route("/delivery-boys/download")
def download_delivery_boys():
    db = SessionLocal()
    try:
        curr = db.execute(text("SELECT stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if not curr: return "No open day", 404
        result = db.execute(text("SELECT name, mobile, is_active FROM delivery_boys ORDER BY name")).fetchall()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df = pd.DataFrame(result, columns=['Name', 'Mobile', 'Is Active'])
            df['Is Active'] = df['Is Active'].apply(lambda x: 'Active' if x == 1 else 'Inactive')
            df.to_excel(writer, index=False, sheet_name='Delivery_Boys')
        output.seek(0)
        return send_file(output, download_name=f"Delivery_Boys_{curr.stock_date}.xlsx", as_attachment=True)
    finally:
        db.close()