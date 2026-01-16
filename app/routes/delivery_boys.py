from flask import Blueprint, render_template, request, redirect, url_for, flash, Response,send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import csv
import io
import pandas as pd

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

        # Handle Create Action
        if request.method == "POST":
            action = request.form.get("action")
            if action == "create":
                name = request.form.get("name", "").strip()
                mobile = request.form.get("mobile", "").strip()

                if not name or not mobile:
                    flash("Name and Mobile are required", "error")
                elif not mobile.isdigit() or len(mobile) != 10:
                    flash("Enter a valid 10-digit mobile number", "error")
                else:
                    # Check for duplicates
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

        # Fetch all delivery boys for the table
        results = db.execute(
            text("SELECT delivery_boy_id, name, mobile, is_active FROM delivery_boys ORDER BY name")).fetchall()
        return render_template("delivery_boys.html", delivery_boys=results, stock_date=open_day.stock_date if open_day else None)
    finally:
        db.close()


@delivery_boys_bp.route("/delivery-boys/download")
def download_delivery_boys():
    db = SessionLocal()
    try:
        #Get current open day
        curr = db.execute(text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if not curr: 
            return "No open day", 404
        
        result = db.execute(text("SELECT name, mobile, is_active FROM delivery_boys ORDER BY name")).fetchall()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df = pd.DataFrame(result, columns=['Name', 'Mobile', 'Is Active'])
            df.to_excel(writer, index=False, sheet_name='Delivery_Boys')
        output.seek(0)

        return send_file(output, download_name=f"Delivery_Boys_{curr.stock_date}.xlsx", as_attachment=True)
    finally:
        db.close()