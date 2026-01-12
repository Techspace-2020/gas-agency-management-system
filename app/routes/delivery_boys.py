from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from sqlalchemy import text
from app.db.session import SessionLocal
import csv
import io

delivery_boys_bp = Blueprint("delivery_boys", __name__)


@delivery_boys_bp.route("/delivery-boys", methods=["GET", "POST"])
def delivery_boys():
    db = SessionLocal()
    try:
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
        return render_template("delivery_boys.html", delivery_boys=results)
    finally:
        db.close()


@delivery_boys_bp.route("/delivery-boys/download")
def download_delivery_boys():
    db = SessionLocal()
    try:
        result = db.execute(text("SELECT name, mobile, is_active FROM delivery_boys ORDER BY name")).fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Name", "Mobile", "Status"])
        for row in result:
            writer.writerow([row.name, row.mobile, "Active" if row.is_active else "Inactive"])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=delivery_boys_report.csv"}
        )
    finally:
        db.close()