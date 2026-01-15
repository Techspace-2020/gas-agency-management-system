import csv
import io
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from sqlalchemy import text
from app.db.session import SessionLocal

opening_stock_bp = Blueprint("opening_stock", __name__)

# Helper to get current and previous days
def get_stock_days(db):
    prev = db.execute(text("""
        SELECT stock_day_id, stock_date FROM stock_days 
        WHERE status = 'CLOSED' ORDER BY stock_date DESC LIMIT 1
    """)).fetchone()
    curr = db.execute(text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
    return prev, curr

@opening_stock_bp.route("/opening-stock")
def summary_view():
    db = SessionLocal()
    try:
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        prev_day, open_day = get_stock_days(db)
        if not open_day:
            return "No Active Stock Day Found", 404

        is_confirmed = bool(db.execute(text("SELECT 1 FROM daily_stock_summary WHERE stock_day_id = :id"),
                                       {"id": open_day.stock_day_id}).fetchone())

        rows = db.execute(text("""
            SELECT ct.code AS cylinder_type,
                COALESCE(ods.opening_filled, pds.closing_filled, 0) AS opening_filled,
                COALESCE(ods.opening_empty, pds.closing_empty, 0) AS opening_empty,
                COALESCE(ods.defective_empty_vehicle, pds.defective_empty_vehicle, 0) AS defective_empty_vehicle,
                (COALESCE(ods.opening_filled, pds.closing_filled, 0) + 
                 COALESCE(ods.opening_empty, pds.closing_empty, 0) + 
                 COALESCE(ods.defective_empty_vehicle, pds.defective_empty_vehicle, 0)) AS total_stock
            FROM cylinder_types ct
            LEFT JOIN daily_stock_summary ods ON ods.cylinder_type_id = ct.cylinder_type_id AND ods.stock_day_id = :open_id
            LEFT JOIN daily_stock_summary pds ON pds.cylinder_type_id = ct.cylinder_type_id AND pds.stock_day_id = :prev_id
            ORDER BY ct.code
        """), {"open_id": open_day.stock_day_id, "prev_id": prev_day.stock_day_id if prev_day else 0}).fetchall()

        return render_template("opening_stock_summary.html", rows=rows, is_confirmed=is_confirmed, stock_date=open_day.stock_date)
    finally:
        db.close()

@opening_stock_bp.route("/opening-stock/reconcile", methods=["GET", "POST"])
def reconcile_view():
    db = SessionLocal()
    try:
        prev_day, open_day = get_stock_days(db)
        if request.method == "POST":
            # Corrected Save Logic
            for key, value in request.form.items():
                if key.startswith("actual_"):
                    parts = key.split("_")
                    b_id, c_id = int(parts[1]), int(parts[2])
                    actual = int(value or 0)

                    expected = db.execute(text("""
                        SELECT COALESCE(SUM(regular_qty), 0) FROM delivery_issues 
                        WHERE stock_day_id=:p AND delivery_boy_id=:b AND cylinder_type_id=:c
                    """), {"p": prev_day.stock_day_id, "b": b_id, "c": c_id}).scalar() or 0

                    prev_v = db.execute(text("""
                        SELECT COALESCE(empty_qty, 0) FROM delivery_vehicle_empty_stock 
                        WHERE stock_day_id < :o AND delivery_boy_id = :b AND cylinder_type_id = :c 
                        ORDER BY stock_day_id DESC LIMIT 1
                    """), {"o": open_day.stock_day_id, "b": b_id, "c": c_id}).scalar() or 0

                    new_v = (prev_v + expected) - actual

                    db.execute(text("""
                        INSERT INTO delivery_vehicle_empty_stock (stock_day_id, delivery_boy_id, cylinder_type_id, empty_qty)
                        VALUES (:o, :b, :c, :v) ON DUPLICATE KEY UPDATE empty_qty = :v
                    """), {"o": open_day.stock_day_id, "b": b_id, "c": c_id, "v": new_v})

            # Sync with summary table
            db.execute(text("""
                INSERT INTO daily_stock_summary (stock_day_id, cylinder_type_id, opening_filled, opening_empty, defective_empty_vehicle)
                SELECT :o, pds.cylinder_type_id, pds.closing_filled, 
                    ((pds.closing_empty + pds.defective_empty_vehicle) - COALESCE(v.v_sum, 0)), 
                    COALESCE(v.v_sum, 0)
                FROM daily_stock_summary pds
                LEFT JOIN (
                    SELECT cylinder_type_id, SUM(empty_qty) as v_sum 
                    FROM delivery_vehicle_empty_stock WHERE stock_day_id = :o GROUP BY cylinder_type_id
                ) v ON v.cylinder_type_id = pds.cylinder_type_id
                WHERE pds.stock_day_id = :p
                ON DUPLICATE KEY UPDATE defective_empty_vehicle = VALUES(defective_empty_vehicle), opening_empty = VALUES(opening_empty)
            """), {"o": open_day.stock_day_id, "p": prev_day.stock_day_id})

            db.commit()
            flash("Reconciliation saved successfully.", "success")
            return redirect(url_for("opening_stock.summary_view"))

        # Fetch rows for UI
        rows = db.execute(text("""
            SELECT db.delivery_boy_id, db.name AS delivery_boy, ct.cylinder_type_id, ct.code AS cylinder_type,
            COALESCE((SELECT SUM(regular_qty) FROM delivery_issues WHERE stock_day_id = :p AND delivery_boy_id = db.delivery_boy_id AND cylinder_type_id = ct.cylinder_type_id), 0) AS expected_empty,
            COALESCE((SELECT empty_qty FROM delivery_vehicle_empty_stock WHERE stock_day_id < :o AND delivery_boy_id = db.delivery_boy_id AND cylinder_type_id = ct.cylinder_type_id ORDER BY stock_day_id DESC LIMIT 1), 0) AS prev_vehicle_empty
            FROM delivery_boys db CROSS JOIN cylinder_types ct
            WHERE (SELECT COUNT(*) FROM delivery_issues WHERE stock_day_id = :p AND delivery_boy_id = db.delivery_boy_id AND cylinder_type_id = ct.cylinder_type_id) > 0
               OR (SELECT COUNT(*) FROM delivery_vehicle_empty_stock WHERE stock_day_id < :o AND delivery_boy_id = db.delivery_boy_id AND cylinder_type_id = ct.cylinder_type_id AND empty_qty > 0) > 0
            ORDER BY db.name, ct.code
        """), {"p": prev_day.stock_day_id if prev_day else 0, "o": open_day.stock_day_id}).fetchall()

        return render_template("opening_stock_reconciliation.html", rows=rows, stock_date=open_day.stock_date)
    finally:
        db.close()

@opening_stock_bp.route("/opening-stock/download-vehicle-report")
def download_vehicle_report():
    db = SessionLocal()
    try:
        curr = db.execute(text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if not curr: return "No open day", 404

        # Query for report data
        results = db.execute(text("""
            SELECT db.name AS delivery_boy, ct.code AS cylinder_type, v.empty_qty
            FROM delivery_vehicle_empty_stock v
            JOIN delivery_boys db ON v.delivery_boy_id = db.delivery_boy_id
            JOIN cylinder_types ct ON v.cylinder_type_id = ct.cylinder_type_id
            WHERE v.stock_day_id = :s_id AND v.empty_qty > 0
            ORDER BY db.name, ct.code
        """), {"s_id": curr.stock_day_id}).fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Delivery Boy", "Cylinder Type", "Empty Qty in Vehicle"])
        for row in results:
            writer.writerow([row.delivery_boy, row.cylinder_type, row.empty_qty])

        return Response(output.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=vehicle_stock_{curr.stock_date}.csv"})
    finally:
        db.close()

@opening_stock_bp.route("/opening-stock/confirm-all", methods=["POST"])
def confirm_all_returned():
    db = SessionLocal()
    try:
        prev_day, open_day = get_stock_days(db)
        db.execute(text("""
            INSERT INTO daily_stock_summary (stock_day_id, cylinder_type_id, opening_filled, opening_empty, defective_empty_vehicle)
            SELECT :o, cylinder_type_id, closing_filled, closing_empty, defective_empty_vehicle
            FROM daily_stock_summary WHERE stock_day_id = :p
        """), {"o": open_day.stock_day_id, "p": prev_day.stock_day_id})
        db.commit()
        return redirect(url_for("opening_stock.summary_view"))
    finally:
        db.close()