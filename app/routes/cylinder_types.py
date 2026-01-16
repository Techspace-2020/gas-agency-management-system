from flask import Blueprint, render_template, request, Response,send_file
from sqlalchemy import text
from app.db.session import SessionLocal
import csv
import io
import pandas as pd

cylinder_types_bp = Blueprint("cylinder_types", __name__)


@cylinder_types_bp.route("/cylinder-types", methods=["GET"])
def cylinder_types():
    db = SessionLocal()
    try:
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        # Fetching all types by default for the new modern UI
        cylinder_types = db.execute(
            text("""
                SELECT cylinder_type_id, code, category
                FROM cylinder_types
                ORDER BY category, code
            """)
        ).fetchall()

        return render_template("cylinder_types.html", cylinder_types=cylinder_types, stock_date=open_day.stock_date if open_day else None)
    finally:
        db.close()


@cylinder_types_bp.route("/cylinder-types/download", methods=["GET"])
def download_cylinder_types():
    db = SessionLocal()
    try:
        #Get current open day
        curr = db.execute(text("SELECT stock_day_id, stock_date FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if not curr: 
            return "No open day", 404
        
        result = db.execute(
            text("SELECT code, category FROM cylinder_types ORDER BY category, code")).fetchall()
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df = pd.DataFrame(result, columns=['Cylinder Code', 'Category'])
            df.to_excel(writer, index=False, sheet_name='Cylinder_Types')
        output.seek(0)

        return send_file(output, download_name=f"Cylinder_Types_{curr.stock_date}.xlsx", as_attachment=True)
    finally:
        db.close()