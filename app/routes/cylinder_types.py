from flask import Blueprint, render_template, request, Response
from sqlalchemy import text
from app.db.session import SessionLocal
import csv
import io

cylinder_types_bp = Blueprint("cylinder_types", __name__)


@cylinder_types_bp.route("/cylinder-types", methods=["GET"])
def cylinder_types():
    db = SessionLocal()
    try:
        # Fetching all types by default for the new modern UI
        cylinder_types = db.execute(
            text("""
                SELECT cylinder_type_id, code, category
                FROM cylinder_types
                ORDER BY category, code
            """)
        ).fetchall()

        return render_template("cylinder_types.html", cylinder_types=cylinder_types)
    finally:
        db.close()


@cylinder_types_bp.route("/cylinder-types/download", methods=["GET"])
def download_cylinder_types():
    db = SessionLocal()
    try:
        result = db.execute(
            text("SELECT cylinder_type_id, code, category FROM cylinder_types ORDER BY category, code")).fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Cylinder Code", "Category"])
        for row in result:
            writer.writerow([row.cylinder_type_id, row.code, row.category])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=cylinder_types_report.csv"}
        )
    finally:
        db.close()