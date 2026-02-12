from flask import Flask
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from sqlalchemy import text
from app.db.session import SessionLocal
from dotenv import load_dotenv
import os

# Import the User class from your auth route file
from app.routes.auth import auth_bp, User

# Import other blueprints
from app.routes.stock_day import stock_day_bp
from app.routes.delivery_boys import delivery_boys_bp
from app.routes.cylinder_types import cylinder_types_bp
from app.routes.opening_stock import opening_stock_bp
from app.routes.iocl_movements import iocl_movements_bp
from app.routes.delivery_transactions import delivery_transactions_bp
from app.routes.closing_stock import closing_stock_bp
from app.routes.cash_settlement import cash_settlement_bp
from app.routes.cash_collection import cash_collection_bp
from app.routes.cash_reconciliation import cash_reconciliation_bp
from app.routes.office_sales import office_sales_bp
# NEW: Import for Range Reports
from app.routes.reports import reports_bp

load_dotenv()


def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY")

    # Enable CSRF protection
    csrf = CSRFProtect(app)

    # 1. Initialize Flask-Login
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'  # Where to redirect if not logged in
    login_manager.login_message_category = "info"
    login_manager.init_app(app)

    # 2. Define the User Loader
    @login_manager.user_loader
    def load_user(user_id):
        db = SessionLocal()
        try:
            # Query the user by ID
            row = db.execute(
                text("SELECT user_id, username FROM users WHERE user_id = :id"),
                {"id": user_id}
            ).fetchone()
            if row:
                return User(user_id=row.user_id, username=row.username)
            return None
        finally:
            db.close()

    # 3. Register Blueprints
    app.register_blueprint(stock_day_bp)
    app.register_blueprint(delivery_boys_bp)
    app.register_blueprint(cylinder_types_bp)
    app.register_blueprint(opening_stock_bp)
    app.register_blueprint(iocl_movements_bp)
    app.register_blueprint(delivery_transactions_bp)
    app.register_blueprint(closing_stock_bp)
    app.register_blueprint(cash_settlement_bp)
    app.register_blueprint(cash_collection_bp)
    app.register_blueprint(cash_reconciliation_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(office_sales_bp)
    # NEW: Register the Reports Blueprint
    app.register_blueprint(reports_bp)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)