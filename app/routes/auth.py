from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, UserMixin
from werkzeug.security import check_password_hash, generate_password_hash
from app.db.session import SessionLocal
from sqlalchemy import text

auth_bp = Blueprint("auth", __name__)

class User(UserMixin):
    def __init__(self, user_id, username):
        self.id = str(user_id)
        self.username = username

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        db = SessionLocal()
        try:
            result = db.execute(
                text("SELECT user_id, username, password_hash, full_name, is_approved FROM users WHERE username = :u"),
                {"u": username}
            ).fetchone()

            if result and check_password_hash(result.password_hash, password):
                if result.is_approved == 1:
                    user_obj = User(user_id=result.user_id, username=result.username)
                    login_user(user_obj)
                    flash(f"{result.username} Logged in successfully!", "success")
                    return redirect(url_for('stock_day.dashboard'))
                else:
                    flash("Your account is pending administrator approval.", "warning")
            else:
                flash("Invalid username or password", "danger")
        finally:
            db.close()

    return render_template("login.html")

@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        full_name = request.form.get("full_name")
        username = request.form.get("username")
        password = request.form.get("password")
        confirm_password = request.form.get("confirm_password")

        if password != confirm_password:
            flash("Passwords do not match. Please try again.", "danger")
            return render_template("register.html")

        db = SessionLocal()
        try:
            exists = db.execute(text("SELECT 1 FROM users WHERE username = :u"), {"u": username}).fetchone()
            if exists:
                flash(f"This username is already taken. {username}", "danger")
            else:
                hashed_pw = generate_password_hash(password)
                db.execute(text("""
                    INSERT INTO users (username, password_hash, full_name, is_approved) 
                    VALUES (:u, :p, :f, 0)
                """), {"u": username, "p": hashed_pw, "f": full_name})
                db.commit()
                flash("Registration successful! Your account is now pending approval.", "success")
        except Exception as e:
            flash(f"An error occurred: {str(e)}", "danger")
        finally:
            db.close()

    return render_template("register.html")

@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out.", "info")
    return redirect(url_for('auth.login'))