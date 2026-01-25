from flask import Blueprint, render_template

from app.tenant import current_company_id


home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def home():
    company_id = current_company_id() or 1
    return render_template("procurement_home.html", company_id=company_id)