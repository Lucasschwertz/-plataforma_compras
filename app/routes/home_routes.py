from flask import Blueprint, render_template

from app.tenant import DEFAULT_TENANT_ID, current_tenant_id


home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def home():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    return render_template("procurement_home.html", tenant_id=tenant_id)
