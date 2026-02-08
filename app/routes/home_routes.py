from flask import Blueprint, render_template

from app.procurement.flow_policy import build_process_steps
from app.tenant import DEFAULT_TENANT_ID, current_tenant_id


home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def home():
    tenant_id = current_tenant_id() or DEFAULT_TENANT_ID
    process_stage = "solicitacao"
    return render_template(
        "procurement_home.html",
        tenant_id=tenant_id,
        process_stage=process_stage,
        process_steps=build_process_steps(process_stage),
    )
