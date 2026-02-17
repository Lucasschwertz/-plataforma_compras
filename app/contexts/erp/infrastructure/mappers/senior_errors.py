from __future__ import annotations


_REJECTION_CODE_MARKERS: tuple[tuple[str, str], ...] = (
    ("valid", "VALIDATION"),
    ("inval", "VALIDATION"),
    ("supplier", "SUPPLIER"),
    ("fornecedor", "SUPPLIER"),
    ("price", "PRICE"),
    ("preco", "PRICE"),
)


def normalize_rejection_code(raw: str | None) -> str | None:
    normalized = str(raw or "").strip().lower()
    if not normalized:
        return None
    upper = normalized.upper()
    if upper in {"VALIDATION", "SUPPLIER", "PRICE"}:
        return upper
    for marker, code in _REJECTION_CODE_MARKERS:
        if marker in normalized:
            return code
    return "VALIDATION"


def is_temporary_failure(raw: str | None) -> bool:
    normalized = str(raw or "").strip().lower()
    if not normalized:
        return False
    temporary_markers = (
        "timeout",
        "temporar",
        "temporary",
        "connection",
        "conexao",
        "unavailable",
        "indispon",
        "429",
        "502",
        "503",
        "504",
    )
    return any(marker in normalized for marker in temporary_markers)


def is_definitive_failure(raw: str | None) -> bool:
    normalized = str(raw or "").strip().lower()
    if not normalized:
        return False
    definitive_markers = (
        "422",
        "rejected",
        "rejeit",
        "invalid",
        "inval",
        "fornecedor",
        "supplier",
        "price",
        "preco",
    )
    return any(marker in normalized for marker in definitive_markers)


def classify_response_status(raw_status: str | None, message: str | None) -> str:
    status = str(raw_status or "").strip().lower()
    if status in {"accepted", "ok", "success", "erp_accepted"}:
        return "accepted"
    if status in {"rejected", "reject", "failed", "error", "erp_error"}:
        if is_temporary_failure(message):
            return "temporary_failure"
        return "rejected"
    if status in {"temporary_failure", "temporary", "retry", "queued"}:
        return "temporary_failure"
    if is_definitive_failure(message):
        return "rejected"
    if is_temporary_failure(message):
        return "temporary_failure"
    return "accepted"

