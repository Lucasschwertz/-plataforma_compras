(function () {
  const FALLBACK_TEXT = "Nao informado";

  function fallback(value) {
    if (value === null || value === undefined) return FALLBACK_TEXT;
    const text = String(value).trim();
    return text ? text : FALLBACK_TEXT;
  }

  function escapeHtml(value) {
    return fallback(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function withWorkspace(path, workspaceId) {
    if (!workspaceId) return path;
    const separator = path.includes("?") ? "&" : "?";
    return `${path}${separator}workspace_id=${encodeURIComponent(workspaceId)}`;
  }

  function setButtonBusy(button, busy, labels) {
    if (!button) return;
    const loadingLabel = labels?.loading || "Processando";
    const idleLabel = labels?.idle || button.dataset.idleLabel || button.textContent || "";
    if (!button.dataset.idleLabel) {
      button.dataset.idleLabel = idleLabel;
    }
    button.disabled = Boolean(busy);
    button.textContent = busy ? loadingLabel : button.dataset.idleLabel;
  }

  function focusGlobalSearch() {
    const target =
      document.querySelector("[data-global-search]") ||
      document.querySelector("#searchInput") ||
      document.querySelector("input[type='search']") ||
      document.querySelector("input[name='search']");
    if (!target) return false;
    target.focus();
    if (typeof target.select === "function") target.select();
    return true;
  }

  function setupGlobalShortcuts() {
    if (window.__platformShortcutsBound) return;
    window.__platformShortcutsBound = true;

    document.addEventListener("keydown", (event) => {
      if (event.defaultPrevented) return;
      const target = event.target;
      const tag = target && target.tagName ? target.tagName.toLowerCase() : "";
      const isTyping = tag === "input" || tag === "textarea" || target?.isContentEditable;

      if (event.key === "/" && !isTyping) {
        event.preventDefault();
        focusGlobalSearch();
        return;
      }

      if (!event.altKey) return;
      if (isTyping) return;

      const workspaceSelect = document.getElementById("workspaceSelect");
      const workspaceId = workspaceSelect ? workspaceSelect.value : "";

      if (event.key === "1") {
        event.preventDefault();
        window.location.href = withWorkspace("/", workspaceId);
        return;
      }
      if (event.key === "2") {
        event.preventDefault();
        window.location.href = withWorkspace("/procurement/inbox", workspaceId);
        return;
      }
      if (event.key === "3") {
        event.preventDefault();
        window.location.href = withWorkspace("/procurement/inbox?type=rfq", workspaceId);
      }
    });
  }

  window.PlatformUI = {
    FALLBACK_TEXT,
    fallback,
    escapeHtml,
    withWorkspace,
    setButtonBusy,
    focusGlobalSearch,
    setupGlobalShortcuts,
  };
})();
