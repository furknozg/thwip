(() => {
  const installSiteIcons = () => {
    const icons = [
      ["icon", "/favicon.png?v=1", "1254x1254"],
      ["icon", "/favicon-32x32.png?v=1", "32x32"],
      ["apple-touch-icon", "/apple-touch-icon.png?v=1", "180x180"],
      ["manifest", "/site.webmanifest?v=1", null],
    ];
    icons.forEach(([rel, href, sizes]) => {
      const link = document.createElement("link");
      link.rel = rel;
      link.href = href;
      if (sizes) link.sizes = sizes;
      if (rel === "icon") link.type = "image/png";
      document.head.append(link);
    });
  };

  const escapeHtml = (value) => value.replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);

  const isToml = (source) =>
    /(^|\n)\s*\[\[?[^\]]+\]?\]/.test(source) ||
    /(^|\n)\s*[A-Za-z_][\w-]*\s*=/.test(source);

  const highlightToml = (source) => source.split("\n").map((line) => {
    let rendered = escapeHtml(line);
    rendered = rendered.replace(/^(\s*)(\[\[?[^\]]+\]?\])/, "$1<span class=\"toml-table\">$2</span>");
    rendered = rendered.replace(/^(\s*)([A-Za-z_][\w-]*)(\s*=)/, "$1<span class=\"toml-key\">$2</span>$3");
    rendered = rendered.replace(/(&quot;.*?&quot;)/g, "<span class=\"toml-string\">$1</span>");
    rendered = rendered.replace(/\b(\d+)\b/g, "<span class=\"toml-number\">$1</span>");
    return rendered.replace(/(\s#.*)$/, "<span class=\"toml-comment\">$1</span>");
  }).join("\n");

  const copy = async (text) => {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const selection = document.createElement("textarea");
    selection.value = text;
    selection.setAttribute("readonly", "");
    selection.style.position = "fixed";
    selection.style.opacity = "0";
    document.body.append(selection);
    selection.select();
    document.execCommand("copy");
    selection.remove();
  };

  const enhanceCodeExamples = () => document.querySelectorAll("pre > code").forEach((code) => {
    if (code.dataset.thwipEnhanced) return;
    code.dataset.thwipEnhanced = "true";
    const source = code.textContent;
    const toml = isToml(source);
    const wrapper = document.createElement("div");
    wrapper.className = "code-example";
    code.parentElement.before(wrapper);
    wrapper.append(code.parentElement);

    const toolbar = document.createElement("div");
    toolbar.className = "code-toolbar";
    toolbar.innerHTML = `<span>${toml ? "TOML" : "TEXT"}</span>`;
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Copy";
    button.addEventListener("click", async () => {
      try {
        await copy(source);
        button.textContent = "Copied";
      } catch {
        button.textContent = "Unavailable";
      }
      window.setTimeout(() => { button.textContent = "Copy"; }, 1600);
    });
    toolbar.append(button);
    wrapper.prepend(toolbar);

    if (toml) {
      code.classList.add("language-toml");
      code.innerHTML = highlightToml(source);
    }
  });

  window.thwipEnhanceCodeExamples = enhanceCodeExamples;
  installSiteIcons();
  enhanceCodeExamples();
})();
