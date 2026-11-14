// Progressive enhancement only: theme toggle and copy-code buttons. The site works without this file.
(function () {
  var d = document.documentElement;
  var dark = window.matchMedia("(prefers-color-scheme: dark)");

  var toggle = document.querySelector(".theme-toggle");
  if (toggle) {
    var current = function () {
      return d.getAttribute("data-theme") || (dark.matches ? "dark" : "light");
    };
    var sync = function () {
      toggle.setAttribute("aria-pressed", current() === "dark" ? "true" : "false");
    };
    toggle.hidden = false;
    sync();
    dark.addEventListener("change", sync);
    toggle.addEventListener("click", function () {
      var next = current() === "dark" ? "light" : "dark";
      d.setAttribute("data-theme", next);
      try {
        localStorage.setItem("theme", next);
      } catch (e) {}
      sync();
    });
  }

  if (!navigator.clipboard) return;
  var status = document.getElementById("copy-status");
  document.querySelectorAll("div.highlight").forEach(function (block) {
    var code = block.querySelector("pre code");
    if (!code) return;
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "copy";
    btn.textContent = "Copy";
    btn.addEventListener("click", function () {
      navigator.clipboard.writeText(code.innerText.replace(/\n$/, "")).then(
        function () {
          btn.textContent = "Copied";
          if (status) status.textContent = "Code copied to clipboard";
          setTimeout(function () {
            btn.textContent = "Copy";
            if (status) status.textContent = "";
          }, 2000);
        },
        function () {
          btn.textContent = "Copy failed";
        }
      );
    });
    block.appendChild(btn);
  });
})();
