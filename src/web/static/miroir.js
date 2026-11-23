// Behaviour that used to sit in inline handlers, which the content security
// policy refuses.
// Logging out leaves nothing in the page's storage (htmx keeps the last
// address there), also where a browser ignores Clear-Site-Data.
document.addEventListener("submit", (event) => {
  if (event.target.matches("form[data-logout]")) {
    sessionStorage.clear();
    localStorage.clear();
  }
});
document.addEventListener("click", (event) => {
  const copy = event.target.closest("button[data-copy-previous]");
  if (copy) {
    navigator.clipboard
      .writeText(copy.previousElementSibling.value)
      .then(() => (copy.textContent = "Copied"));
    return;
  }
  const back = event.target.closest("a[data-history-back]");
  if (back && history.length > 1) {
    event.preventDefault();
    history.back();
  }
});
