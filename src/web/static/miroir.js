// Behaviour that used to sit in inline handlers, which the content security
// policy refuses.
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
