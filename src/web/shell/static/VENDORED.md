# Vendored files

Served as they are, never edited. To update one, take the file from the
package's published archive, check the archive against the registry's integrity
value, and replace the row; `tests/test_web_static.py` compares the file with
its SHA-256 here.

| File | Package | Version | SHA-256 | From |
|---|---|---|---|---|
| `htmx.min.js` | htmx.org | 2.0.11 | `d6fdc75f204e6bdefa99b69bf1e6d4ac69b8a364f77929f45c13476b4000f717` | `dist/htmx.min.js` of https://registry.npmjs.org/htmx.org/-/htmx.org-2.0.11.tgz (npm integrity `sha512-Thx/WtpeOQqSrqBCw/A1cwGJGg4UrVa3+sW0GmrM3p4gJgO89ecH4qtbnyzDDWFvBTqjnIMCgELTNt636dtamA==`), licence 0BSD |

htmx is configured in `templates/base.html` (`htmx-config`): no eval, no script
tags in swapped content, no injected indicator styles, same-origin requests
only. The content security policy would refuse the first three anyway.
