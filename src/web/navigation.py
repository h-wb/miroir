"""The header's links to the control plane's admin pages, as data (package
split, step 10).

The admin part mounts them, and so does the `view` process, which serves the
viewer alone: its pages carry the same header without importing the admin part
(`src.web.app`) or the control plane's service. Imports only the web shell.
"""

from src.web.shell import Link

# Before every other part's.
LINKS = (Link("/", "Connections"), Link("/config", "Configuration"))
