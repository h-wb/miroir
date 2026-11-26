"""The viewer: reading the warehouse and showing it (package split, step 6).

The reader (`reader`, `database`), the timeline's routes and pages
(`timeline`, `timeline_pages`, `conversations`), the `/media` view route and its
withheld check (`media`, step 7), their templates and their static files. It reads what it needs of the installation as one
`ViewerSettings` (`settings`), built by the composition root, and imports
nothing of the control plane. Its pages extend the base template by name; its
static files are served under the same `/static/` paths as the shell's.
"""

from pathlib import Path

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
