"""The viewer: reading the warehouse and showing it (package split, step 6).

The reader (`reader`, `database`), the timeline's routes and pages
(`timeline`, `timeline_pages`, `conversations`), the `/media` view route and its
withheld check (`media`, step 7), their templates and their static files. It reads what it needs of the installation as one
`ViewerSettings` (`settings`), built by the composition root, and imports
nothing of the control plane. Its pages extend the base template by name; its
static files are served under a prefix of their own, `/static/viewer/`
(`part.STATIC_URL`), so a gateway can send them to the `view` process.
"""

from pathlib import Path

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
