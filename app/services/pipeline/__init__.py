"""Audit pipeline: stages, registry and dispatcher.

Layout:
    registry.py   stage name -> handler, ``@stage("NAME")`` decorator
    context.py    ``StageContext`` (typed) and ``build_context``
    runner.py     ``execute`` - runs one stage and turns failures into log lines
    logfmt.py     ``plog`` log line formatting
    helpers.py    small helpers shared by several stages
    stages/       the handlers themselves, one module per phase

Importing the package registers all stages.
"""

from app.services.pipeline import stages  # noqa: F401  (registers the handlers)
from app.services.pipeline.registry import STAGE_HANDLERS
from app.services.pipeline.runner import execute

__all__ = ["STAGE_HANDLERS", "execute"]
