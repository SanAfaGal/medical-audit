"""Page routers: serve full HTML pages."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

_STATIC_DIR = Path("app/static")

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()


def asset(path: str) -> str:
    """URL of a static file with its mtime as version, so browsers never serve a stale script after a deploy."""
    try:
        version = int((_STATIC_DIR / path).stat().st_mtime)
    except OSError:
        version = 0
    return f"/static/{path}?v={version}"


templates.env.globals["asset"] = asset


@router.get("/", response_class=HTMLResponse)
async def pipeline_page(request: Request):
    return templates.TemplateResponse("pipeline/index.html", {"request": request})


@router.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    return templates.TemplateResponse("audit/index.html", {"request": request})


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    return templates.TemplateResponse("settings/index.html", {"request": request})


@router.get("/help", response_class=HTMLResponse)
async def help_page(request: Request):
    return templates.TemplateResponse("help/index.html", {"request": request})
