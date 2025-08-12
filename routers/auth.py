# routers/auth.py — v1.1.0 (2025-08-11)
# Changes:
# - Preserves and validates `next` (prevents open redirects; only relative paths allowed).
# - Writes session keys `user_id` and `username` on successful login (compat with AuthRequiredMiddleware).
# - Uses 303 redirects (avoids POST resubmission).
# - Keeps compatibility with your existing templates setup (request.app.state.templates).
# - Returns 401 with the login template (or minimal HTML) on invalid credentials.
# - Keeps /api/health route.

from datetime import datetime
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy.orm import Session
from jinja2 import TemplateNotFound
from urllib.parse import urlparse

from database import SessionLocal
from security import verify_password
from auth_models import User  # ensures table creation on import

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _safe_next(next_url: str | None, default: str = "/dashboard") -> str:
    if not next_url:
        return default
    try:
        p = urlparse(next_url)
    except Exception:
        return default
    # allow only same-app relative paths (no scheme/netloc)
    if p.scheme or p.netloc:
        return default
    if not next_url.startswith("/"):
        return default
    # avoid redirect loops to /login
    if next_url.startswith("/login"):
        return default
    return next_url

@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request):
    nxt = _safe_next(request.query_params.get("next"), default="/dashboard")
    try:
        return request.app.state.templates.TemplateResponse(
            "login.html",
            {"request": request, "ano": datetime.now().year, "next": nxt},
        )
    except TemplateNotFound:
        html = f"""
        <!doctype html><meta charset="utf-8">
        <title>Login</title>
        <form method="post" style="max-width:340px;margin:80px auto;font-family:system-ui">
          <h3>Login</h3>
          <div><input name="username" placeholder="Usuário" required style="width:100%;padding:8px;margin:6px 0;"></div>
          <div><input name="password" type="password" placeholder="Senha" required style="width:100%;padding:8px;margin:6px 0;"></div>
          <input type="hidden" name="next" value="{nxt}">
          <button style="padding:8px 16px;">Entrar</button>
        </form>"""
        return HTMLResponse(html)

@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form(default="/dashboard"),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.username == username).first()

    if not user or not getattr(user, "is_active", True) or not verify_password(password, user.password_hash):
        nxt = _safe_next(next, default="/dashboard")
        try:
            return request.app.state.templates.TemplateResponse(
                "login.html",
                {
                    "request": request,
                    "erro": "Usuário ou senha inválidos.",
                    "ano": datetime.now().year,
                    "next": nxt,
                },
                status_code=401,
            )
        except TemplateNotFound:
            return HTMLResponse("<h3>Usuário ou senha inválidos</h3>", status_code=401)

    # write session in-place (do not replace the entire session object)
    request.session["user_id"] = int(getattr(user, "id"))
    request.session["username"] = getattr(user, "username", username)
    request.session["login_ts"] = datetime.utcnow().isoformat()  # optional

    nxt = _safe_next(next, default="/dashboard")
    return RedirectResponse(nxt, status_code=303)

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

@router.get("/api/health")
async def health(request: Request):
    return {"ok": True, "auth": bool(request.session.get("user_id"))}
