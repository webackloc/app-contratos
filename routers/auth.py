# routers/auth.py — v1.2.0 (2025-08-12)
# Base: v1.1.0 — preserva validação de `next`, sessão (user_id/username),
# redirects 303, compat com templates via request.app.state.templates.
# Novidades v1.2.0:
# - Verificação flexível de senha: password_hash -> hashed_password -> password (texto).
# - Rehash automático para bcrypt em password_hash quando logar com formato legado.
# - Busca de usuário com lower() para evitar divergência de caixa no username.

from datetime import datetime
import secrets
from typing import Optional

from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import func
from jinja2 import TemplateNotFound
from urllib.parse import urlparse

from database import SessionLocal
from auth_models import User  # garante criação de tabela no import

# Tente usar seu helper se existir; caso não, usamos o contexto local
try:
    from security import verify_password as _verify_password  # tipo: ignore
except Exception:
    _verify_password = None  # fallback abaixo

# Passlib: aceitar bcrypt e pbkdf2_sha256 como legados
from passlib.context import CryptContext
_pwd_ctx = CryptContext(schemes=["bcrypt", "pbkdf2_sha256"], deprecated="auto")

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _safe_next(next_url: Optional[str], default: str = "/dashboard") -> str:
    if not next_url:
        return default
    try:
        p = urlparse(next_url)
    except Exception:
        return default
    if p.scheme or p.netloc:
        return default
    if not next_url.startswith("/"):
        return default
    if next_url.startswith("/login"):
        return default
    return next_url

def _pick_password(user: User) -> Optional[str]:
    """Escolhe o primeiro campo de senha disponível em ordem de prioridade."""
    for attr in ("password_hash", "hashed_password", "password"):
        if hasattr(user, attr):
            val = getattr(user, attr)
            if val:
                return str(val)
    return None

def _verify_password_flex(plain: str, stored: str) -> tuple[bool, bool]:
    """
    Tenta validar a senha em múltiplos formatos.
    Retorna (ok, precisa_rehash):
      - ok=True se bateu
      - precisa_rehash=True se armazenado era texto puro/legado (devemos rehash)
    """
    # Se parece hash (começa com $), tente via passlib
    if stored.startswith("$"):
        try:
            ok = _pwd_ctx.verify(plain, stored)
            # rehash se o esquema estiver deprecado
            return ok, (ok and _pwd_ctx.needs_update(stored))
        except Exception:
            return False, False

    # Último recurso: comparar com texto puro legado
    try:
        if secrets.compare_digest(plain, stored):
            return True, True  # ok e precisa rehash
    except Exception:
        pass
    return False, False

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
    # Case-insensitive para evitar divergência de caixa
    user: Optional[User] = (
        db.query(User)
        .filter(func.lower(User.username) == func.lower(username))
        .first()
    )

    def _invalid():
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

    if not user or not getattr(user, "is_active", True):
        return _invalid()

    stored = _pick_password(user)
    ok = False
    needs_rehash = False

    # 1) Se houver helper do projeto, tente primeiro
    if _verify_password and stored:
        try:
            ok = _verify_password(password, stored)
        except Exception:
            ok = False

    # 2) Fallback robusto
    if not ok and stored:
        ok, needs_rehash = _verify_password_flex(password, stored)

    if not ok:
        return _invalid()

    # Rehash e normaliza para password_hash (bcrypt) se necessário
    if needs_rehash or (stored and not stored.startswith("$")):
        try:
            user.password_hash = _pwd_ctx.hash(password)
            # opcional: manter role/created_at/updated_at intactos; só atualiza updated_at
            if hasattr(user, "updated_at"):
                try:
                    setattr(user, "updated_at", datetime.utcnow())
                except Exception:
                    pass
            db.add(user)
            db.commit()
        except Exception:
            db.rollback()  # não falhe o login por erro de rehash

    # sessão ok
    request.session["user_id"] = int(getattr(user, "id"))
    request.session["username"] = getattr(user, "username", username)
    request.session["login_ts"] = datetime.utcnow().isoformat()

    nxt = _safe_next(next, default="/dashboard")
    return RedirectResponse(nxt, status_code=303)

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

@router.get("/api/health")
async def health(request: Request):
    return {"ok": True, "auth": bool(request.session.get("user_id"))}
