# routers/auth.py — v1.0.0 (11/08/2025)
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy.orm import Session
from jinja2 import TemplateNotFound

from database import SessionLocal
from security import verify_password
from auth_models import User  # garante criação da tabela ao importar

router = APIRouter()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request):
    try:
        return request.app.state.templates.TemplateResponse("login.html", {"request": request})
    except Exception:
        # fallback simples se não existir login.html
        html = """
        <!doctype html><meta charset="utf-8">
        <title>Login</title>
        <form method="post" style="max-width:340px;margin:80px auto;font-family:system-ui">
          <h3>Login</h3>
          <div><input name="username" class="form-control" placeholder="Usuário" required style="width:100%;padding:8px;margin:6px 0;"></div>
          <div><input name="password" type="password" class="form-control" placeholder="Senha" required style="width:100%;padding:8px;margin:6px 0;"></div>
          <input type="hidden" name="next" value="{{ next }}">
          <button class="btn btn-primary" style="padding:8px 16px;">Entrar</button>
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
    if not user or not user.is_active or not verify_password(password, user.password_hash):
        # voltar com erro simples
        try:
            return request.app.state.templates.TemplateResponse(
                "login.html",
                {"request": request, "erro": "Usuário ou senha inválidos."},
                status_code=401,
            )
        except TemplateNotFound:
            return HTMLResponse("<h3>Usuário ou senha inválidos</h3>", status_code=401)

    request.session["user_id"] = user.id
    request.session["username"] = user.username
    return RedirectResponse(next or "/dashboard", status_code=303)

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

# (opcional) endpoint para ver sessão
@router.get("/api/health")
async def health(request: Request):
    uid = request.session.get("user_id")
    return {"ok": True, "auth": bool(uid)}
