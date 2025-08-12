# routers/debug_auth.py — v1.0.0 (temporary, dev only)
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse

router = APIRouter()

@router.get("/debug/session")
async def debug_session(request: Request):
    data = {
        "cookie_header": request.headers.get("cookie"),
        "has_session_scope": "session" in request.scope,
        "session_keys": list(getattr(request, "session", {}).keys()) if "session" in request.scope else [],
        "session": dict(request.session) if "session" in request.scope else {},
    }
    return JSONResponse(data)

@router.post("/debug/set")
async def debug_set_session(request: Request):
    if "session" in request.scope:
        request.session["user_id"] = 999
        request.session["username"] = "debug"
    return RedirectResponse("/debug/session", status_code=303)

@router.get("/debug/ping")
async def ping():
    return {"ok": True}
