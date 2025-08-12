 # auth_middleware.py v1.1.2 (2025-08-11)
# - Fixed: safe ASCII header to avoid copy/paste issues.
# - Behavior: whitelist items ending with '/' are treated as prefixes (e.g., '/static/').
# - No '/' in whitelist by default. Checks whitelist BEFORE touching session.
# - Returns 401 for API requests, 303 redirect to /login for HTML (with ?next=).

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, JSONResponse

DEFAULT_WHITELIST = (
    "/login",
    "/logout",
    "/favicon.ico",
    "/healthz",
    "/api/health",
    "/static/",
)

class AuthRequiredMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        whitelist: tuple[str, ...] = DEFAULT_WHITELIST,
        api_prefix: str = "/api",
        login_path: str = "/login",
    ):
        super().__init__(app)
        self.login_path = login_path
        self.api_prefix = api_prefix

        exact = []
        prefixes = []
        for w in whitelist:
            if w.endswith("/") and w != "/":
                prefixes.append(w)
            else:
                exact.append(w)
        self.exact_whitelist = tuple(exact)
        self.prefix_whitelist = tuple(prefixes)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path or "/"

        # Allow preflight/HEAD without session check
        if request.method in ("OPTIONS", "HEAD"):
            return await call_next(request)

        # 1) Whitelist first (exact matches and prefix matches)
        if self._is_whitelisted(path):
            return await call_next(request)

        # 2) Guard: avoid 500 if SessionMiddleware is missing
        if "session" not in request.scope:
            return self._unauthorized_response(request)

        # 3) Authenticated?
        if request.session.get("user_id"):
            return await call_next(request)

        # 4) Not authenticated
        return self._unauthorized_response(request)

    def _is_whitelisted(self, path: str) -> bool:
        if path in self.exact_whitelist:
            return True
        for p in self.prefix_whitelist:
            if path.startswith(p):
                return True
        return False

    def _unauthorized_response(self, request: Request):
        accepts = (request.headers.get("accept") or "").lower()
        wants_json = request.url.path.startswith(self.api_prefix) or "application/json" in accepts

        if wants_json:
            return JSONResponse({"detail": "unauthorized"}, status_code=401)

        # Build next= with preserved query string
        next_url = request.url.path
        if request.url.query:
            next_url += "?" + request.url.query

        # 303 See Other avoids resubmitting POST
        return RedirectResponse(url=f"{self.login_path}?next={next_url}", status_code=303)

