# security.py — v1.0.0 (11/08/2025)
import os, hashlib, hmac

_ALG = "pbkdf2_sha256"
_ITERS = 480000

def _pbkdf2(password: str, salt: bytes, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)

def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = _pbkdf2(password, salt, _ITERS)
    return f"{_ALG}${_ITERS}${salt.hex()}${dk.hex()}"

def verify_password(password: str, encoded: str) -> bool:
    try:
        alg, iters_s, salt_hex, dk_hex = encoded.split("$", 3)
        if alg != _ALG: return False
        iters = int(iters_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(dk_hex)
        test = _pbkdf2(password, salt, iters)
        return hmac.compare_digest(test, expected)
    except Exception:
        return False
