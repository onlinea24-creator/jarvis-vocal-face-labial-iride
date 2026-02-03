import os
import json
import time
import uuid
import hashlib
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import httpx
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

APP_VERSION = "BLOCCO06_ORCHESTRATOR_v1_0"

ROOT = Path(__file__).resolve().parents[1]
PROOFS_DIR = ROOT / "proofs"
SESSIONS_DIR = ROOT / "sessions"
UI_DIR = ROOT / "ui"

PROOFS_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# CONFIG (override via env)
# -------------------------
def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v and v.strip() else default

CHALLENGE_BASE_URL = _env("CHALLENGE_BASE_URL", "http://127.0.0.1:5012")
VOICE_BASE_URL = _env("VOICE_BASE_URL", "http://127.0.0.1:5011")
FACE_BASE_URL = _env("FACE_BASE_URL", "http://127.0.0.1:5011")  # override in env if different
LIPSYNC_BASE_URL = _env("LIPSYNC_BASE_URL", "http://127.0.0.1:5013")
VSR_BASE_URL = _env("VSR_BASE_URL", "http://127.0.0.1:5014")
FUSION_BASE_URL = _env("FUSION_BASE_URL", "http://127.0.0.1:5015")

CHALLENGE_START_PATH = _env("CHALLENGE_START_PATH", "/api/challenge/start")
CHALLENGE_VALIDATE_PATH = _env("CHALLENGE_VALIDATE_PATH", "/api/challenge/validate")

VOICE_VERIFY_PATH = _env("VOICE_VERIFY_PATH", "/api/voice/verify")
FACE_VERIFY_PATH = _env("FACE_VERIFY_PATH", "/api/face/verify")
LIPSYNC_VALIDATE_PATH = _env("LIPSYNC_VALIDATE_PATH", "/api/lipsync/validate")
VSR_VALIDATE_PATH = _env("VSR_VALIDATE_PATH", "/api/vsr/validate")
FUSION_EVALUATE_PATH = _env("FUSION_EVALUATE_PATH", "/api/fusion/evaluate")

HTTP_TIMEOUT_S = float(_env("HTTP_TIMEOUT_S", "25"))
HTTP_RETRIES = int(_env("HTTP_RETRIES", "1"))

MAX_VIDEO_MB = int(_env("MAX_VIDEO_MB", "50"))
MAX_AUDIO_MB = int(_env("MAX_AUDIO_MB", "15"))

# -------------------------
# APP
# -------------------------
app = FastAPI(title="BLOCCO 06 Orchestrator", version=APP_VERSION)

# Static UI
if (UI_DIR / "static").exists():
    app.mount("/static", StaticFiles(directory=str(UI_DIR / "static")), name="static")


# -------------------------
# HELPERS
# -------------------------
def _now_utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _new_session_id() -> str:
    return "SES-" + uuid.uuid4().hex[:16].upper()


def _new_proof_id() -> str:
    return "PROOF-" + uuid.uuid4().hex[:16].upper()


def _policy_to_mode(policy_id: str) -> str:
    if policy_id == "STRICT_STANDARD":
        return "SPOKEN"
    if policy_id == "STRICT_SILENT":
        return "SILENT"
    raise ValueError("INVALID_POLICY")


def _join(base: str, path: str) -> str:
    base = base.rstrip("/")
    path = path if path.startswith("/") else "/" + path
    return base + path


def _err(status: int, code: str, msg: str, flags=None):
    return JSONResponse(
        status_code=status,
        content={
            "ok": False,
            "error_code": code,
            "error_message": msg,
            "flags_summary": flags or [],
        },
    )


async def _post_json(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    last_exc = None
    for attempt in range(max(1, HTTP_RETRIES + 1)):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
                r = await client.post(url, json=payload, headers=headers)
                ct = r.headers.get("content-type", "")
                if r.status_code >= 400:
                    if "application/json" in ct:
                        return {"__http_error__": True, "__status__": r.status_code, "__json__": r.json()}
                    return {"__http_error__": True, "__status__": r.status_code, "__text__": r.text[:500]}
                return r.json() if "application/json" in ct else {"raw": r.text}
        except Exception as e:
            last_exc = e
            if attempt < HTTP_RETRIES:
                await asyncio.sleep(0.05)
    raise RuntimeError(f"HTTP_POST_JSON_FAILED: {url} :: {last_exc}")


async def _post_multipart(
    url: str,
    data: Dict[str, Any],
    files: Dict[str, Tuple[str, bytes, str]],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    last_exc = None
    for attempt in range(max(1, HTTP_RETRIES + 1)):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
                r = await client.post(url, data=data, files=files, headers=headers)
                ct = r.headers.get("content-type", "")
                if r.status_code >= 400:
                    if "application/json" in ct:
                        return {"__http_error__": True, "__status__": r.status_code, "__json__": r.json()}
                    return {"__http_error__": True, "__status__": r.status_code, "__text__": r.text[:500]}
                return r.json() if "application/json" in ct else {"raw": r.text}
        except Exception as e:
            last_exc = e
            if attempt < HTTP_RETRIES:
                await asyncio.sleep(0.05)
    raise RuntimeError(f"HTTP_POST_MULTIPART_FAILED: {url} :: {last_exc}")


# -------------------------
# ROUTES
# -------------------------
@app.get("/", response_class=RedirectResponse)
def root():
    return RedirectResponse(url="/ui")


@app.get("/health")
def health():
    return {
        "service": "orchestrator",
        "version": APP_VERSION,
        "time_utc": _now_utc_iso(),
        "limits": {"max_video_mb": MAX_VIDEO_MB, "max_audio_mb": MAX_AUDIO_MB},
        "http": {"timeout_s": HTTP_TIMEOUT_S, "retries": HTTP_RETRIES},
        "modules": {
            "challenge": {"base": CHALLENGE_BASE_URL, "start": CHALLENGE_START_PATH, "validate": CHALLENGE_VALIDATE_PATH},
            "voice": {"base": VOICE_BASE_URL, "verify": VOICE_VERIFY_PATH},
            "face": {"base": FACE_BASE_URL, "verify": FACE_VERIFY_PATH},
            "lipsync": {"base": LIPSYNC_BASE_URL, "validate": LIPSYNC_VALIDATE_PATH},
            "vsr": {"base": VSR_BASE_URL, "validate": VSR_VALIDATE_PATH},
            "fusion": {"base": FUSION_BASE_URL, "evaluate": FUSION_EVALUATE_PATH},
        },
    }


@app.get("/ui", response_class=HTMLResponse)
def ui():
    idx = UI_DIR / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8", errors="ignore")
    return "<html><body><h3>UI missing</h3></body></html>"


# UI calls same-origin /api/challenge/start. We proxy to CHALLENGE engine.
@app.post("/api/challenge/start")
async def challenge_start_proxy(payload: Dict[str, Any]):
    url = _join(CHALLENGE_BASE_URL, CHALLENGE_START_PATH)
    res = await _post_json(url, payload)
    if res.get("__http_error__"):
        j = res.get("__json__") or {}
        return _err(res.get("__status__", 500), "CHALLENGE_START_ERROR", "challenge start failed", flags=j.get("flags_summary") or [])
    return res


@app.post("/api/multimodal/verify")
async def multimodal_verify(
    policy_id: str = Form(...),
    enrollment_id_face: str = Form(...),
    enrollment_id_voice: Optional[str] = Form(None),
    challenge_id: str = Form(...),
    accept_face: float = Form(0.85),
    reject_face: float = Form(0.55),
    accept_voice: float = Form(0.85),
    reject_voice: float = Form(0.55),
    clip_video: UploadFile = File(...),
    clip_audio: Optional[UploadFile] = File(None),
):
    t0 = time.time()
    flags_summary = []

    # 1) Validate policy & required fields
    try:
        mode = _policy_to_mode(policy_id)
    except ValueError:
        return _err(400, "INVALID_POLICY", "policy_id not supported")

    if not enrollment_id_face:
        return _err(400, "MISSING_FIELD", "enrollment_id_face required")

    if policy_id == "STRICT_STANDARD" and not enrollment_id_voice:
        return _err(400, "MISSING_FIELD", "enrollment_id_voice required for STRICT_STANDARD")

    if policy_id == "STRICT_STANDARD" and clip_audio is None:
        return _err(400, "MISSING_FIELD", "clip_audio required for STRICT_STANDARD")

    if policy_id == "STRICT_SILENT" and clip_audio is not None:
        flags_summary.append("AUDIO_IGNORED_SILENT")

    # 2) Read + size guard (demo safety)
    video_bytes = await clip_video.read()
    if len(video_bytes) > MAX_VIDEO_MB * 1024 * 1024:
        return _err(413, "VIDEO_TOO_LARGE", f"clip_video exceeds {MAX_VIDEO_MB}MB")

    audio_bytes = None
    if policy_id == "STRICT_STANDARD" and clip_audio is not None:
        audio_bytes = await clip_audio.read()
        if len(audio_bytes) > MAX_AUDIO_MB * 1024 * 1024:
            return _err(413, "AUDIO_TOO_LARGE", f"clip_audio exceeds {MAX_AUDIO_MB}MB")

    # 3) Persist raw media (session)
    session_id = _new_session_id()
    ses_dir = SESSIONS_DIR / session_id / "raw"
    ses_dir.mkdir(parents=True, exist_ok=True)

    video_ext = Path(clip_video.filename or "clip_video.bin").suffix or ".bin"
    video_path = ses_dir / f"clip_video{video_ext}"
    video_path.write_bytes(video_bytes)

    audio_path = None
    if policy_id == "STRICT_STANDARD" and audio_bytes is not None:
        audio_ext = Path((clip_audio.filename if clip_audio else "clip_audio.bin") or "clip_audio.bin").suffix or ".bin"
        audio_path = ses_dir / f"clip_audio{audio_ext}"
        audio_path.write_bytes(audio_bytes)

    metadata = {
        "session_id": session_id,
        "time_utc": _now_utc_iso(),
        "policy_id": policy_id,
        "mode": mode,
        "challenge_id": challenge_id,
        "enrollment_id_face": enrollment_id_face,
        "enrollment_id_voice": enrollment_id_voice,
        "input_files": {
            "clip_video": str(video_path.name),
            "clip_audio": str(audio_path.name) if audio_path else None,
        },
    }
    (ses_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    timings = {"total": 0, "challenge": 0, "voice": 0, "face": 0, "lipsync": 0, "vsr": 0, "fusion": 0}

    headers = {"X-Veritas-Session": session_id}

    # 4) Challenge validate (BLOCCO 02)
    t_ch0 = time.time()
    ch_url = _join(CHALLENGE_BASE_URL, CHALLENGE_VALIDATE_PATH)
    ch_files = {
        "clip_video": (video_path.name, video_bytes, clip_video.content_type or "application/octet-stream"),
    }
    ch_data = {"challenge_id": challenge_id, "mode": mode}
    if policy_id == "STRICT_STANDARD" and audio_bytes is not None and audio_path is not None:
        ch_files["clip_audio"] = (audio_path.name, audio_bytes, clip_audio.content_type or "application/octet-stream")

    ch_res = await _post_multipart(ch_url, data=ch_data, files=ch_files, headers=headers)
    timings["challenge"] = int((time.time() - t_ch0) * 1000)

    if ch_res.get("__http_error__"):
        j = ch_res.get("__json__") or {}
        return _err(ch_res.get("__status__", 500), "CHALLENGE_ERROR", "challenge validate failed", flags=j.get("flags_summary") or [])

    challenge_score = ch_res.get("score_challenge")
    challenge_decision = ch_res.get("decision_challenge")
    flags_summary += (ch_res.get("flags_challenge") or ch_res.get("flags") or [])

    # 5) Modules
    voice_score = None
    face_score = None
    lipsync_score = None
    vsr_score = None

    # 5A) Face verify
    t_f0 = time.time()
    face_url = _join(FACE_BASE_URL, FACE_VERIFY_PATH)
    face_res = await _post_multipart(
        face_url,
        data={"enrollment_id_face": enrollment_id_face},
        files={"clip_video": (video_path.name, video_bytes, clip_video.content_type or "application/octet-stream")},
        headers=headers,
    )
    timings["face"] = int((time.time() - t_f0) * 1000)

    if face_res.get("__http_error__"):
        j = face_res.get("__json__") or {}
        return _err(face_res.get("__status__", 500), "FACE_ERROR", "face verify failed", flags=j.get("flags_face") or j.get("flags_summary") or [])

    face_score = face_res.get("score_face_match") or face_res.get("face_score")
    flags_summary += (face_res.get("flags_face") or face_res.get("flags") or [])

    # 5B) Voice verify + Lipsync (STRICT_STANDARD)
    if policy_id == "STRICT_STANDARD" and audio_bytes is not None and audio_path is not None:
        t_v0 = time.time()
        voice_url = _join(VOICE_BASE_URL, VOICE_VERIFY_PATH)
        voice_res = await _post_multipart(
            voice_url,
            data={"enrollment_id_voice": enrollment_id_voice},
            files={"clip_audio": (audio_path.name, audio_bytes, clip_audio.content_type or "application/octet-stream")},
            headers=headers,
        )
        timings["voice"] = int((time.time() - t_v0) * 1000)

        if voice_res.get("__http_error__"):
            j = voice_res.get("__json__") or {}
            return _err(voice_res.get("__status__", 500), "VOICE_ERROR", "voice verify failed", flags=j.get("flags_voice") or j.get("flags_summary") or [])

        voice_score = voice_res.get("score_voice_match") or voice_res.get("voice_score")
        flags_summary += (voice_res.get("flags_voice") or voice_res.get("flags") or [])

        t_l0 = time.time()
        lipsync_url = _join(LIPSYNC_BASE_URL, LIPSYNC_VALIDATE_PATH)
        lipsync_res = await _post_multipart(
            lipsync_url,
            data={"challenge_id": challenge_id},
            files={
                "clip_video": (video_path.name, video_bytes, clip_video.content_type or "application/octet-stream"),
                "clip_audio": (audio_path.name, audio_bytes, clip_audio.content_type or "application/octet-stream"),
            },
            headers=headers,
        )
        timings["lipsync"] = int((time.time() - t_l0) * 1000)

        if lipsync_res.get("__http_error__"):
            j = lipsync_res.get("__json__") or {}
            return _err(lipsync_res.get("__status__", 500), "LIPSYNC_ERROR", "lipsync failed", flags=j.get("flags_lipsync") or j.get("flags_summary") or [])

        lipsync_score = lipsync_res.get("score_lipsync") or lipsync_res.get("lipsync_score")
        flags_summary += (lipsync_res.get("flags_lipsync") or lipsync_res.get("flags") or [])

    # 5C) VSR (STRICT_SILENT)
    if policy_id == "STRICT_SILENT":
        t_s0 = time.time()
        vsr_url = _join(VSR_BASE_URL, VSR_VALIDATE_PATH)
        vsr_res = await _post_multipart(
            vsr_url,
            data={"challenge_id": challenge_id},
            files={"clip_video": (video_path.name, video_bytes, clip_video.content_type or "application/octet-stream")},
            headers=headers,
        )
        timings["vsr"] = int((time.time() - t_s0) * 1000)

        if vsr_res.get("__http_error__"):
            j = vsr_res.get("__json__") or {}
            return _err(vsr_res.get("__status__", 500), "VSR_ERROR", "vsr failed", flags=j.get("flags_vsr") or j.get("flags_summary") or [])

        vsr_score = vsr_res.get("score_vsr") or vsr_res.get("vsr_score")
        flags_summary += (vsr_res.get("flags_vsr") or vsr_res.get("flags") or [])

    # 6) Fusion evaluate
    t_fu0 = time.time()
    fusion_url = _join(FUSION_BASE_URL, FUSION_EVALUATE_PATH)
    fusion_payload = {
        "policy_id": policy_id,
        "thresholds": {
            "accept_face": accept_face,
            "reject_face": reject_face,
            "accept_voice": accept_voice,
            "reject_voice": reject_voice,
        },
        "inputs": {
            "challenge": {"score": challenge_score, "decision": challenge_decision},
            "face": {"score": face_score},
            "voice": {"score": voice_score},
            "lipsync": {"score": lipsync_score},
            "vsr": {"score": vsr_score},
        },
    }

    fusion_res = await _post_json(fusion_url, fusion_payload, headers=headers)
    timings["fusion"] = int((time.time() - t_fu0) * 1000)

    if fusion_res.get("__http_error__"):
        j = fusion_res.get("__json__") or {}
        return _err(fusion_res.get("__status__", 500), "FUSION_ERROR", "fusion evaluate failed", flags=j.get("flags_summary") or [])

    final_decision = fusion_res.get("final_decision") or fusion_res.get("decision") or "INCONCLUSIVE"
    flags_summary += (fusion_res.get("flags_summary") or fusion_res.get("flags") or [])
    flags_summary = list(dict.fromkeys(flags_summary))[:20]  # unique preserve order

    # 7) Proof + evidence hashes
    proof_id = _new_proof_id()
    proof_path = PROOFS_DIR / f"{proof_id}.json"

    sha_video = _sha256_file(video_path)
    sha_audio = _sha256_file(audio_path) if audio_path else None

    proof_obj = {
        "proof_version": "proof_multimodal_v1",
        "proof_id": proof_id,
        "time_utc": _now_utc_iso(),
        "policy_id": policy_id,
        "mode": mode,
        "session_id": session_id,
        "inputs": {
            "enrollment_id_face": enrollment_id_face,
            "enrollment_id_voice": enrollment_id_voice,
            "challenge_id": challenge_id,
        },
        "results": {
            "final_decision": final_decision,
            "breakdown": {
                "voice_score": voice_score,
                "face_score": face_score,
                "lipsync_score": lipsync_score,
                "challenge_score": challenge_score,
                "vsr_score": vsr_score,
            },
            "flags_summary": flags_summary,
        },
        "evidence_refs": {
            "video": {"path": str(video_path), "sha256": sha_video},
            "audio": {"path": str(audio_path), "sha256": sha_audio} if audio_path else None,
        },
        "module_refs": {
            "challenge": {"base_url": CHALLENGE_BASE_URL, "start_path": CHALLENGE_START_PATH, "validate_path": CHALLENGE_VALIDATE_PATH},
            "voice": {"base_url": VOICE_BASE_URL, "verify_path": VOICE_VERIFY_PATH},
            "face": {"base_url": FACE_BASE_URL, "verify_path": FACE_VERIFY_PATH},
            "lipsync": {"base_url": LIPSYNC_BASE_URL, "validate_path": LIPSYNC_VALIDATE_PATH},
            "vsr": {"base_url": VSR_BASE_URL, "validate_path": VSR_VALIDATE_PATH},
            "fusion": {"base_url": FUSION_BASE_URL, "evaluate_path": FUSION_EVALUATE_PATH},
        },
        "timings_ms": timings,
    }

    proof_path.write_text(json.dumps(proof_obj, indent=2), encoding="utf-8")

    sha_proof = _sha256_file(proof_path)
    (ses_dir / "sha256.json").write_text(
        json.dumps(
            {"clip_video_sha256": sha_video, "clip_audio_sha256": sha_audio, "proof_sha256": sha_proof},
            indent=2,
        ),
        encoding="utf-8",
    )

    timings["total"] = int((time.time() - t0) * 1000)

    return {
        "ok": True,
        "final_decision": final_decision,
        "proof_id": proof_id,
        "proof_file": str(proof_path),
        "breakdown": {
            "voice_score": voice_score,
            "face_score": face_score,
            "lipsync_score": lipsync_score,
            "challenge_score": challenge_score,
            "vsr_score": vsr_score,
        },
        "flags_summary": flags_summary,
        "timings_ms": timings,
    }
