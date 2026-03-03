import asyncio
import atexit
import hashlib
import json
import logging
import os
import secrets
import shutil
import threading
import traceback
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from core.tasks import runTasks
from utils.logger import setup_logger


logger = setup_logger(level=logging.DEBUG)

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
ROOT_CONFIG_PATH = BASE_DIR / "config.json"
DATA_DIR = BASE_DIR / "data"
TENANTS_DIR = DATA_DIR / "tenants"
USERS_META_PATH = DATA_DIR / "users.json"
SESSION_COOKIE_NAME = "sparkflow_auth"
DEFAULT_TIMEZONE = "Asia/Shanghai"
MAX_LOG_LINES = 1200
MAX_TEMPLATE_LENGTH = 2000
PASSWORD_ITERATIONS = 210000

DEFAULT_USER_CONFIG = {
    "multiTask": True,
    "taskCount": 5,
    "proxyAddress": "",
    "messageTemplate": "[续火花]",
    "hitokotoTypes": ["文学", "影视", "诗词", "哲学"],
    "scheduler": {
        "enabled": True,
        "timezone": DEFAULT_TIMEZONE,
        "hour": 9,
        "minute": 0,
        "runOnStartup": False,
    },
}

AUTH_SESSIONS: dict[str, dict[str, str]] = {}
data_file_lock = threading.Lock()
scheduler_lock = threading.Lock()
runtime_map_lock = threading.Lock()


class UserRuntimeState:
    def __init__(self, username: str):
        self.username = username
        self._run_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self.is_running = False
        self.last_status = "未开始"
        self.last_error = ""
        self.last_trigger = "-"
        self.last_start = None
        self.last_end = None
        self.next_run = None
        self.schedule_hour = 9
        self.schedule_minute = 0
        self.schedule_timezone = DEFAULT_TIMEZONE
        self.history = deque(maxlen=50)
        self.logs = deque(maxlen=2000)

    def _format_ts(self, value: Optional[datetime]):
        if not value:
            return "-"
        return value.strftime("%Y-%m-%d %H:%M:%S")

    def schedule_time(self):
        return f"{self.schedule_hour:02d}:{self.schedule_minute:02d}"

    def _set_running(self, value: bool):
        with self._state_lock:
            self.is_running = value

    def add_log(self, message: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._state_lock:
            self.logs.append(f"{ts} [{self.username}] {message}")

    def update_schedule(self, hour: int, minute: int, timezone: str):
        with self._state_lock:
            self.schedule_hour = hour
            self.schedule_minute = minute
            self.schedule_timezone = timezone

    def update_next_run(self, next_run):
        with self._state_lock:
            self.next_run = next_run

    def snapshot(self, account_count: int, target_count: int):
        with self._state_lock:
            return {
                "is_running": self.is_running,
                "last_status": self.last_status,
                "last_error": self.last_error,
                "last_trigger": self.last_trigger,
                "last_start": self._format_ts(self.last_start),
                "last_end": self._format_ts(self.last_end),
                "next_run": self._format_ts(self.next_run),
                "account_count": account_count,
                "target_count": target_count,
                "schedule_time": self.schedule_time(),
                "schedule_timezone": self.schedule_timezone,
            }

    def history_rows(self):
        with self._state_lock:
            return list(self.history)[::-1]

    def recent_logs(self, limit=MAX_LOG_LINES):
        with self._state_lock:
            lines = list(self.logs)[-max(1, limit):]
        return "\n".join(lines) if lines else "暂无日志。"

    def run_once(self, trigger: str):
        if not self._run_lock.acquire(blocking=False):
            self.add_log(f"任务已在运行中，忽略触发：{trigger}")
            return False, "已有任务在运行，本次触发已跳过。"

        self._set_running(True)
        with self._state_lock:
            self.last_trigger = trigger
            self.last_start = datetime.now()
            self.last_end = None
            self.last_error = ""
            self.last_status = "运行中"
        self.add_log(f"任务开始执行，触发方式：{trigger}")

        ok = True
        message = "任务执行完成。"
        try:
            asyncio.run(_run_user_tasks(self.username))
            with self._state_lock:
                self.last_status = "成功"
        except Exception as exc:
            ok = False
            message = f"任务执行失败：{exc}"
            with self._state_lock:
                self.last_status = "失败"
                self.last_error = repr(exc)
            self.add_log(f"任务失败：{exc}")
            logger.error("Task failed. user=%s trigger=%s error=%s", self.username, trigger, exc)
            logger.debug("Task traceback:\n%s", traceback.format_exc())
        finally:
            end_at = datetime.now()
            with self._state_lock:
                self.last_end = end_at
                duration = (self.last_end - self.last_start).total_seconds()
                self.history.append(
                    {
                        "trigger": trigger,
                        "start": self._format_ts(self.last_start),
                        "end": self._format_ts(self.last_end),
                        "status": self.last_status,
                        "duration": f"{duration:.2f}s",
                        "message": self.last_error or "OK",
                    }
                )
                current_status = self.last_status
            self.add_log(f"任务结束，状态={current_status}，耗时={duration:.2f}s")
            self._set_running(False)
            self._run_lock.release()
        return ok, message


runtime_map: dict[str, UserRuntimeState] = {}
scheduler = None


class UserLoginPayload(BaseModel):
    username: str
    password: str


class AdminLoginPayload(BaseModel):
    password: str


class SchedulePayload(BaseModel):
    time: str


class MessageTemplatePayload(BaseModel):
    message: str


class UserTargetsItem(BaseModel):
    unique_id: str
    targets: list[str] = Field(default_factory=list)


class UserTargetsPayload(BaseModel):
    users: list[UserTargetsItem]


def _ensure_data_layout():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TENANTS_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_META_PATH.exists():
        _save_json(USERS_META_PATH, {"users": []})


def _load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, payload):
    with data_file_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


def _safe_slug(text: str):
    allowed = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_"):
            allowed.append(ch)
        else:
            allowed.append("_")
    slug = "".join(allowed).strip("_")
    return slug or "user"


def _hash_password(password: str, salt_hex: Optional[str] = None):
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_ITERATIONS,
    )
    return {
        "salt": salt.hex(),
        "hash": digest.hex(),
    }


def _verify_password(password: str, salt_hex: str, expected_hash: str):
    data = _hash_password(password, salt_hex=salt_hex)
    return secrets.compare_digest(data["hash"], expected_hash)


def _load_users_meta():
    _ensure_data_layout()
    raw = _load_json(USERS_META_PATH, {"users": []})
    users = raw.get("users", []) if isinstance(raw, dict) else []
    result = {}
    for item in users:
        username = str(item.get("username", "")).strip()
        if username:
            result[username] = item
    return result


def _save_users_meta(users_map: dict[str, dict[str, Any]]):
    payload = {"users": sorted(users_map.values(), key=lambda x: x.get("username", ""))}
    _save_json(USERS_META_PATH, payload)


def _get_user_meta_or_404(username: str):
    users_map = _load_users_meta()
    user = users_map.get(username)
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    return user


def _get_tenant_dir(user_meta: dict[str, Any]):
    tenant_rel = user_meta.get("tenant_dir", "")
    if not tenant_rel:
        raise RuntimeError(f"用户 {user_meta.get('username')} 缺少 tenant_dir")
    return (BASE_DIR / tenant_rel).resolve()


def _get_user_config_path(username: str):
    user_meta = _get_user_meta_or_404(username)
    return _get_tenant_dir(user_meta) / "config.json"


def _get_user_data_path(username: str):
    user_meta = _get_user_meta_or_404(username)
    return _get_tenant_dir(user_meta) / "usersData.json"


def _get_default_user_config():
    if ROOT_CONFIG_PATH.exists():
        try:
            return _load_json(ROOT_CONFIG_PATH, DEFAULT_USER_CONFIG)
        except Exception:
            logger.warning("Failed to read root config.json. fallback to DEFAULT_USER_CONFIG")
    return json.loads(json.dumps(DEFAULT_USER_CONFIG, ensure_ascii=False))


def _load_user_config(username: str):
    path = _get_user_config_path(username)
    if not path.exists():
        cfg = _get_default_user_config()
        _save_json(path, cfg)
        return cfg
    return _load_json(path, _get_default_user_config())


def _save_user_config(username: str, cfg: dict):
    path = _get_user_config_path(username)
    _save_json(path, cfg)


def _load_user_users_data(username: str):
    path = _get_user_data_path(username)
    if not path.exists():
        raise FileNotFoundError(f"用户 {username} 的 usersData.json 不存在")
    data = _load_json(path, [])
    if not isinstance(data, list):
        raise ValueError("usersData.json 必须是数组")
    return data


def _save_user_users_data(username: str, users_data: list):
    path = _get_user_data_path(username)
    _save_json(path, users_data)


def _sanitize_targets(values):
    cleaned = []
    seen = set()
    for value in values or []:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def _validate_and_normalize_users_data(raw_bytes: bytes):
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"上传文件不是合法 JSON：{exc}")

    if not isinstance(payload, list) or not payload:
        raise ValueError("usersData.json 必须是非空数组")

    normalized = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"第 {idx + 1} 条用户数据格式错误（必须是对象）")

        unique_id = str(item.get("unique_id", "")).strip()
        username = str(item.get("username", "")).strip()
        cookies = item.get("cookies", [])
        targets = item.get("targets", [])

        if not unique_id:
            raise ValueError(f"第 {idx + 1} 条缺少 unique_id")
        if not username:
            raise ValueError(f"第 {idx + 1} 条缺少 username")
        if not isinstance(cookies, list) or not cookies:
            raise ValueError(f"第 {idx + 1} 条 cookies 不能为空且必须是数组")
        if not isinstance(targets, list):
            raise ValueError(f"第 {idx + 1} 条 targets 必须是数组")

        normalized.append(
            {
                "unique_id": unique_id,
                "username": username,
                "cookies": cookies,
                "targets": _sanitize_targets(targets),
            }
        )

    primary_username = normalized[0]["username"]
    primary_unique_id = normalized[0]["unique_id"]
    return normalized, primary_username, primary_unique_id


def _count_targets(users_data: list):
    return sum(len(user.get("targets", [])) for user in users_data)


def _get_runtime(username: str):
    with runtime_map_lock:
        runtime = runtime_map.get(username)
        if runtime is None:
            runtime = UserRuntimeState(username=username)
            runtime_map[username] = runtime
        return runtime


def _delete_runtime(username: str):
    with runtime_map_lock:
        runtime_map.pop(username, None)


def _session_from_request(request: Request):
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    return AUTH_SESSIONS.get(token)


def _require_user_session(request: Request):
    session = _session_from_request(request)
    if not session or session.get("role") != "user":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未登录或登录已失效",
        )
    return session


def _require_admin_session(request: Request):
    session = _session_from_request(request)
    if not session or session.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未登录或登录已失效",
        )
    return session


def _parse_time_string(value: str):
    parts = value.strip().split(":")
    if len(parts) not in (2, 3):
        raise ValueError("时间格式错误，必须是 HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("时间范围错误，小时 0-23，分钟 0-59")
    return hour, minute


def _build_editor_state(username: str):
    cfg = _load_user_config(username)
    users = _load_user_users_data(username)
    return {
        "message_template": str(cfg.get("messageTemplate", "")),
        "users": [
            {
                "unique_id": str(user.get("unique_id", "")),
                "username": str(user.get("username", "未知用户")),
                "targets": _sanitize_targets(user.get("targets", [])),
            }
            for user in users
        ],
    }


def _scheduler_job_id(username: str):
    return f"daily_task::{username}"


def _run_scheduled_once(username: str):
    runtime = _get_runtime(username)
    runtime.run_once("schedule")
    if scheduler:
        job = scheduler.get_job(_scheduler_job_id(username))
        runtime.update_next_run(job.next_run_time if job else None)


async def _run_user_tasks(username: str):
    cfg = _load_user_config(username)
    users_data = _load_user_users_data(username)
    await runTasks(config=cfg, userData=users_data)


def _schedule_user_job(username: str):
    global scheduler

    cfg = _load_user_config(username)
    scheduler_cfg = cfg.get("scheduler", {}) if isinstance(cfg, dict) else {}
    enabled = bool(scheduler_cfg.get("enabled", True))
    timezone = str(scheduler_cfg.get("timezone", DEFAULT_TIMEZONE))
    hour = int(scheduler_cfg.get("hour", 9))
    minute = int(scheduler_cfg.get("minute", 0))

    runtime = _get_runtime(username)
    runtime.update_schedule(hour, minute, timezone)

    with scheduler_lock:
        if scheduler is None:
            scheduler = BackgroundScheduler(timezone=timezone)
            scheduler.start()

        job_id = _scheduler_job_id(username)
        if not enabled:
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
            runtime.update_next_run(None)
            runtime.add_log("定时任务已禁用")
            return

        scheduler.add_job(
            _run_scheduled_once,
            args=[username],
            trigger=CronTrigger(hour=hour, minute=minute, timezone=timezone),
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        job = scheduler.get_job(job_id)
        runtime.update_next_run(job.next_run_time if job else None)
        runtime.add_log(f"定时任务更新为 {hour:02d}:{minute:02d} ({timezone})")


def _remove_user_schedule_job(username: str):
    with scheduler_lock:
        if scheduler is None:
            return
        job_id = _scheduler_job_id(username)
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)


def _start_background_run(username: str, trigger: str):
    runtime = _get_runtime(username)

    def _worker():
        runtime.run_once(trigger)
        if scheduler:
            job = scheduler.get_job(_scheduler_job_id(username))
            runtime.update_next_run(job.next_run_time if job else None)

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    return True


def _start_scheduler():
    global scheduler
    _ensure_data_layout()
    with scheduler_lock:
        if scheduler is None:
            scheduler = BackgroundScheduler(timezone=DEFAULT_TIMEZONE)
            scheduler.start()

    users_map = _load_users_meta()
    for username in users_map.keys():
        _schedule_user_job(username)
        cfg = _load_user_config(username)
        run_on_startup = bool(cfg.get("scheduler", {}).get("runOnStartup", False))
        if run_on_startup:
            _start_background_run(username, "startup")


def _stop_scheduler():
    global scheduler
    with scheduler_lock:
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped.")
        scheduler = None


app = FastAPI(title="DouYin Spark Flow Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.on_event("startup")
async def on_startup():
    _ensure_data_layout()
    _start_scheduler()
    atexit.register(_stop_scheduler)


@app.on_event("shutdown")
async def on_shutdown():
    _stop_scheduler()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    session = _session_from_request(request)
    if not session:
        return RedirectResponse(url="/login", status_code=303)
    if session.get("role") == "admin":
        return RedirectResponse(url="/admin", status_code=303)

    username = session.get("username")
    runtime = _get_runtime(username)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "default_time": runtime.schedule_time(),
            "username": username,
        },
    )


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    session = _session_from_request(request)
    if session:
        if session.get("role") == "admin":
            return RedirectResponse(url="/admin", status_code=303)
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    session = _session_from_request(request)
    if session:
        if session.get("role") == "admin":
            return RedirectResponse(url="/admin", status_code=303)
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse("register.html", {"request": request})


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    session = _session_from_request(request)
    if not session or session.get("role") != "admin":
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "password_missing": not bool(os.getenv("PASSWORD")),
            },
        )
    return templates.TemplateResponse("admin.html", {"request": request})


@app.post("/api/login")
async def api_login(payload: UserLoginPayload):
    username = payload.username.strip()
    if not username:
        return JSONResponse(status_code=400, content={"ok": False, "message": "用户名不能为空。"})

    users_map = _load_users_meta()
    user = users_map.get(username)
    if not user:
        return JSONResponse(status_code=401, content={"ok": False, "message": "用户名或密码错误。"})

    if not _verify_password(payload.password, user.get("password_salt", ""), user.get("password_hash", "")):
        return JSONResponse(status_code=401, content={"ok": False, "message": "用户名或密码错误。"})

    token = secrets.token_urlsafe(32)
    AUTH_SESSIONS[token] = {"role": "user", "username": username}

    response = JSONResponse({"ok": True, "message": "登录成功。"})
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=7 * 24 * 3600,
    )
    return response


@app.post("/api/admin/login")
async def api_admin_login(payload: AdminLoginPayload):
    expected_password = os.getenv("PASSWORD")
    if not expected_password:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": "服务端未配置 PASSWORD 环境变量。"},
        )

    if payload.password != expected_password:
        return JSONResponse(status_code=401, content={"ok": False, "message": "密码错误。"})

    token = secrets.token_urlsafe(32)
    AUTH_SESSIONS[token] = {"role": "admin", "username": "admin"}
    response = JSONResponse({"ok": True, "message": "登录成功。"})
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=7 * 24 * 3600,
    )
    return response


@app.post("/api/register")
async def api_register(password: str = Form(...), users_file: UploadFile = File(...)):
    if len(password.strip()) < 4:
        return JSONResponse(status_code=400, content={"ok": False, "message": "密码至少 4 位。"})

    if not users_file.filename.lower().endswith(".json"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "请上传 usersData.json 文件。"})

    try:
        raw = await users_file.read()
        users_data, username, unique_id = _validate_and_normalize_users_data(raw)
    except Exception as exc:
        return JSONResponse(status_code=400, content={"ok": False, "message": str(exc)})

    users_map = _load_users_meta()
    if username in users_map:
        return JSONResponse(status_code=409, content={"ok": False, "message": f"用户名 {username} 已注册。"})

    for existing in users_map.values():
        if str(existing.get("unique_id", "")).strip() == unique_id:
            return JSONResponse(status_code=409, content={"ok": False, "message": f"unique_id {unique_id} 已注册。"})

    tenant_slug = _safe_slug(f"{username}_{unique_id}_{secrets.token_hex(3)}")
    tenant_dir = TENANTS_DIR / tenant_slug
    tenant_dir.mkdir(parents=True, exist_ok=True)

    _save_json(tenant_dir / "usersData.json", users_data)
    default_config = _get_default_user_config()
    default_config.setdefault("scheduler", {})
    default_config["scheduler"].setdefault("enabled", True)
    default_config["scheduler"].setdefault("timezone", DEFAULT_TIMEZONE)
    default_config["scheduler"].setdefault("hour", 9)
    default_config["scheduler"].setdefault("minute", 0)
    default_config["scheduler"].setdefault("runOnStartup", False)
    _save_json(tenant_dir / "config.json", default_config)

    hash_data = _hash_password(password.strip())
    users_map[username] = {
        "username": username,
        "unique_id": unique_id,
        "password_hash": hash_data["hash"],
        "password_salt": hash_data["salt"],
        "tenant_dir": str(tenant_dir.relative_to(BASE_DIR)).replace("\\", "/"),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _save_users_meta(users_map)

    _schedule_user_job(username)
    _get_runtime(username).add_log("用户已注册并完成定时任务初始化")

    return {
        "ok": True,
        "message": "注册成功，请使用用户名和密码登录。",
        "username": username,
    }


@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        AUTH_SESSIONS.pop(token, None)
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@app.get("/api/status")
async def api_status(request: Request):
    session = _require_user_session(request)
    username = session["username"]
    runtime = _get_runtime(username)
    users_data = _load_user_users_data(username)
    return {
        "ok": True,
        "runtime": runtime.snapshot(
            account_count=len(users_data),
            target_count=_count_targets(users_data),
        ),
        "history": runtime.history_rows(),
    }


@app.get("/api/logs")
async def api_logs(request: Request, limit: int = MAX_LOG_LINES):
    session = _require_user_session(request)
    username = session["username"]
    runtime = _get_runtime(username)
    limit = min(max(100, limit), 3000)
    return {"ok": True, "logs": runtime.recent_logs(limit=limit)}


@app.post("/api/run")
async def api_run(request: Request):
    session = _require_user_session(request)
    username = session["username"]
    runtime = _get_runtime(username)

    if runtime.is_running:
        return JSONResponse(
            status_code=409,
            content={"ok": False, "message": "已有任务正在执行，请稍后再试。"},
        )

    _start_background_run(username, "manual")
    return {"ok": True, "message": "任务已开始执行。"}


@app.post("/api/schedule")
async def api_schedule(request: Request, payload: SchedulePayload):
    session = _require_user_session(request)
    username = session["username"]

    try:
        hour, minute = _parse_time_string(payload.time)
    except Exception as exc:
        return JSONResponse(status_code=400, content={"ok": False, "message": str(exc)})

    cfg = _load_user_config(username)
    scheduler_cfg = cfg.setdefault("scheduler", {})
    scheduler_cfg["enabled"] = True
    scheduler_cfg["hour"] = hour
    scheduler_cfg["minute"] = minute
    scheduler_cfg["timezone"] = str(scheduler_cfg.get("timezone", DEFAULT_TIMEZONE))
    scheduler_cfg["runOnStartup"] = bool(scheduler_cfg.get("runOnStartup", False))
    _save_user_config(username, cfg)

    _schedule_user_job(username)
    runtime = _get_runtime(username)
    return {
        "ok": True,
        "message": f"定时任务已更新为每天 {hour:02d}:{minute:02d}。",
        "time": f"{hour:02d}:{minute:02d}",
        "next_run": runtime.snapshot(0, 0)["next_run"],
    }


@app.get("/api/editor/state")
async def api_editor_state(request: Request):
    session = _require_user_session(request)
    username = session["username"]
    return {"ok": True, **_build_editor_state(username)}


@app.post("/api/editor/message")
async def api_editor_message(request: Request, payload: MessageTemplatePayload):
    session = _require_user_session(request)
    username = session["username"]

    message = payload.message.strip()
    if not message:
        return JSONResponse(status_code=400, content={"ok": False, "message": "消息内容不能为空。"})
    if len(message) > MAX_TEMPLATE_LENGTH:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "message": f"消息内容过长，最多 {MAX_TEMPLATE_LENGTH} 字符。"},
        )

    cfg = _load_user_config(username)
    cfg["messageTemplate"] = message
    _save_user_config(username, cfg)
    _get_runtime(username).add_log("消息模板已更新")
    return {"ok": True, "message": "消息模板已保存。"}


@app.post("/api/editor/targets")
async def api_editor_targets(request: Request, payload: UserTargetsPayload):
    session = _require_user_session(request)
    username = session["username"]

    users_data = _load_user_users_data(username)
    updates = {item.unique_id: _sanitize_targets(item.targets) for item in payload.users}

    updated = 0
    for user in users_data:
        uid = str(user.get("unique_id", ""))
        if uid in updates:
            user["targets"] = updates[uid]
            updated += 1

    _save_user_users_data(username, users_data)
    _get_runtime(username).add_log(f"目标好友已更新，涉及账号数：{updated}")
    return {"ok": True, "message": f"目标好友已保存（{updated} 个账号）。"}


@app.get("/api/admin/overview")
async def api_admin_overview(request: Request):
    _require_admin_session(request)
    users_map = _load_users_meta()

    rows = []
    for username, meta in sorted(users_map.items(), key=lambda x: x[0]):
        try:
            cfg = _load_user_config(username)
            users_data = _load_user_users_data(username)
        except Exception as exc:
            rows.append(
                {
                    "username": username,
                    "unique_id": meta.get("unique_id", ""),
                    "created_at": meta.get("created_at", "-"),
                    "error": str(exc),
                }
            )
            continue

        scheduler_cfg = cfg.get("scheduler", {})
        runtime = _get_runtime(username)
        runtime_snapshot = runtime.snapshot(
            account_count=len(users_data),
            target_count=_count_targets(users_data),
        )

        receivers = []
        for item in users_data:
            receivers.extend(item.get("targets", []))

        rows.append(
            {
                "username": username,
                "unique_id": meta.get("unique_id", ""),
                "created_at": meta.get("created_at", "-"),
                "scheduler_enabled": bool(scheduler_cfg.get("enabled", True)),
                "schedule_time": f"{int(scheduler_cfg.get('hour', 9)):02d}:{int(scheduler_cfg.get('minute', 0)):02d}",
                "schedule_timezone": str(scheduler_cfg.get("timezone", DEFAULT_TIMEZONE)),
                "message_template": str(cfg.get("messageTemplate", "")),
                "targets": receivers,
                "target_count": len(receivers),
                "next_run": runtime_snapshot.get("next_run", "-"),
                "last_status": runtime_snapshot.get("last_status", "-"),
                "last_start": runtime_snapshot.get("last_start", "-"),
                "is_running": runtime_snapshot.get("is_running", False),
            }
        )

    return {
        "ok": True,
        "users": rows,
        "task_count": len(rows),
    }


@app.get("/api/admin/tasks/{username}")
async def api_admin_task_detail(request: Request, username: str, log_limit: int = MAX_LOG_LINES):
    _require_admin_session(request)
    username = username.strip()
    user_meta = _get_user_meta_or_404(username)

    try:
        cfg = _load_user_config(username)
        users_data = _load_user_users_data(username)
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"加载任务详情失败：{exc}"},
        )

    scheduler_cfg = cfg.get("scheduler", {})
    runtime = _get_runtime(username)
    target_count = _count_targets(users_data)
    snapshot = runtime.snapshot(account_count=len(users_data), target_count=target_count)

    accounts = []
    all_targets = []
    for item in users_data:
        targets = _sanitize_targets(item.get("targets", []))
        all_targets.extend(targets)
        accounts.append(
            {
                "username": str(item.get("username", "未知用户")),
                "unique_id": str(item.get("unique_id", "")),
                "target_count": len(targets),
                "targets": targets,
                "cookie_count": len(item.get("cookies", [])) if isinstance(item.get("cookies", []), list) else 0,
            }
        )

    log_limit = min(max(100, log_limit), 3000)
    return {
        "ok": True,
        "task": {
            "username": username,
            "unique_id": user_meta.get("unique_id", ""),
            "created_at": user_meta.get("created_at", "-"),
            "scheduler_enabled": bool(scheduler_cfg.get("enabled", True)),
            "schedule_time": f"{int(scheduler_cfg.get('hour', 9)):02d}:{int(scheduler_cfg.get('minute', 0)):02d}",
            "schedule_timezone": str(scheduler_cfg.get("timezone", DEFAULT_TIMEZONE)),
            "message_template": str(cfg.get("messageTemplate", "")),
            "targets": all_targets,
            "target_count": len(all_targets),
            "runtime": snapshot,
            "history": runtime.history_rows(),
            "logs": runtime.recent_logs(limit=log_limit),
            "config": {
                "multiTask": bool(cfg.get("multiTask", True)),
                "taskCount": int(cfg.get("taskCount", 1) or 1),
                "hitokotoTypes": cfg.get("hitokotoTypes", []),
                "proxyAddress": str(cfg.get("proxyAddress", "")),
            },
            "accounts": accounts,
        },
    }


@app.post("/api/admin/tasks/{username}/delete")
async def api_admin_delete_task(request: Request, username: str):
    _require_admin_session(request)
    username = username.strip()
    _get_user_meta_or_404(username)

    cfg = _load_user_config(username)
    scheduler_cfg = cfg.setdefault("scheduler", {})
    scheduler_cfg["enabled"] = False
    _save_user_config(username, cfg)

    _remove_user_schedule_job(username)
    runtime = _get_runtime(username)
    runtime.update_next_run(None)
    runtime.add_log("管理员已删除（禁用）该用户定时任务")

    return {"ok": True, "message": f"已删除用户 {username} 的定时任务。"}


@app.delete("/api/admin/users/{username}")
async def api_admin_delete_user(request: Request, username: str):
    _require_admin_session(request)
    username = username.strip()

    users_map = _load_users_meta()
    user = users_map.get(username)
    if not user:
        return JSONResponse(status_code=404, content={"ok": False, "message": "用户不存在。"})

    _remove_user_schedule_job(username)
    tenant_dir = _get_tenant_dir(user)
    if tenant_dir.exists():
        shutil.rmtree(tenant_dir, ignore_errors=True)

    users_map.pop(username, None)
    _save_users_meta(users_map)
    _delete_runtime(username)

    return {"ok": True, "message": f"用户 {username} 已删除。"}


@app.get("/health")
async def health():
    return {"ok": True, "status": "alive"}


def run_server():
    port = int(os.getenv("PORT", "7860"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, workers=1)


if __name__ == "__main__":
    run_server()
