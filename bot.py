# -*- coding: utf-8 -*-
"""Нейро-художник: Telegram-бот генерации картинок (всё в одном файле)."""
import os
import re
import html
import time
import queue
import random
import hmac
import hashlib
import sqlite3
import logging
import threading
from io import BytesIO
from datetime import datetime, timedelta
from contextlib import contextmanager
from collections import OrderedDict
from urllib.parse import quote

import requests
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from flask import Flask, request

# ============================================================
#                         НАСТРОЙКИ
# ============================================================
TOKEN = os.environ.get("TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
WEBHOOK_URL = (os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")).rstrip("/")
DATABASE_URL = os.environ.get("DATABASE_URL", "")          # если задан — Postgres, иначе SQLite
DB_PATH = os.environ.get("DB_PATH", "imagebot.db")
IMAGE_API_KEY = os.environ.get("IMAGE_API_KEY", "")        # ключ Pollinations (необязательно)
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "flux")
CRYPTO_PAY_TOKEN = os.environ.get("CRYPTO_PAY_TOKEN", "")  # пусто = оплата отключена

SUPPORT_USERNAME = os.environ.get("SUPPORT_USERNAME", "coo_vernost")
CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "")  # "@channel"; пусто = без проверки подписки
IMG_START = os.environ.get("IMG_START", "")                # картинка для меню (необязательно)

DAILY_FREE = int(os.environ.get("DAILY_FREE", 5))          # бесплатных генераций в сутки
NEWBIE_FREE = int(os.environ.get("NEWBIE_FREE", 2))        # лимит в первые 24 часа после регистрации
REF_BONUS = int(os.environ.get("REF_BONUS", 3))            # приглашающему
REF_WELCOME = int(os.environ.get("REF_WELCOME", 1))        # приглашённому
USER_COOLDOWN = float(os.environ.get("USER_COOLDOWN", 4))  # пауза между запросами одного человека
MIN_INTERVAL = float(os.environ.get("MIN_INTERVAL", 16))   # пауза между запросами к API картинок
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", 15))
WORKERS = int(os.environ.get("WORKERS", 2))
KEEP_ALIVE = os.environ.get("KEEP_ALIVE", "1") == "1"

PACKS = {"p20": (20, 100), "p60": (60, 250), "p150": (150, 500)}   # id: (генераций, цена ₽)
SUB_PRICE = int(os.environ.get("SUB_PRICE", 600))
SUB_DAYS = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("imagebot")
telebot.logger.setLevel(logging.INFO)

if not TOKEN:
    raise RuntimeError("Не задана переменная окружения TOKEN")

bot = telebot.TeleBot(TOKEN, threaded=False)
app = Flask(__name__)
TG_SECRET = hashlib.sha256(TOKEN.encode()).hexdigest()

state_lock = threading.Lock()
busy_users = set()
last_request = {}
pending_jobs = 0
fail_streak = 0
last_alert = 0.0
job_queue = queue.Queue()

# ============================================================
#                     БАЗА ДАННЫХ (SQLite / Postgres)
# ============================================================
USE_PG = bool(DATABASE_URL)
if USE_PG:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    pg_pool = psycopg2.pool.ThreadedConnectionPool(1, 8, DATABASE_URL)


class Conn:
    """Единый интерфейс: SQL пишем в стиле SQLite (? и MAX2), для Postgres он переписывается."""

    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql, params=()):
        if USE_PG:
            sql = sql.replace("?", "%s").replace("MAX2(", "GREATEST(")
            cur = self.raw.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params)
            return cur
        return self.raw.execute(sql.replace("MAX2(", "MAX("), params)


@contextmanager
def db():
    if USE_PG:
        raw = pg_pool.getconn()
    else:
        raw = sqlite3.connect(DB_PATH, timeout=15)
        raw.row_factory = sqlite3.Row
    try:
        yield Conn(raw)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        if USE_PG:
            pg_pool.putconn(raw)
        else:
            raw.close()


def first(cur):
    """Первое значение первой строки (работает и для SQLite, и для Postgres)."""
    row = cur.fetchone()
    if row is None:
        return None
    return list(row.values())[0] if isinstance(row, dict) else row[0]


def insert_returning_id(c, sql, params):
    if USE_PG:
        row = c.execute(sql + " RETURNING id", params).fetchone()
        return row["id"]
    return c.execute(sql, params).lastrowid


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return datetime.now().strftime("%Y-%m-%d")


def parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def init_db():
    pk = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    ddl = [
        """CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, lang TEXT DEFAULT 'ru',
            style TEXT DEFAULT 'none', size TEXT DEFAULT 'square', preset TEXT DEFAULT '',
            enhance INTEGER DEFAULT 1, used_date TEXT DEFAULT '', used_today INTEGER DEFAULT 0,
            credits INTEGER DEFAULT 0, sub_until TEXT DEFAULT '', total_generated INTEGER DEFAULT 0,
            last_prompt TEXT DEFAULT '', referrer_id BIGINT DEFAULT 0, ref_count INTEGER DEFAULT 0,
            banned INTEGER DEFAULT 0, created_at TEXT)""",
        f"""CREATE TABLE IF NOT EXISTS generations (
            id {pk}, user_id BIGINT, prompt TEXT, style TEXT, size TEXT, created_at TEXT)""",
        f"""CREATE TABLE IF NOT EXISTS favorites (
            id {pk}, user_id BIGINT, file_id TEXT, prompt TEXT, created_at TEXT)""",
        f"""CREATE TABLE IF NOT EXISTS reports (
            id {pk}, reporter_id BIGINT, author_id BIGINT, prompt TEXT, created_at TEXT)""",
        f"""CREATE TABLE IF NOT EXISTS orders (
            id {pk}, user_id BIGINT, item TEXT, price REAL, status TEXT, created_at TEXT)""",
        "CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, credits INTEGER, uses_left INTEGER)",
        "CREATE TABLE IF NOT EXISTS promo_used (user_id BIGINT, code TEXT, PRIMARY KEY (user_id, code))",
        "CREATE TABLE IF NOT EXISTS blocked_words (word TEXT PRIMARY KEY)",
    ]
    with db() as c:
        if not USE_PG:
            c.execute("PRAGMA journal_mode=WAL")
        for q in ddl:
            c.execute(q)
    # миграция старой базы (если файл уже существовал)
    new_cols = {"lang": "TEXT DEFAULT 'ru'", "preset": "TEXT DEFAULT ''", "enhance": "INTEGER DEFAULT 1",
                "sub_until": "TEXT DEFAULT ''", "referrer_id": "BIGINT DEFAULT 0", "ref_count": "INTEGER DEFAULT 0"}
    for col, typ in new_cols.items():
        try:
            with db() as c:
                c.execute(f"ALTER TABLE users ADD COLUMN {'IF NOT EXISTS ' if USE_PG else ''}{col} {typ}")
        except Exception:
            pass  # колонка уже есть


# ---------- пользователи ----------
RU_LANGS = ("ru", "uk", "be", "kk", "ky", "tg", "uz", "hy", "az", "ka")
USER_FIELDS = ("style", "size", "preset", "enhance", "lang", "last_prompt")


def get_user(user_id):
    with db() as c:
        row = c.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None


def ensure_user(tg, referrer=0):
    """Возвращает (user_dict, is_new). Обновляет username."""
    uname = tg.username or ""
    u = get_user(tg.id)
    if u:
        if u["username"] != uname:
            with db() as c:
                c.execute("UPDATE users SET username=? WHERE user_id=?", (uname, tg.id))
            u["username"] = uname
        return u, False
    lc = (getattr(tg, "language_code", None) or "ru")[:2].lower()
    lang = "ru" if lc in RU_LANGS else "en"
    with db() as c:
        c.execute("INSERT INTO users (user_id, username, lang, referrer_id, created_at) VALUES (?,?,?,?,?) "
                  "ON CONFLICT DO NOTHING", (tg.id, uname, lang, referrer, now_str()))
    return get_user(tg.id), True


def set_user_field(user_id, field, value):
    assert field in USER_FIELDS
    with db() as c:
        c.execute(f"UPDATE users SET {field}=? WHERE user_id=?", (value, user_id))


def add_credits(user_id, n):
    with db() as c:
        c.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (n, user_id))


# ---------- лимиты ----------
def has_sub(u):
    return bool(u.get("sub_until")) and u["sub_until"] > now_str()


def is_newbie(u):
    dt = parse_dt(u.get("created_at"))
    return bool(dt and datetime.now() - dt < timedelta(hours=24)) and u["user_id"] != ADMIN_ID


def daily_limit(u):
    return NEWBIE_FREE if is_newbie(u) else DAILY_FREE


def free_left(u):
    used = u["used_today"] if u["used_date"] == today_str() else 0
    return max(daily_limit(u) - used, 0)


def try_consume(user_id, n=1):
    """Списывает n генераций. Возвращает список видов списания или None, если не хватает."""
    with db() as c:
        row = c.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        u = dict(row)
        if has_sub(u):
            return ["sub"] * n
        used = u["used_today"] if u["used_date"] == today_str() else 0
        take_free = min(n, max(daily_limit(u) - used, 0))
        take_credit = n - take_free
        if take_credit > u["credits"]:
            return None
        c.execute("UPDATE users SET used_date=?, used_today=?, credits=? WHERE user_id=?",
                  (today_str(), used + take_free, u["credits"] - take_credit, user_id))
        return ["free"] * take_free + ["credit"] * take_credit


def refund(user_id, kinds):
    free_n = kinds.count("free")
    credit_n = kinds.count("credit")
    if not (free_n or credit_n):
        return
    with db() as c:
        c.execute("UPDATE users SET used_today = MAX2(used_today - ?, 0), credits = credits + ? WHERE user_id=?",
                  (free_n, credit_n, user_id))


def log_generation(user_id, prompt, style, size):
    with db() as c:
        c.execute("INSERT INTO generations (user_id, prompt, style, size, created_at) VALUES (?,?,?,?,?)",
                  (user_id, prompt, style, size, now_str()))
        c.execute("UPDATE users SET total_generated = total_generated + 1 WHERE user_id=?", (user_id,))


# ---------- промокоды ----------
def apply_promo(user_id, code):
    """Возвращает (кредиты, статус)."""
    with db() as c:
        row = c.execute("SELECT credits, uses_left FROM promos WHERE code=?", (code,)).fetchone()
        if not row:
            return 0, "not_found"
        if c.execute("SELECT 1 FROM promo_used WHERE user_id=? AND code=?", (user_id, code)).fetchone():
            return 0, "used"
        if row["uses_left"] <= 0:
            return 0, "not_found"
        cur = c.execute("UPDATE promos SET uses_left = uses_left - 1 WHERE code=? AND uses_left > 0", (code,))
        if cur.rowcount != 1:
            return 0, "not_found"
        c.execute("INSERT INTO promo_used (user_id, code) VALUES (?,?)", (user_id, code))
        c.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (row["credits"], user_id))
        return row["credits"], "ok"


# ---------- заказы / оплата ----------
def create_order(user_id, item, price):
    with db() as c:
        return insert_returning_id(
            c, "INSERT INTO orders (user_id, item, price, status, created_at) VALUES (?,?,?,?,?)",
            (user_id, item, price, "pending", now_str()))


def get_order(order_id):
    with db() as c:
        row = c.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        return dict(row) if row else None


def set_order_status(order_id, new_status, expected):
    marks = ",".join("?" * len(expected))
    with db() as c:
        cur = c.execute(f"UPDATE orders SET status=? WHERE id=? AND status IN ({marks})",
                        (new_status, order_id, *expected))
        return cur.rowcount == 1


def extend_subscription(user_id):
    with db() as c:
        row = c.execute("SELECT sub_until FROM users WHERE user_id=?", (user_id,)).fetchone()
        base = datetime.now()
        cur_end = parse_dt(row["sub_until"]) if row and row["sub_until"] else None
        if cur_end and cur_end > base:
            base = cur_end
        new_end = (base + timedelta(days=SUB_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET sub_until=? WHERE user_id=?", (new_end, user_id))
        return new_end


# ============================================================
#                          ТЕКСТЫ (RU / EN)
# ============================================================
T = {
    "ru": {
        "welcome": ("🎨 <b>Нейро-художник</b>\n\nОпишите картинку словами, и я её нарисую.\n"
                    "Например: <i>космонавт верхом на лошади, закат, масляная живопись</i>\n\n"
                    "💡 Можно писать по-русски, бот сам переведёт. Лучше коротко: одна сцена, один-два объекта. "
                    "Надписи на картинке и конкретные персонажи из фильмов получаются плохо."),
        "help": ("❓ <b>Помощь</b>\n\n"
                 "• Просто отправьте описание, и бот нарисует картинку.\n"
                 "• «Стиль», «Формат» и «Пресеты» меняют результат (пресеты — готовые настройки для аватарки, превью YouTube, логотипа и т.д.).\n"
                 "• «Улучшение описания» само дополняет ваш запрос деталями (можно отключить в настройках).\n"
                 "• «🎲 ×3» — три варианта сразу, «📎 Без сжатия» — файл в полном качестве.\n"
                 "• ⭐ — в избранное, 🚩 — пожаловаться на неподходящую картинку.\n"
                 "• /history — история, /favorites — избранное, /promo КОД — промокод, /lang — язык.\n"
                 "• Бесплатно {free} генераций в сутки (первые 24 часа — {newbie}). Больше — за пакеты или приглашения друзей.\n"
                 "• Запросы 18+ и запрещённый контент не принимаются."),
        "b_style": "🎨 Стиль", "b_size": "📐 Формат", "b_preset": "🎯 Пресеты", "b_idea": "🎲 Случайная идея",
        "b_hist": "🕘 История", "b_fav": "⭐ Избранное", "b_buy": "💳 Купить", "b_ref": "🎁 Друзья",
        "b_profile": "👤 Профиль", "b_settings": "⚙️ Настройки", "b_help": "❓ Помощь",
        "b_support": "🛠 Тех поддержка", "b_menu": "🏠 В меню", "b_back": "⬅️ Назад",
        "b_again": "🔄 Ещё вариант", "b_var": "🎲 ×3 варианта", "b_doc": "📎 Без сжатия",
        "b_fav_add": "⭐ В избранное", "b_report": "🚩 Пожаловаться", "b_promo": "🎟 Промокод",
        "banned": "🚫 Вы забанены.",
        "too_short": "Опишите картинку подробнее (минимум 3 символа).",
        "too_long": "Описание слишком длинное (максимум 500 символов).",
        "blocked": "🚫 Такой запрос запрещён правилами бота. Попытка не списана.",
        "cooldown": "⏳ Не так быстро, подождите {s} сек.",
        "busy": "⏳ Дождитесь завершения предыдущей генерации.",
        "queue_full": "😔 Сейчас слишком много запросов. Попробуйте через минуту.",
        "limit_over": ("❌ Бесплатные генерации на сегодня закончились.\nЛимит обновится завтра. "
                       "Можно купить пакет («Купить»), ввести промокод или пригласить друзей. Вопросы: @{support}"),
        "not_enough": "❌ Для этого нужно {n} генераций, а у вас меньше.",
        "queued": "⏳ Рисую... Это займёт до минуты. В очереди: {pos}",
        "gen_fail": "⚠️ Не удалось создать картинку. Попытка не списана, попробуйте ещё раз чуть позже.",
        "translate_fail": "⚠️ Не удалось обработать описание. Напишите его на английском или попробуйте позже. Попытка не списана.",
        "partial": "⚠️ Создано {done} из {n}. Остальные генерации возвращены.",
        "sub_required": "Для использования бота подпишитесь на канал {ch} и повторите запрос.",
        "quota_free": "🎁 Бесплатно сегодня: <b>{left}</b>",
        "quota_credits": " · 💳 Пакет: <b>{credits}</b>",
        "quota_sub": "♾ Безлимит до <b>{date}</b>",
        "profile": ("👤 <b>Профиль</b>\n\n🆔 ID: <code>{uid}</code>\n{quota}\n🖼 Всего создано: {total}\n"
                    "🎨 Стиль: {style}\n📐 Формат: {size}\n🎯 Пресет: {preset}\n✍️ Улучшение описания: {enh}\n"
                    "👥 Приглашено друзей: {refs}"),
        "on": "вкл", "off": "выкл", "none": "нет",
        "settings_title": "⚙️ <b>Настройки</b>",
        "enhance_btn": "✍️ Улучшение описания: {state}",
        "lang_set": "Язык: Русский",
        "history_title": "🕘 <b>История</b>\nНажмите на запрос, чтобы повторить:",
        "history_empty": "История пока пуста. Отправьте первое описание!",
        "fav_title": "⭐ <b>Избранное</b>\nНажмите, чтобы открыть:",
        "fav_empty": "В избранном пока пусто. Под картинкой нажмите «⭐ В избранное».",
        "fav_saved": "⭐ Сохранено в избранное",
        "fav_full": "В избранном уже 50 картинок. Удалите лишние.",
        "fav_deleted": "🗑 Удалено",
        "b_del": "🗑 Удалить",
        "report_sent": "🚩 Жалоба отправлена, спасибо!",
        "expired": "Файл устарел, создайте картинку заново.",
        "promo_usage": "Формат: /promo КОД",
        "promo_ok": "🎟 Промокод активирован! Начислено генераций: {n}",
        "promo_used": "❌ Вы уже использовали этот промокод.",
        "promo_nf": "❌ Промокод не найден или закончился.",
        "promo_hint": "🎟 Отправьте команду: <code>/promo КОД</code>",
        "buy_title": ("💳 <b>Купить генерации</b>\n\nОплата криптовалютой (USDT или TON) через CryptoBot, цена в рублях.\n\n"
                      "• Пакеты не сгорают.\n• Подписка — безлимит на {days} дней."),
        "buy_off": "💳 Оплата пока недоступна. Следите за новостями!",
        "pack_label": "📦 {n} генераций — {price}₽",
        "sub_label": "♾ Безлимит {days} дн. — {price}₽",
        "choose_asset": "Заказ: <b>{item}</b> — {price}₽\nВыберите валюту:",
        "invoice": ("💳 <b>Заказ #{id}</b>\n{item}\nК оплате: <b>{price}₽</b> ({asset})\n\n"
                    "Счёт действует 1 час. После оплаты придёт уведомление."),
        "pay_btn": "💎 Оплатить {asset}",
        "paid_pack": "✅ Оплата прошла! Начислено <b>{n}</b> генераций.",
        "paid_sub": "✅ Оплата прошла! Безлимит активен до <b>{date}</b>.",
        "pay_error": "❌ Ошибка создания счёта. Попробуйте позже.",
        "ref_text": ("🎁 <b>Приглашайте друзей</b>\n\nЗа каждого друга, который запустит бота по вашей ссылке, "
                     "вы получите <b>{bonus}</b> генераций, а друг — <b>{welcome}</b>.\n\n"
                     "👥 Приглашено: {refs}\n\n🔗 Ваша ссылка:\n<code>{link}</code>"),
        "ref_joined": "🎁 По вашей ссылке пришёл новый пользователь! +{n} генераций.",
        "ref_welcome": "🎁 Вам начислено {n} бонусных генераций за переход по приглашению!",
        "style_title": "🎨 <b>Выберите стиль:</b>",
        "size_title": "📐 <b>Выберите формат:</b>",
        "preset_title": "🎯 <b>Пресеты</b>\nГотовые настройки под задачу (стиль, формат и подсказки для нейросети):",
        "preset_reset": "♻️ Сбросить пресет",
        "unknown_cmd": "Неизвестная команда. Просто отправьте описание картинки текстом.",
        "ping": "pong ✅ бот работает",
        "error": "⚠️ Произошла ошибка, попробуйте ещё раз.",
        "start_error": "⚠️ Ошибка при запуске, попробуйте ещё раз позже.",
    },
    "en": {
        "welcome": ("🎨 <b>AI Artist</b>\n\nDescribe a picture in words and I'll draw it.\n"
                    "For example: <i>an astronaut riding a horse, sunset, oil painting</i>\n\n"
                    "💡 Keep it short: one scene, one or two subjects. Text inside the picture and "
                    "specific movie characters come out poorly."),
        "help": ("❓ <b>Help</b>\n\n"
                 "• Just send a description and I'll draw it.\n"
                 "• «Style», «Format» and «Presets» change the result (presets are ready-made settings for avatars, YouTube thumbnails, logos, etc.).\n"
                 "• «Prompt enhancement» adds details to your request automatically (can be turned off in settings).\n"
                 "• «🎲 ×3» gives three variants, «📎 Uncompressed» sends the full-quality file.\n"
                 "• ⭐ saves to favorites, 🚩 reports an inappropriate picture.\n"
                 "• /history, /favorites, /promo CODE, /lang.\n"
                 "• {free} free generations per day (first 24 hours: {newbie}). Get more with packs or by inviting friends.\n"
                 "• 18+ and prohibited content is not allowed."),
        "b_style": "🎨 Style", "b_size": "📐 Format", "b_preset": "🎯 Presets", "b_idea": "🎲 Random idea",
        "b_hist": "🕘 History", "b_fav": "⭐ Favorites", "b_buy": "💳 Buy", "b_ref": "🎁 Friends",
        "b_profile": "👤 Profile", "b_settings": "⚙️ Settings", "b_help": "❓ Help",
        "b_support": "🛠 Support", "b_menu": "🏠 Menu", "b_back": "⬅️ Back",
        "b_again": "🔄 Another one", "b_var": "🎲 ×3 variants", "b_doc": "📎 Uncompressed",
        "b_fav_add": "⭐ Favorite", "b_report": "🚩 Report", "b_promo": "🎟 Promo code",
        "banned": "🚫 You are banned.",
        "too_short": "Please describe the picture in more detail (3+ characters).",
        "too_long": "Description is too long (500 characters max).",
        "blocked": "🚫 This request is not allowed. Your attempt was not used.",
        "cooldown": "⏳ Slow down, wait {s} sec.",
        "busy": "⏳ Please wait for your previous generation to finish.",
        "queue_full": "😔 Too many requests right now. Try again in a minute.",
        "limit_over": ("❌ You're out of free generations for today.\nThe limit resets tomorrow. "
                       "You can buy a pack («Buy»), use a promo code or invite friends. Questions: @{support}"),
        "not_enough": "❌ You need {n} generations for this, but you have fewer.",
        "queued": "⏳ Drawing... This takes up to a minute. Queue position: {pos}",
        "gen_fail": "⚠️ Couldn't create the picture. Your attempt was not used, please try again a bit later.",
        "translate_fail": "⚠️ Couldn't process the description. Try again later. Your attempt was not used.",
        "partial": "⚠️ Created {done} of {n}. The rest were refunded.",
        "sub_required": "Please subscribe to {ch} and repeat your request.",
        "quota_free": "🎁 Free today: <b>{left}</b>",
        "quota_credits": " · 💳 Pack: <b>{credits}</b>",
        "quota_sub": "♾ Unlimited until <b>{date}</b>",
        "profile": ("👤 <b>Profile</b>\n\n🆔 ID: <code>{uid}</code>\n{quota}\n🖼 Total created: {total}\n"
                    "🎨 Style: {style}\n📐 Format: {size}\n🎯 Preset: {preset}\n✍️ Prompt enhancement: {enh}\n"
                    "👥 Friends invited: {refs}"),
        "on": "on", "off": "off", "none": "none",
        "settings_title": "⚙️ <b>Settings</b>",
        "enhance_btn": "✍️ Prompt enhancement: {state}",
        "lang_set": "Language: English",
        "history_title": "🕘 <b>History</b>\nTap a request to repeat it:",
        "history_empty": "History is empty. Send your first description!",
        "fav_title": "⭐ <b>Favorites</b>\nTap to open:",
        "fav_empty": "No favorites yet. Tap «⭐ Favorite» under a picture.",
        "fav_saved": "⭐ Saved to favorites",
        "fav_full": "You already have 50 favorites. Delete some first.",
        "fav_deleted": "🗑 Deleted",
        "b_del": "🗑 Delete",
        "report_sent": "🚩 Report sent, thank you!",
        "expired": "This file has expired, please generate again.",
        "promo_usage": "Usage: /promo CODE",
        "promo_ok": "🎟 Promo code activated! Generations added: {n}",
        "promo_used": "❌ You've already used this promo code.",
        "promo_nf": "❌ Promo code not found or exhausted.",
        "promo_hint": "🎟 Send the command: <code>/promo CODE</code>",
        "buy_title": ("💳 <b>Buy generations</b>\n\nPay with crypto (USDT or TON) via CryptoBot, prices in RUB.\n\n"
                      "• Packs never expire.\n• Subscription — unlimited for {days} days."),
        "buy_off": "💳 Payments are not available yet. Stay tuned!",
        "pack_label": "📦 {n} generations — {price}₽",
        "sub_label": "♾ Unlimited {days} days — {price}₽",
        "choose_asset": "Order: <b>{item}</b> — {price}₽\nChoose a currency:",
        "invoice": ("💳 <b>Order #{id}</b>\n{item}\nTo pay: <b>{price}₽</b> ({asset})\n\n"
                    "The invoice is valid for 1 hour. You'll get a notification after payment."),
        "pay_btn": "💎 Pay {asset}",
        "paid_pack": "✅ Payment received! Added <b>{n}</b> generations.",
        "paid_sub": "✅ Payment received! Unlimited is active until <b>{date}</b>.",
        "pay_error": "❌ Couldn't create the invoice. Try again later.",
        "ref_text": ("🎁 <b>Invite friends</b>\n\nFor every friend who starts the bot via your link you get "
                     "<b>{bonus}</b> generations, and your friend gets <b>{welcome}</b>.\n\n"
                     "👥 Invited: {refs}\n\n🔗 Your link:\n<code>{link}</code>"),
        "ref_joined": "🎁 A new user joined via your link! +{n} generations.",
        "ref_welcome": "🎁 You received {n} bonus generations for joining via an invitation!",
        "style_title": "🎨 <b>Choose a style:</b>",
        "size_title": "📐 <b>Choose a format:</b>",
        "preset_title": "🎯 <b>Presets</b>\nReady-made settings for a task (style, format and hints for the AI):",
        "preset_reset": "♻️ Reset preset",
        "unknown_cmd": "Unknown command. Just send a picture description as text.",
        "ping": "pong ✅ the bot is running",
        "error": "⚠️ Something went wrong, please try again.",
        "start_error": "⚠️ Startup error, please try again later.",
    },
}


def t(u, key, **kw):
    lang = (u or {}).get("lang", "ru")
    s = T.get(lang, T["ru"]).get(key) or T["ru"][key]
    return s.format(**kw) if kw else s


def lab(item, u):
    """Название из словаря с ключами ru/en."""
    return item.get((u or {}).get("lang", "ru"), item["ru"])


# ============================================================
#                   СТИЛИ, ФОРМАТЫ, ПРЕСЕТЫ, ИДЕИ
# ============================================================
STYLES = {
    "none": {"ru": "✨ Без стиля", "en": "✨ No style", "prefix": "", "suffix": "", "model": "", "neg": ""},
    "cartoon": {"ru": "🎬 Мультфильм", "en": "🎬 Cartoon",
                "prefix": "cartoon illustration, 2d animation style, flat colors, bold black outlines, cel shading, ",
                "suffix": ", vibrant colors", "model": "flux",
                "neg": "photorealistic, photo, realistic, 3d render, detailed skin, painting"},
    "realistic": {"ru": "📷 Реализм", "en": "📷 Realistic", "prefix": "professional photo, ",
                  "suffix": ", photorealistic, natural lighting, sharp focus, 8k", "model": "flux-realism",
                  "neg": "cartoon, anime, illustration, drawing"},
    "anime": {"ru": "🌸 Аниме", "en": "🌸 Anime", "prefix": "anime illustration, ",
              "suffix": ", vibrant colors, detailed, studio quality", "model": "flux-anime",
              "neg": "photorealistic, 3d render, photo"},
    "art": {"ru": "🎨 Арт", "en": "🎨 Art", "prefix": "digital painting, concept art, ",
            "suffix": ", artstation, highly detailed", "model": "flux", "neg": "photo, low quality, blurry"},
    "3d": {"ru": "🧊 3D", "en": "🧊 3D", "prefix": "3d render, ",
           "suffix": ", octane render, soft lighting, high detail, pixar style", "model": "flux-3d",
           "neg": "flat, 2d, sketch, drawing"},
    "pixel": {"ru": "👾 Пиксель-арт", "en": "👾 Pixel art", "prefix": "pixel art, ",
              "suffix": ", 16-bit, retro game style, limited palette", "model": "flux",
              "neg": "smooth, blurry, photorealistic, 3d render"},
    "cyber": {"ru": "🌃 Киберпанк", "en": "🌃 Cyberpunk", "prefix": "cyberpunk scene, ",
              "suffix": ", neon lights, futuristic city, cinematic lighting", "model": "flux",
              "neg": "daylight, rustic, countryside"},
}

SIZES = {
    "square": {"ru": "⬜ Квадрат", "en": "⬜ Square", "wh": (1024, 1024)},
    "portrait": {"ru": "📱 Вертикальный", "en": "📱 Portrait", "wh": (768, 1024)},
    "landscape": {"ru": "🖥 Горизонтальный", "en": "🖥 Landscape", "wh": (1024, 768)},
    "wide": {"ru": "📺 YouTube 16:9", "en": "📺 Wide 16:9", "wh": (1024, 576)},
}

PRESETS = {
    "yt_avatar": {"ru": "📺 Аватарка канала", "en": "📺 Channel avatar", "style": "cartoon", "size": "square",
                  "extra": ", centered subject, bold simple shapes, high contrast, clean background, avatar"},
    "yt_thumb": {"ru": "🖼 Превью YouTube", "en": "🖼 YouTube thumbnail", "style": "art", "size": "wide",
                 "extra": ", dramatic lighting, high contrast, vivid colors, bold composition, empty space on one side"},
    "logo": {"ru": "🔷 Логотип", "en": "🔷 Logo", "style": "none", "size": "square",
             "extra": ", minimalist vector logo, flat design, simple shapes, white background"},
    "sticker": {"ru": "😀 Стикер", "en": "😀 Sticker", "style": "cartoon", "size": "square",
                "extra": ", sticker design, thick white border, plain background, cute"},
    "wallpaper": {"ru": "📱 Обои на телефон", "en": "📱 Phone wallpaper", "style": "art", "size": "portrait",
                  "extra": ", wallpaper, highly detailed, beautiful composition"},
}

IDEAS = [
    "a cozy cabin in a snowy forest at night, warm light in the windows",
    "a friendly robot gardener watering glowing flowers",
    "an astronaut riding a horse on mars, sunset",
    "a fox in a tiny wizard hat reading a book",
    "a floating island with waterfalls and a lighthouse",
    "a retro diner on the moon with neon signs",
    "a dragon sleeping on a pile of books in a grand library",
    "a steampunk airship above a city of clocks",
    "a cat chef cooking ramen in a tiny kitchen",
    "an underwater city lit by glowing jellyfish lamps",
    "a samurai panda under cherry blossoms",
    "a giant treehouse village at golden hour",
    "a lonely lighthouse in a stormy sea, dramatic clouds",
    "a cute baby dinosaur wearing a raincoat in a puddle",
    "a futuristic train crossing a desert at sunrise",
    "a magical bookshop hidden in an alley, lanterns glowing",
]

# ============================================================
#                          ФИЛЬТР ЗАПРОСОВ
# ============================================================
BASE_BLOCK = re.compile(
    r"(loli\w*|shota\w*|child\s*porn\w*|детск\w*\s*порн\w*|порн\w*|\bporn\w*|nsfw|nude\w*|naked|topless|"
    r"гол(ая|ый|ые|ых|ую)\b|обнажённ\w*|обнажен\w*|эрот\w*|erotic|explicit|hentai|хентай|\bsex\w*|секс\w*|"
    r"lingerie|нижнее\s+бель\w*|boob\w*|nipple\w*|undress\w*|раздет\w*|раздень|underage|несовершеннолетн\w*|"
    r"self[\s-]?harm|gore|расчленен\w*)", re.I)
COMPACT_BLOCK = ("porn", "nsfw", "nude", "naked", "hentai", "порн", "хентай")
extra_block_words = set(w.strip().lower() for w in os.environ.get("BLOCKED_EXTRA", "").split(",") if w.strip())


def reload_block_words():
    try:
        with db() as c:
            words = [r["word"] for r in c.execute("SELECT word FROM blocked_words").fetchall()]
        extra_block_words.update(w.lower() for w in words)
    except Exception:
        log.exception("reload_block_words")


def is_blocked(text):
    low = text.lower()
    if BASE_BLOCK.search(low):
        return True
    compact = re.sub(r"[\W_]+", "", low)
    if any(w in compact for w in COMPACT_BLOCK):
        return True
    return any(w in low for w in extra_block_words)


SAFE_SUFFIX = ", fully clothed, family friendly, safe for work"
NEGATIVE_PROMPT = "nsfw, nude, naked, topless, nudity, sexual, explicit, erotic, underwear, gore, violence, text, watermark"

# ============================================================
#                          ВСПОМОГАТЕЛЬНЫЕ
# ============================================================


def notify_user(user_id, text, markup=None):
    try:
        bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        log.warning("notify_user %s: %s", user_id, e)


def notify_admin(text, markup=None):
    if ADMIN_ID:
        notify_user(ADMIN_ID, "🛠 " + text, markup)


def edit_safe(call, text, markup=None):
    msg = call.message
    try:
        if msg.content_type == "photo":
            bot.edit_message_caption(text, msg.chat.id, msg.message_id, parse_mode="HTML", reply_markup=markup)
        else:
            bot.edit_message_text(text, msg.chat.id, msg.message_id, parse_mode="HTML", reply_markup=markup)
    except ApiTelegramException as e:
        if "message is not modified" in str(e):
            return
        log.error("edit_safe FAILED: %s | text=%r", e, text[:80])
        try:
            bot.send_message(msg.chat.id, text, parse_mode="HTML", reply_markup=markup)
        except Exception as e2:
            log.error("edit_safe fallback FAILED: %s", e2)


def is_subscribed(user_id):
    if not CHANNEL_USERNAME:
        return True
    try:
        m = bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return m.status in ("member", "administrator", "creator")
    except Exception as e:
        log.warning("subscription check failed: %s", e)
        return True


def quota_line(u):
    if has_sub(u):
        return t(u, "quota_sub", date=u["sub_until"][:10])
    s = t(u, "quota_free", left=free_left(u))
    if u["credits"]:
        s += t(u, "quota_credits", credits=u["credits"])
    return s


def item_label(u, item):
    if item == "sub":
        return t(u, "sub_label", days=SUB_DAYS, price=SUB_PRICE)
    n, price = PACKS[item]
    return t(u, "pack_label", n=n, price=price)


def item_price(item):
    return SUB_PRICE if item == "sub" else PACKS[item][1]


# ============================================================
#                   ГЕНЕРАЦИЯ: ПЕРЕВОД, УЛУЧШЕНИЕ, КАРТИНКА
# ============================================================
CYR = re.compile(r"[а-яёА-ЯЁ]")


def translate_to_en(text):
    """Возвращает (английский_текст, был_ли_перевод). Бросает исключение, если перевод нужен, но не вышел."""
    if not CYR.search(text):
        return text, False
    try:
        r = requests.get("https://translate.googleapis.com/translate_a/single",
                         params={"client": "gtx", "sl": "auto", "tl": "en", "dt": "t", "q": text}, timeout=8)
        r.raise_for_status()
        out = "".join(part[0] for part in r.json()[0] if part and part[0]).strip()
        if out:
            return out, True
    except Exception as e:
        log.warning("translate (google) failed: %s", e)
    try:
        r = requests.get("https://api.mymemory.translated.net/get",
                         params={"q": text[:450], "langpair": "ru|en"}, timeout=8)
        out = r.json()["responseData"]["translatedText"].strip()
        if out and not CYR.search(out):
            return out, True
    except Exception as e:
        log.warning("translate (mymemory) failed: %s", e)
    raise RuntimeError("translation failed")


ENHANCE_SYSTEM = ("You write prompts for a text-to-image model. Rewrite the user's idea (it may be in any language) "
                  "as ONE vivid English description of a single scene, at most 50 words. Keep every key element "
                  "the user mentioned. Describe subjects, setting, lighting and mood. Do not include any written "
                  "text, captions or logos in the picture. Keep it family friendly. Output only the description.")


def enhance_prompt(text):
    """Улучшает описание через бесплатный текстовый API. При любой проблеме возвращает None."""
    try:
        r = requests.get("https://text.pollinations.ai/" + quote(text, safe=""),
                         params={"model": "openai", "system": ENHANCE_SYSTEM, "seed": random.randint(1, 10**6)},
                         headers={"Authorization": f"Bearer {IMAGE_API_KEY}"} if IMAGE_API_KEY else {}, timeout=20)
        if r.status_code != 200:
            return None
        lines = [ln.strip() for ln in r.text.strip().splitlines()
                 if ln.strip() and "pollinations" not in ln.lower() and not ln.strip().startswith("---")]
        out = " ".join(lines).strip().strip('"')
        if not (10 <= len(out) <= 700) or out.startswith(("{", "<")):
            return None
        return out
    except Exception as e:
        log.warning("enhance failed: %s", e)
        return None


def build_prompt(prompt, enhance):
    """Возвращает (английский_промпт, был_перевод, было_улучшение)."""
    if enhance:
        out = enhance_prompt(prompt)
        if out:
            return out, bool(CYR.search(prompt)), True
    en, translated = translate_to_en(prompt)
    return en, translated, False


gate_lock = threading.Lock()
last_call = 0.0


def wait_gate():
    """Выдерживает паузу между запросами (у бесплатного API есть лимит)."""
    global last_call
    with gate_lock:
        delta = time.time() - last_call
        if delta < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - delta)
        last_call = time.time()


def generate_image(prompt, style_key, size_key, seed):
    """Возвращает байты картинки или бросает исключение. Пробует модель стиля, затем обычную, затем без модели."""
    st = STYLES[style_key]
    full = f"{st['prefix']}{prompt}{st['suffix']}{SAFE_SUFFIX}"
    negative = ", ".join(x for x in (NEGATIVE_PROMPT, st["neg"]) if x)
    width, height = SIZES[size_key]["wh"]
    url = f"https://image.pollinations.ai/prompt/{quote(full, safe='')}"
    headers = {"Authorization": f"Bearer {IMAGE_API_KEY}"} if IMAGE_API_KEY else {}

    models = []
    for m in (st["model"] or IMAGE_MODEL, IMAGE_MODEL, None):
        if m not in models:
            models.append(m)

    last_err = None
    for i, model in enumerate(models):
        params = {"width": width, "height": height, "seed": seed, "nologo": "true",
                  "safe": "true", "negative_prompt": negative}
        if model:
            params["model"] = model
        try:
            r = requests.get(url, params=params, headers=headers, timeout=120)
            ctype = r.headers.get("content-type", "")
            if r.status_code == 200 and ctype.startswith("image/") and len(r.content) >= 2000:
                return r.content
            last_err = f"{r.status_code} {ctype} {r.text[:150] if ('text' in ctype or 'json' in ctype) else ''}"
        except requests.RequestException as e:
            last_err = str(e)
        log.warning("image api: модель %s не сработала: %s", model, last_err)
        if i < len(models) - 1:
            time.sleep(3)
    raise RuntimeError(f"image api failed: {last_err}")


# ---------- кэш результатов (для кнопок под картинками) ----------
results = OrderedDict()
result_counter = [0]
RESULTS_MAX = 40


def store_result(img, meta):
    with state_lock:
        result_counter[0] += 1
        key = result_counter[0]
        results[key] = dict(meta, img=img, file_id=None)
        while len(results) > RESULTS_MAX:
            results.popitem(last=False)
    return key


# ============================================================
#                           КЛАВИАТУРЫ
# ============================================================
def btn(u, key, cb):
    return types.InlineKeyboardButton(t(u, key), callback_data=cb)


def kb_main(u):
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(btn(u, "b_style", "menu_style"), btn(u, "b_size", "menu_size"))
    m.add(btn(u, "b_preset", "menu_preset"), btn(u, "b_idea", "menu_idea"))
    m.add(btn(u, "b_hist", "menu_history"), btn(u, "b_fav", "menu_fav"))
    m.add(btn(u, "b_buy", "menu_buy"), btn(u, "b_ref", "menu_ref"))
    m.add(btn(u, "b_profile", "menu_profile"), btn(u, "b_settings", "menu_settings"))
    m.add(btn(u, "b_help", "menu_help"),
          types.InlineKeyboardButton(t(u, "b_support"), url=f"https://t.me/{SUPPORT_USERNAME}"))
    return m


def kb_back(u):
    m = types.InlineKeyboardMarkup()
    m.add(btn(u, "b_menu", "menu_main"))
    return m


def kb_result(u, key):
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(btn(u, "b_again", f"again_{key}"), btn(u, "b_var", f"var3_{key}"))
    m.add(btn(u, "b_doc", f"doc_{key}"), btn(u, "b_fav_add", f"fav_{key}"))
    m.add(btn(u, "b_report", f"rep_{key}"), btn(u, "b_menu", "menu_new"))
    return m


def kb_variants(u, keys):
    m = types.InlineKeyboardMarkup(row_width=len(keys))
    m.add(*[types.InlineKeyboardButton(f"📎 {i}", callback_data=f"doc_{k}") for i, k in enumerate(keys, 1)])
    m.add(*[types.InlineKeyboardButton(f"⭐ {i}", callback_data=f"fav_{k}") for i, k in enumerate(keys, 1)])
    m.add(btn(u, "b_again", f"again_{keys[0]}"), btn(u, "b_menu", "menu_new"))
    return m


def kb_choice(u, options, current, prefix, extra_buttons=()):
    m = types.InlineKeyboardMarkup(row_width=2)
    for key, item in options.items():
        mark = "✅ " if key == current else ""
        m.add(types.InlineKeyboardButton(mark + lab(item, u), callback_data=f"{prefix}_{key}"))
    for b in extra_buttons:
        m.add(b)
    m.add(btn(u, "b_menu", "menu_main"))
    return m


# ============================================================
#                          ЭКРАНЫ МЕНЮ
# ============================================================
def screen_main(u):
    return t(u, "welcome") + "\n\n" + quota_line(u), kb_main(u)


def screen_help(u):
    return t(u, "help", free=DAILY_FREE, newbie=NEWBIE_FREE), kb_back(u)


def screen_profile(u):
    preset = lab(PRESETS[u["preset"]], u) if u["preset"] in PRESETS else t(u, "none")
    text = t(u, "profile", uid=u["user_id"], quota=quota_line(u), total=u["total_generated"],
             style=lab(STYLES[u["style"]], u), size=lab(SIZES[u["size"]], u), preset=preset,
             enh=t(u, "on") if u["enhance"] else t(u, "off"), refs=u["ref_count"])
    return text, kb_back(u)


def screen_style(u):
    return t(u, "style_title"), kb_choice(u, STYLES, u["style"], "set_style")


def screen_size(u):
    return t(u, "size_title"), kb_choice(u, SIZES, u["size"], "set_size")


def screen_preset(u):
    reset = types.InlineKeyboardButton(t(u, "preset_reset"), callback_data="set_preset_none")
    return t(u, "preset_title"), kb_choice(u, PRESETS, u["preset"], "set_preset", extra_buttons=(reset,))


def screen_settings(u):
    m = types.InlineKeyboardMarkup(row_width=2)
    state = t(u, "on") if u["enhance"] else t(u, "off")
    m.add(types.InlineKeyboardButton(t(u, "enhance_btn", state=state), callback_data="toggle_enhance"))
    m.add(types.InlineKeyboardButton(("✅ " if u["lang"] == "ru" else "") + "🇷🇺 Русский", callback_data="set_lang_ru"),
          types.InlineKeyboardButton(("✅ " if u["lang"] == "en" else "") + "🇬🇧 English", callback_data="set_lang_en"))
    m.add(btn(u, "b_promo", "menu_promo"))
    m.add(btn(u, "b_menu", "menu_main"))
    return t(u, "settings_title"), m


def screen_history(u):
    with db() as c:
        rows = c.execute("SELECT id, prompt FROM generations WHERE user_id=? ORDER BY id DESC LIMIT 10",
                         (u["user_id"],)).fetchall()
    if not rows:
        return t(u, "history_empty"), kb_back(u)
    m = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        m.add(types.InlineKeyboardButton("🔁 " + r["prompt"][:40], callback_data=f"hist_{r['id']}"))
    m.add(btn(u, "b_menu", "menu_main"))
    return t(u, "history_title"), m


def screen_favorites(u):
    with db() as c:
        rows = c.execute("SELECT id, prompt FROM favorites WHERE user_id=? ORDER BY id DESC LIMIT 10",
                         (u["user_id"],)).fetchall()
    if not rows:
        return t(u, "fav_empty"), kb_back(u)
    m = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        m.add(types.InlineKeyboardButton("⭐ " + r["prompt"][:40], callback_data=f"favsend_{r['id']}"))
    m.add(btn(u, "b_menu", "menu_main"))
    return t(u, "fav_title"), m


def screen_buy(u):
    if not CRYPTO_PAY_TOKEN:
        return t(u, "buy_off"), kb_back(u)
    m = types.InlineKeyboardMarkup(row_width=1)
    for item in list(PACKS) + ["sub"]:
        m.add(types.InlineKeyboardButton(item_label(u, item), callback_data=f"buy_{item}"))
    m.add(btn(u, "b_menu", "menu_main"))
    return t(u, "buy_title", days=SUB_DAYS), m


_bot_username = [None]


def bot_username():
    if not _bot_username[0]:
        _bot_username[0] = bot.get_me().username
    return _bot_username[0]


def screen_ref(u):
    link = f"https://t.me/{bot_username()}?start=ref_{u['user_id']}"
    return t(u, "ref_text", bonus=REF_BONUS, welcome=REF_WELCOME, refs=u["ref_count"], link=link), kb_back(u)


def send_menu(chat_id, u):
    text, markup = screen_main(u)
    if IMG_START:
        try:
            return bot.send_photo(chat_id, IMG_START, caption=text, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            log.warning("menu photo failed: %s", e)
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)


# ============================================================
#                  ЗАПУСК ГЕНЕРАЦИИ И ОЧЕРЕДЬ
# ============================================================
def start_generation(chat_id, tg_user, prompt, n=1, style=None, size=None, extra=None):
    """Проверяет запрос, списывает попытки и ставит задачу в очередь."""
    global pending_jobs
    u, _ = ensure_user(tg_user)
    uid = u["user_id"]
    if u["banned"]:
        return bot.send_message(chat_id, t(u, "banned"))
    prompt = prompt.strip()
    if len(prompt) < 3:
        return bot.send_message(chat_id, t(u, "too_short"))
    if len(prompt) > 500:
        return bot.send_message(chat_id, t(u, "too_long"))
    if is_blocked(prompt):
        return bot.send_message(chat_id, t(u, "blocked"))
    if not is_subscribed(uid):
        return bot.send_message(chat_id, t(u, "sub_required", ch=CHANNEL_USERNAME))

    with state_lock:
        wait = USER_COOLDOWN - (time.time() - last_request.get(uid, 0))
        if wait > 0:
            return bot.send_message(chat_id, t(u, "cooldown", s=int(wait) + 1))
        if uid in busy_users:
            return bot.send_message(chat_id, t(u, "busy"))
        if pending_jobs >= MAX_QUEUE:
            return bot.send_message(chat_id, t(u, "queue_full"))
        kinds = try_consume(uid, n)
        if not kinds:
            if n > 1:
                return bot.send_message(chat_id, t(u, "not_enough", n=n))
            return bot.send_message(chat_id, t(u, "limit_over", support=SUPPORT_USERNAME), parse_mode="HTML",
                                    reply_markup=kb_back(u))
        busy_users.add(uid)
        pending_jobs += 1
        position = pending_jobs
        last_request[uid] = time.time()

    use_style = style or u["style"]
    use_size = size or u["size"]
    if extra is None:
        extra = PRESETS[u["preset"]]["extra"] if u["preset"] in PRESETS else ""
    set_user_field(uid, "last_prompt", prompt)
    status = bot.send_message(chat_id, t(u, "queued", pos=position))
    job_queue.put({"uid": uid, "chat_id": chat_id, "prompt": prompt, "n": n, "kinds": kinds,
                   "style": use_style, "size": use_size, "extra": extra, "status_id": status.message_id})


def register_failure():
    global fail_streak, last_alert
    fail_streak += 1
    if fail_streak >= 3 and time.time() - last_alert > 1800:
        last_alert = time.time()
        notify_admin(f"⚠️ Генерация падает {fail_streak} раз подряд. Проверь логи Render и сервис картинок.")


def register_success():
    global fail_streak
    fail_streak = 0


def send_results(job, u, en, translated, enhanced, images):
    chat_id, uid = job["chat_id"], job["uid"]
    meta = {"prompt": job["prompt"], "style": job["style"], "size": job["size"],
            "extra": job["extra"], "author": uid}
    caption = (f"🖼 <b>{html.escape(job['prompt'][:200])}</b>\n"
               f"{lab(STYLES[job['style']], u)} · {lab(SIZES[job['size']], u)}")
    if translated or enhanced:
        caption += f"\n🌐 <i>{html.escape(en[:150])}</i>"
    keys = [store_result(img, meta) for img in images]

    if len(images) == 1:
        photo = BytesIO(images[0])
        photo.name = "image.jpg"
        msg = bot.send_photo(chat_id, photo, caption=caption, parse_mode="HTML", reply_markup=kb_result(u, keys[0]))
        results[keys[0]]["file_id"] = msg.photo[-1].file_id
    else:
        media = []
        for i, img in enumerate(images):
            ph = BytesIO(img)
            ph.name = f"image{i}.jpg"
            media.append(types.InputMediaPhoto(ph, caption=caption if i == 0 else None,
                                               parse_mode="HTML" if i == 0 else None))
        msgs = bot.send_media_group(chat_id, media)
        for k, m in zip(keys, msgs):
            results[k]["file_id"] = m.photo[-1].file_id
        bot.send_message(chat_id, "👆", reply_markup=kb_variants(u, keys))
    for _ in images:
        log_generation(uid, job["prompt"], job["style"], job["size"])


def process_job(job):
    global pending_jobs
    uid, chat_id, n, kinds = job["uid"], job["chat_id"], job["n"], job["kinds"]
    u = get_user(uid) or {"lang": "ru", "user_id": uid, "enhance": 1}
    try:
        try:
            en, translated, enhanced = build_prompt(job["prompt"], bool(u.get("enhance", 1)))
        except Exception:
            log.exception("prompt preparation failed")
            refund(uid, kinds)
            notify_user(chat_id, t(u, "translate_fail"))
            return
        if is_blocked(en):
            refund(uid, kinds)
            notify_user(chat_id, t(u, "blocked"))
            return

        done = []
        for _ in range(n):
            try:
                wait_gate()
                done.append(generate_image(en[:350] + job["extra"], job["style"], job["size"],
                                           random.randint(1, 10**9)))
            except Exception:
                log.exception("generation failed")
                break
        failed = n - len(done)
        if failed:
            refund(uid, kinds[len(done):])
        if not done:
            register_failure()
            notify_user(chat_id, t(u, "gen_fail"))
            return
        register_success()
        if failed:
            notify_user(chat_id, t(u, "partial", done=len(done), n=n))
        send_results(job, u, en, translated, enhanced, done)
    except Exception:
        log.exception("process_job crashed")
        try:
            refund(uid, kinds)
        except Exception:
            pass
        notify_user(chat_id, t(u, "gen_fail"))
    finally:
        with state_lock:
            busy_users.discard(uid)
            pending_jobs = max(pending_jobs - 1, 0)
        try:
            bot.delete_message(chat_id, job["status_id"])
        except Exception:
            pass


def worker_loop():
    while True:
        job = job_queue.get()
        try:
            process_job(job)
        except Exception:
            log.exception("worker error")
        finally:
            job_queue.task_done()


# ============================================================
#                         ОПЛАТА (Crypto Pay)
# ============================================================
def crypto_create_invoice(price_rub, description, order_id, asset):
    try:
        headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN, "Content-Type": "application/json"}
        data = {"currency_type": "fiat", "fiat": "RUB", "accepted_assets": asset, "amount": f"{price_rub:.2f}",
                "description": description, "payload": str(order_id), "expires_in": 3600}
        r = requests.post("https://pay.crypt.bot/api/createInvoice", headers=headers, json=data, timeout=10)
        j = r.json()
        if r.status_code == 200 and j.get("ok"):
            res = j["result"]
            return res.get("bot_invoice_url") or res.get("pay_url")
        log.error("crypto invoice error: %s", j)
    except Exception as e:
        log.error("crypto invoice exception: %s", e)
    return None


def process_paid_order(order_id):
    """Безопасна к повторным вызовам: начисляет только один раз."""
    if not set_order_status(order_id, "paid", ["pending"]):
        return
    order = get_order(order_id)
    if not order:
        return
    uid = order["user_id"]
    u = get_user(uid)
    if order["item"] == "sub":
        end = extend_subscription(uid)
        notify_user(uid, t(u, "paid_sub", date=end[:10]))
    elif order["item"] in PACKS:
        n = PACKS[order["item"]][0]
        add_credits(uid, n)
        notify_user(uid, t(u, "paid_pack", n=n))
    notify_admin(f"💰 Оплачен заказ #{order_id}: {order['item']}, {order['price']:.0f}₽, user {uid}")


# ============================================================
#                              КОМАНДЫ
# ============================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    try:
        ref = 0
        parts = message.text.split()
        if len(parts) > 1:
            arg = parts[1].replace("ref_", "")
            if arg.isdigit() and int(arg) != message.from_user.id and get_user(int(arg)):
                ref = int(arg)
        u, is_new = ensure_user(message.from_user, ref)
        if u["banned"]:
            return bot.send_message(message.chat.id, t(u, "banned"))
        if is_new and ref:
            add_credits(ref, REF_BONUS)
            add_credits(u["user_id"], REF_WELCOME)
            with db() as c:
                c.execute("UPDATE users SET ref_count = ref_count + 1 WHERE user_id=?", (ref,))
            notify_user(ref, t(get_user(ref), "ref_joined", n=REF_BONUS))
            u = get_user(u["user_id"])
            notify_user(u["user_id"], t(u, "ref_welcome", n=REF_WELCOME))
        send_menu(message.chat.id, u)
    except Exception:
        log.exception("ОШИБКА В /start")
        try:
            bot.send_message(message.chat.id, "⚠️ " + T["ru"]["start_error"])
        except Exception:
            pass


def user_for(message):
    u, _ = ensure_user(message.from_user)
    return None if u["banned"] else u


@bot.message_handler(commands=["ping"])
def cmd_ping(message):
    bot.send_message(message.chat.id, T["ru"]["ping"])


def send_screen(message, builder):
    u = user_for(message)
    if not u:
        return
    text, markup = builder(u)
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=markup)


@bot.message_handler(commands=["help"])
def cmd_help(message):
    send_screen(message, screen_help)


@bot.message_handler(commands=["history"])
def cmd_history(message):
    send_screen(message, screen_history)


@bot.message_handler(commands=["favorites"])
def cmd_favorites(message):
    send_screen(message, screen_favorites)


@bot.message_handler(commands=["lang", "settings"])
def cmd_lang(message):
    send_screen(message, screen_settings)


@bot.message_handler(commands=["buy"])
def cmd_buy(message):
    send_screen(message, screen_buy)


@bot.message_handler(commands=["promo"])
def cmd_promo(message):
    u = user_for(message)
    if not u:
        return
    parts = message.text.split()
    if len(parts) != 2:
        return bot.send_message(message.chat.id, t(u, "promo_usage"))
    n, status = apply_promo(u["user_id"], parts[1].upper())
    key = {"ok": "promo_ok", "used": "promo_used"}.get(status, "promo_nf")
    bot.send_message(message.chat.id, t(u, key, n=n))


# ---------- админ ----------
def admin_only(func):
    def wrapper(message):
        if message.from_user.id != ADMIN_ID:
            return
        try:
            func(message)
        except Exception as e:
            log.exception("admin cmd")
            bot.send_message(message.chat.id, f"Ошибка: {e}")
    return wrapper


def admin_markup():
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(types.InlineKeyboardButton("📊 Статистика", callback_data="adm_stats"),
          types.InlineKeyboardButton("🏆 Топ", callback_data="adm_top"))
    m.add(types.InlineKeyboardButton("🕘 Последние запросы", callback_data="adm_last"))
    return m


ADMIN_HELP = ("🛠 <b>Админ-панель</b>\n\n/give ID КОЛ-ВО — выдать генерации\n/ban ID · /unban ID\n"
              "/addpromo КОД КОЛ-ВО ИСПОЛЬЗОВАНИЙ — промокод на генерации\n"
              "/blockword слово · /unblockword слово — стоп-слова\n/broadcast ТЕКСТ")


@bot.message_handler(commands=["admin"])
@admin_only
def cmd_admin(message):
    bot.send_message(message.chat.id, ADMIN_HELP, parse_mode="HTML", reply_markup=admin_markup())


def admin_stats_text():
    with db() as c:
        users = first(c.execute("SELECT COUNT(*) FROM users"))
        total = first(c.execute("SELECT COUNT(*) FROM generations"))
        day = first(c.execute("SELECT COUNT(*) FROM generations WHERE created_at LIKE ?", (today_str() + "%",)))
        reports = first(c.execute("SELECT COUNT(*) FROM reports"))
        paid = c.execute("SELECT COUNT(*) AS n, COALESCE(SUM(price),0) AS s FROM orders WHERE status='paid'").fetchone()
        subs = first(c.execute("SELECT COUNT(*) FROM users WHERE sub_until > ?", (now_str(),)))
        queue_now = pending_jobs
    return (f"📊 <b>Статистика</b>\n\n👥 Пользователей: {users}\n🖼 Генераций всего: {total}\n📅 Сегодня: {day}\n"
            f"🚩 Жалоб: {reports}\n💰 Оплат: {paid['n']} на {paid['s']:.0f}₽\n♾ Активных подписок: {subs}\n"
            f"⏳ В очереди сейчас: {queue_now}")


@bot.message_handler(commands=["stats"])
@admin_only
def cmd_stats(message):
    bot.send_message(message.chat.id, admin_stats_text(), parse_mode="HTML")


@bot.message_handler(commands=["give"])
@admin_only
def cmd_give(message):
    _, uid, n = message.text.split()
    add_credits(int(uid), int(n))
    bot.send_message(message.chat.id, f"✅ Выдано {n} генераций пользователю {uid}")
    u = get_user(int(uid))
    if u:
        notify_user(int(uid), f"🎁 {n}")


@bot.message_handler(commands=["ban", "unban"])
@admin_only
def cmd_ban(message):
    cmd, uid = message.text.split()
    with db() as c:
        c.execute("UPDATE users SET banned=? WHERE user_id=?", (1 if cmd.startswith("/ban") else 0, int(uid)))
    bot.send_message(message.chat.id, "✅ Готово")


@bot.message_handler(commands=["addpromo"])
@admin_only
def cmd_addpromo(message):
    _, code, credits, uses = message.text.split()
    with db() as c:
        c.execute("INSERT INTO promos (code, credits, uses_left) VALUES (?,?,?) "
                  "ON CONFLICT (code) DO UPDATE SET credits=excluded.credits, uses_left=excluded.uses_left",
                  (code.upper(), int(credits), int(uses)))
    bot.send_message(message.chat.id, f"✅ Промокод {code.upper()}: +{credits} генераций, использований: {uses}")


@bot.message_handler(commands=["blockword", "unblockword"])
@admin_only
def cmd_blockword(message):
    cmd, _, word = message.text.partition(" ")
    word = word.strip().lower()
    if not word:
        return bot.send_message(message.chat.id, "Формат: /blockword слово")
    with db() as c:
        if cmd.startswith("/block"):
            c.execute("INSERT INTO blocked_words (word) VALUES (?) ON CONFLICT DO NOTHING", (word,))
            extra_block_words.add(word)
        else:
            c.execute("DELETE FROM blocked_words WHERE word=?", (word,))
            extra_block_words.discard(word)
    bot.send_message(message.chat.id, "✅ Готово")


@bot.message_handler(commands=["broadcast"])
@admin_only
def cmd_broadcast(message):
    text = message.text.partition(" ")[2].strip()
    if not text:
        return bot.send_message(message.chat.id, "Формат: /broadcast ТЕКСТ")

    def run():
        with db() as c:
            ids = [r["user_id"] for r in c.execute("SELECT user_id FROM users WHERE banned=0").fetchall()]
        ok = 0
        for uid in ids:
            try:
                bot.send_message(uid, text)
                ok += 1
            except Exception:
                pass
            time.sleep(0.05)
        bot.send_message(message.chat.id, f"📢 Отправлено: {ok}/{len(ids)}")
    threading.Thread(target=run, daemon=True).start()


# ---------- обычный текст = описание картинки ----------
@bot.message_handler(content_types=["text"])
def text_handler(message):
    text = (message.text or "").strip()
    if text.startswith("/"):
        u = user_for(message)
        return bot.send_message(message.chat.id, t(u, "unknown_cmd"))
    start_generation(message.chat.id, message.from_user, text)


# ============================================================
#                       ОБРАБОТКА КНОПОК
# ============================================================
def handle_menu(call, u, name):
    chat_id = call.message.chat.id
    screens = {"main": screen_main, "help": screen_help, "profile": screen_profile, "style": screen_style,
               "size": screen_size, "preset": screen_preset, "settings": screen_settings,
               "history": screen_history, "fav": screen_favorites, "buy": screen_buy, "ref": screen_ref}
    if name in screens:
        text, markup = screens[name](u)
        return edit_safe(call, text, markup)
    if name == "new":
        return send_menu(chat_id, u)
    if name == "promo":
        return edit_safe(call, t(u, "promo_hint"), kb_back(u))
    if name == "idea":
        return start_generation(chat_id, call.from_user, random.choice(IDEAS))


def handle_set(call, u, rest):
    uid = u["user_id"]
    if rest.startswith("style_"):
        key = rest[6:]
        if key in STYLES:
            set_user_field(uid, "style", key)
            set_user_field(uid, "preset", "")
            u = get_user(uid)
            text, markup = screen_style(u)
            edit_safe(call, text, markup)
            return lab(STYLES[key], u)
    elif rest.startswith("size_"):
        key = rest[5:]
        if key in SIZES:
            set_user_field(uid, "size", key)
            set_user_field(uid, "preset", "")
            u = get_user(uid)
            text, markup = screen_size(u)
            edit_safe(call, text, markup)
            return lab(SIZES[key], u)
    elif rest.startswith("preset_"):
        key = rest[7:]
        if key in PRESETS:
            set_user_field(uid, "preset", key)
            set_user_field(uid, "style", PRESETS[key]["style"])
            set_user_field(uid, "size", PRESETS[key]["size"])
        elif key == "none":
            set_user_field(uid, "preset", "")
        u = get_user(uid)
        text, markup = screen_preset(u)
        edit_safe(call, text, markup)
        return lab(PRESETS[key], u) if key in PRESETS else None
    elif rest.startswith("lang_"):
        lang = rest[5:]
        if lang in ("ru", "en"):
            set_user_field(uid, "lang", lang)
            u = get_user(uid)
            text, markup = screen_settings(u)
            edit_safe(call, text, markup)
            return t(u, "lang_set")
    return None


def handle_pay(call, u, rest):
    asset_key, _, item = rest.partition("_")
    if not CRYPTO_PAY_TOKEN or asset_key not in ("usdt", "ton") or (item != "sub" and item not in PACKS):
        return t(u, "pay_error")
    price = item_price(item)
    order_id = create_order(u["user_id"], item, price)
    asset = asset_key.upper()
    url = crypto_create_invoice(price, item_label(u, item), order_id, asset)
    if not url:
        set_order_status(order_id, "cancelled", ["pending"])
        return t(u, "pay_error")
    m = types.InlineKeyboardMarkup(row_width=1)
    m.add(types.InlineKeyboardButton(t(u, "pay_btn", asset=asset), url=url), btn(u, "b_menu", "menu_main"))
    edit_safe(call, t(u, "invoice", id=order_id, item=item_label(u, item), price=price, asset=asset), m)
    return None


@bot.callback_query_handler(func=lambda c: True)
def router(call):
    alert = None
    try:
        u, _ = ensure_user(call.from_user)
        if u["banned"]:
            return bot.answer_callback_query(call.id, t(u, "banned"), show_alert=True)
        data = call.data
        prefix, _, rest = data.partition("_")
        chat_id = call.message.chat.id

        if prefix == "menu":
            handle_menu(call, u, rest)
        elif prefix == "set":
            alert = handle_set(call, u, rest)
        elif prefix == "toggle" and rest == "enhance":
            set_user_field(u["user_id"], "enhance", 0 if u["enhance"] else 1)
            text, markup = screen_settings(get_user(u["user_id"]))
            edit_safe(call, text, markup)
        elif prefix in ("again", "var3"):
            meta = results.get(int(rest))
            if not meta:
                alert = t(u, "expired")
            else:
                start_generation(chat_id, call.from_user, meta["prompt"], n=3 if prefix == "var3" else 1,
                                 style=meta["style"], size=meta["size"], extra=meta["extra"])
        elif prefix == "doc":
            meta = results.get(int(rest))
            if not meta:
                alert = t(u, "expired")
            else:
                doc = BytesIO(meta["img"])
                doc.name = "image.jpg"
                bot.send_document(chat_id, doc)
        elif prefix == "fav":
            meta = results.get(int(rest))
            if not meta or not meta.get("file_id"):
                alert = t(u, "expired")
            else:
                with db() as c:
                    count = first(c.execute("SELECT COUNT(*) FROM favorites WHERE user_id=?", (u["user_id"],)))
                    if count >= 50:
                        alert = t(u, "fav_full")
                    else:
                        c.execute("INSERT INTO favorites (user_id, file_id, prompt, created_at) VALUES (?,?,?,?)",
                                  (u["user_id"], meta["file_id"], meta["prompt"][:300], now_str()))
                        alert = t(u, "fav_saved")
        elif prefix == "favsend":
            with db() as c:
                row = c.execute("SELECT file_id, prompt FROM favorites WHERE id=? AND user_id=?",
                                (int(rest), u["user_id"])).fetchone()
            if row:
                m = types.InlineKeyboardMarkup()
                m.add(types.InlineKeyboardButton(t(u, "b_del"), callback_data=f"favdel_{rest}"))
                bot.send_photo(chat_id, row["file_id"], caption="⭐ " + html.escape(row["prompt"][:200]),
                               parse_mode="HTML", reply_markup=m)
        elif prefix == "favdel":
            with db() as c:
                c.execute("DELETE FROM favorites WHERE id=? AND user_id=?", (int(rest), u["user_id"]))
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            alert = t(u, "fav_deleted")
        elif prefix == "hist":
            with db() as c:
                row = c.execute("SELECT prompt, style, size FROM generations WHERE id=? AND user_id=?",
                                (int(rest), u["user_id"])).fetchone()
            if row:
                st = row["style"] if row["style"] in STYLES else None
                sz = row["size"] if row["size"] in SIZES else None
                start_generation(chat_id, call.from_user, row["prompt"], style=st, size=sz, extra="")
        elif prefix == "rep":
            meta = results.get(int(rest))
            if not meta or not meta.get("file_id"):
                alert = t(u, "expired")
            else:
                with db() as c:
                    c.execute("INSERT INTO reports (reporter_id, author_id, prompt, created_at) VALUES (?,?,?,?)",
                              (u["user_id"], meta["author"], meta["prompt"][:300], now_str()))
                m = types.InlineKeyboardMarkup(row_width=2)
                m.add(types.InlineKeyboardButton("🚫 Забанить автора", callback_data=f"adm_ban_{meta['author']}"),
                      types.InlineKeyboardButton("✅ Ок", callback_data="adm_ok"))
                try:
                    bot.send_photo(ADMIN_ID, meta["file_id"], reply_markup=m,
                                   caption=f"🚩 Жалоба от {u['user_id']}\nАвтор: {meta['author']}\n"
                                           f"Запрос: {meta['prompt'][:300]}")
                except Exception as e:
                    log.warning("report to admin failed: %s", e)
                alert = t(u, "report_sent")
        elif prefix == "buy":
            if CRYPTO_PAY_TOKEN and (rest == "sub" or rest in PACKS):
                m = types.InlineKeyboardMarkup(row_width=2)
                m.add(types.InlineKeyboardButton("💎 USDT", callback_data=f"pay_usdt_{rest}"),
                      types.InlineKeyboardButton("💎 TON", callback_data=f"pay_ton_{rest}"))
                m.add(btn(u, "b_back", "menu_buy"))
                edit_safe(call, t(u, "choose_asset", item=item_label(u, rest), price=item_price(rest)), m)
        elif prefix == "pay":
            alert = handle_pay(call, u, rest)
        elif prefix == "adm" and call.from_user.id == ADMIN_ID:
            alert = handle_admin_callback(call, rest)
    except Exception:
        log.exception("router: %s", call.data)
        alert = T["ru"]["error"]
    try:
        bot.answer_callback_query(call.id, alert)
    except Exception:
        pass


def handle_admin_callback(call, rest):
    chat_id = call.message.chat.id
    if rest == "stats":
        bot.send_message(chat_id, admin_stats_text(), parse_mode="HTML")
    elif rest == "top":
        with db() as c:
            rows = c.execute("SELECT user_id, username, total_generated FROM users "
                             "ORDER BY total_generated DESC LIMIT 10").fetchall()
        lines = [f"{i}. {r['user_id']} @{r['username'] or '-'} — {r['total_generated']}" for i, r in enumerate(rows, 1)]
        bot.send_message(chat_id, "🏆 <b>Топ пользователей</b>\n\n" + "\n".join(lines), parse_mode="HTML")
    elif rest == "last":
        with db() as c:
            rows = c.execute("SELECT user_id, prompt, created_at FROM generations ORDER BY id DESC LIMIT 10").fetchall()
        lines = [f"{r['created_at'][11:16]} · {r['user_id']}: {html.escape(r['prompt'][:60])}" for r in rows]
        bot.send_message(chat_id, "🕘 <b>Последние запросы</b>\n\n" + "\n".join(lines or ["пока пусто"]),
                         parse_mode="HTML")
    elif rest.startswith("ban_"):
        with db() as c:
            c.execute("UPDATE users SET banned=1 WHERE user_id=?", (int(rest[4:]),))
        return "Автор забанен"
    elif rest == "ok":
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except Exception:
            pass
    return None


# ============================================================
#                         ВЕБХУКИ И ЗАПУСК
# ============================================================
@app.route(f"/{TOKEN}", methods=["POST"])
def telegram_webhook():
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != TG_SECRET:
        log.warning("Вебхук отклонён: неверный секретный заголовок")
        return "Forbidden", 403
    try:
        update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
        log.info("Получено обновление %s", update.update_id)
        bot.process_new_updates([update])
    except Exception:
        log.exception("ОШИБКА ОБРАБОТКИ ОБНОВЛЕНИЯ")
    return "OK", 200


@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def index():
    return "Bot is running", 200


@app.route("/crypto/webhook", methods=["POST"])
def crypto_webhook():
    if not CRYPTO_PAY_TOKEN:
        return "Forbidden", 403
    raw = request.get_data()
    signature = request.headers.get("crypto-pay-api-signature", "")
    secret = hashlib.sha256(CRYPTO_PAY_TOKEN.encode()).digest()
    expected = hmac.new(secret, raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return "Forbidden", 403
    update = request.get_json(silent=True) or {}
    if update.get("update_type") == "invoice_paid":
        try:
            order_id = int(update["payload"].get("payload", ""))
        except (ValueError, KeyError, TypeError, AttributeError):
            return "OK", 200
        threading.Thread(target=process_paid_order, args=(order_id,), daemon=True).start()
    return "OK", 200


def setup_webhook():
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_URL не задан — вебхук Telegram не установлен!")
        return
    bot.remove_webhook()
    bot.set_webhook(url=f"{WEBHOOK_URL}/{TOKEN}", secret_token=TG_SECRET)
    log.info("Webhook установлен")


def keep_alive_loop():
    """Пока сервис бодрствует, пингует сам себя, чтобы Render не усыплял его через 15 минут."""
    while True:
        time.sleep(600)
        try:
            requests.get(WEBHOOK_URL + "/health", timeout=20)
        except Exception as e:
            log.warning("keep-alive failed: %s", e)


def startup():
    init_db()
    reload_block_words()
    setup_webhook()
    for _ in range(WORKERS):
        threading.Thread(target=worker_loop, daemon=True).start()
    if KEEP_ALIVE and WEBHOOK_URL:
        threading.Thread(target=keep_alive_loop, daemon=True).start()
    log.info("Бот запущен (bot.py), БД: %s", "Postgres" if USE_PG else "SQLite")


startup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
