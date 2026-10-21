import os
import re
import html
import time
import random
import hashlib
import sqlite3
import logging
import threading
from io import BytesIO
from datetime import datetime
from contextlib import contextmanager
from urllib.parse import quote

import requests
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from flask import Flask, request

# ===================== НАСТРОЙКИ =====================
TOKEN = os.environ.get("TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")
DB_PATH = os.environ.get("DB_PATH", "imagebot.db")
IMAGE_API_KEY = os.environ.get("IMAGE_API_KEY", "")      # необязательно (ключ Pollinations)

SUPPORT_USERNAME = "coo_vernost"                          # поменяй на свой
CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "")  # например "@mychannel"; пусто = без проверки
IMG_START = os.environ.get("IMG_START", "")               # ссылка на картинку для меню; пусто = без картинки

DAILY_FREE = int(os.environ.get("DAILY_FREE", 5))         # бесплатных генераций в сутки
MIN_INTERVAL = float(os.environ.get("MIN_INTERVAL", 16))  # пауза между запросами к API (сек)
MAX_QUEUE = 10                                            # максимум задач в очереди

STYLES = {
    "none":      ("✨ Без стиля", ""),
    "realistic": ("📷 Реализм", ", photorealistic, highly detailed, natural lighting, 8k"),
    "anime":     ("🌸 Аниме", ", anime style, vibrant colors, detailed illustration"),
    "art":       ("🎨 Арт", ", digital painting, concept art, artstation, detailed"),
    "3d":        ("🧊 3D", ", 3d render, octane render, soft lighting, high detail"),
}
SIZES = {
    "square":    ("⬜ Квадрат", (1024, 1024)),
    "portrait":  ("📱 Вертикальный", (768, 1024)),
    "landscape": ("🖥 Горизонтальный", (1024, 768)),
}

# Защита от неподходящих картинок (передаётся нейросети вместе с каждым запросом)
SAFE_SUFFIX = ", fully clothed, family friendly, safe for work"
NEGATIVE_PROMPT = "nsfw, nude, naked, topless, nudity, sexual, explicit, erotic, underwear, gore, violence, text, watermark"

# Базовый фильтр запросов. Это минимум, а не полноценная модерация.
BLOCKED = re.compile(r"(loli|shota|child\s*porn|детск\w*\s*порн|порн|nsfw|nude|naked|гол[аы][яй]\b)", re.I)

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
queue_size = 0


# ===================== БАЗА ДАННЫХ =====================
@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today():
    return datetime.now().strftime("%Y-%m-%d")


def init_db():
    with db() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT,
            style TEXT DEFAULT 'none', size TEXT DEFAULT 'square',
            used_date TEXT DEFAULT '', used_today INTEGER DEFAULT 0,
            credits INTEGER DEFAULT 0, total_generated INTEGER DEFAULT 0,
            last_prompt TEXT DEFAULT '', banned INTEGER DEFAULT 0, created_at TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS generations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            prompt TEXT, style TEXT, size TEXT, created_at TEXT)""")


def add_user(user_id, username):
    with db() as c:
        cur = c.execute("INSERT OR IGNORE INTO users (user_id, username, created_at) VALUES (?,?,?)",
                        (user_id, username, now()))
        if cur.rowcount == 0:
            c.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))


def get_user_data(user_id):
    with db() as c:
        row = c.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None


def prepare(user):
    """Регистрирует пользователя. False — если забанен."""
    add_user(user.id, user.username or "")
    u = get_user_data(user.id)
    return not (u and u["banned"] == 1)


def set_user_field(user_id, field, value):
    assert field in ("style", "size", "last_prompt")
    with db() as c:
        c.execute(f"UPDATE users SET {field}=? WHERE user_id=?", (value, user_id))


def free_left(u):
    used = u["used_today"] if u["used_date"] == today() else 0
    return max(DAILY_FREE - used, 0)


def try_consume(user_id):
    """Списывает одну генерацию. Возвращает 'free' | 'credit' | None."""
    with db() as c:
        u = c.execute("SELECT used_date, used_today, credits FROM users WHERE user_id=?", (user_id,)).fetchone()
        used = u["used_today"] if u["used_date"] == today() else 0
        if used < DAILY_FREE:
            c.execute("UPDATE users SET used_date=?, used_today=? WHERE user_id=?", (today(), used + 1, user_id))
            return "free"
        if u["credits"] > 0:
            c.execute("UPDATE users SET credits = credits - 1 WHERE user_id=?", (user_id,))
            return "credit"
    return None


def refund_consume(user_id, kind):
    with db() as c:
        if kind == "free":
            c.execute("UPDATE users SET used_today = MAX(used_today - 1, 0) WHERE user_id=?", (user_id,))
        else:
            c.execute("UPDATE users SET credits = credits + 1 WHERE user_id=?", (user_id,))


def log_generation(user_id, prompt, style, size):
    with db() as c:
        c.execute("INSERT INTO generations (user_id, prompt, style, size, created_at) VALUES (?,?,?,?,?)",
                  (user_id, prompt, style, size, now()))
        c.execute("UPDATE users SET total_generated = total_generated + 1 WHERE user_id=?", (user_id,))


# ===================== ВСПОМОГАТЕЛЬНЫЕ =====================
def notify_user(user_id, text, markup=None):
    try:
        bot.send_message(user_id, text, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        log.warning("notify_user %s: %s", user_id, e)


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


WELCOME = ("🎨 <b>Нейро-художник</b>\n\n"
           "Опишите картинку словами, и я её нарисую.\n"
           "Например: <i>космонавт верхом на лошади, закат, масляная живопись</i>\n\n"
           "💡 Пишите коротко и по делу, лучше на английском: одна сцена, один-два объекта. "
           "Надписи на картинке и конкретные персонажи из фильмов получаются плохо.")


def send_menu(chat_id):
    u = get_user_data(chat_id)
    text = WELCOME
    if u:
        text += f"\n\n🎁 Бесплатных генераций сегодня: <b>{free_left(u)}</b>"
    if IMG_START:
        try:
            return bot.send_photo(chat_id, IMG_START, caption=text, parse_mode="HTML", reply_markup=kb_main())
        except Exception as e:
            log.warning("menu photo failed: %s", e)
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb_main())


# ===================== ГЕНЕРАЦИЯ КАРТИНОК =====================
# Вся работа с нейросетью — в одной функции. Для смены сервиса меняйте только её.
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
    """Возвращает байты картинки или бросает исключение."""
    full = prompt + STYLES[style_key][1] + SAFE_SUFFIX
    width, height = SIZES[size_key][1]
    url = f"https://image.pollinations.ai/prompt/{quote(full, safe='')}"
    params = {"width": width, "height": height, "seed": seed, "nologo": "true",
              "safe": "true", "negative_prompt": NEGATIVE_PROMPT}
    headers = {"Authorization": f"Bearer {IMAGE_API_KEY}"} if IMAGE_API_KEY else {}
    r = requests.get(url, params=params, headers=headers, timeout=120)
    ctype = r.headers.get("content-type", "")
    if r.status_code != 200 or not ctype.startswith("image/") or len(r.content) < 2000:
        raise RuntimeError(f"image api: {r.status_code} {ctype} {r.text[:200] if 'text' in ctype or 'json' in ctype else ''}")
    return r.content


def run_generation(user_id, chat_id, prompt, kind, status_id):
    global queue_size
    try:
        u = get_user_data(user_id)
        wait_gate()
        img = generate_image(prompt, u["style"], u["size"], random.randint(1, 10**9))
        photo = BytesIO(img)
        photo.name = "image.jpg"
        caption = (f"🖼 <b>{html.escape(prompt[:200])}</b>\n"
                   f"{STYLES[u['style']][0]} · {SIZES[u['size']][0]}")
        bot.send_photo(chat_id, photo, caption=caption, parse_mode="HTML", reply_markup=kb_result())
        log_generation(user_id, prompt, u["style"], u["size"])
    except Exception:
        log.exception("generation failed")
        refund_consume(user_id, kind)
        notify_user(chat_id, "⚠️ Не удалось создать картинку. Попытка не списана, попробуйте ещё раз чуть позже.")
    finally:
        with state_lock:
            busy_users.discard(user_id)
            queue_size = max(queue_size - 1, 0)
        try:
            bot.delete_message(chat_id, status_id)
        except Exception:
            pass


def start_generation(chat_id, user, prompt):
    global queue_size
    uid = user.id
    if not prepare(user):
        return bot.send_message(chat_id, "🚫 Вы забанены.")
    prompt = prompt.strip()
    if len(prompt) < 3:
        return bot.send_message(chat_id, "Опишите картинку подробнее (минимум 3 символа).")
    if len(prompt) > 500:
        return bot.send_message(chat_id, "Описание слишком длинное (максимум 500 символов).")
    if BLOCKED.search(prompt):
        return bot.send_message(chat_id, "🚫 Такой запрос запрещён правилами бота.")
    if not is_subscribed(uid):
        return bot.send_message(chat_id, f"Для использования бота подпишитесь на канал {CHANNEL_USERNAME} и повторите запрос.")

    with state_lock:
        if uid in busy_users:
            return bot.send_message(chat_id, "⏳ Дождитесь завершения предыдущей генерации.")
        if queue_size >= MAX_QUEUE:
            return bot.send_message(chat_id, "😔 Сейчас слишком много запросов. Попробуйте через минуту.")
        kind = try_consume(uid)
        if not kind:
            return bot.send_message(chat_id,
                                    f"❌ Бесплатные генерации на сегодня закончились.\nЛимит обновится завтра. Вопросы: @{SUPPORT_USERNAME}")
        busy_users.add(uid)
        queue_size += 1
        position = queue_size

    set_user_field(uid, "last_prompt", prompt)
    status = bot.send_message(chat_id, f"⏳ Рисую... Это займёт до минуты. В очереди: {position}")
    threading.Thread(target=run_generation, args=(uid, chat_id, prompt, kind, status.message_id), daemon=True).start()


# ===================== КЛАВИАТУРЫ =====================
def kb_main():
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(types.InlineKeyboardButton("🎨 Стиль", callback_data="menu_style"),
          types.InlineKeyboardButton("📐 Формат", callback_data="menu_size"))
    m.add(types.InlineKeyboardButton("👤 Профиль", callback_data="menu_profile"),
          types.InlineKeyboardButton("❓ Помощь", callback_data="menu_help"))
    m.add(types.InlineKeyboardButton("🛠 Тех поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"))
    return m


def kb_back():
    m = types.InlineKeyboardMarkup()
    m.add(types.InlineKeyboardButton("🏠 В меню", callback_data="menu_main"))
    return m


def kb_result():
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(types.InlineKeyboardButton("🔄 Ещё вариант", callback_data="again"),
          types.InlineKeyboardButton("🏠 Меню", callback_data="menu_new"))
    return m


def kb_choice(options, current, prefix):
    m = types.InlineKeyboardMarkup(row_width=2)
    for key, val in options.items():
        mark = "✅ " if key == current else ""
        m.add(types.InlineKeyboardButton(mark + val[0], callback_data=f"{prefix}_{key}"))
    m.add(types.InlineKeyboardButton("🏠 В меню", callback_data="menu_main"))
    return m


# ===================== ОБРАБОТЧИКИ =====================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    try:
        if not prepare(message.from_user):
            return bot.send_message(message.chat.id, "🚫 Вы забанены.")
        send_menu(message.chat.id)
    except Exception:
        log.exception("ОШИБКА В /start")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при запуске, попробуйте ещё раз позже.")
        except Exception:
            pass


@bot.message_handler(commands=["ping"])
def cmd_ping(message):
    bot.send_message(message.chat.id, "pong ✅ бот работает")


HELP_TEXT = ("❓ <b>Помощь</b>\n\n"
             "• Просто отправьте текстовое описание — бот нарисует картинку.\n"
             "• «Стиль» и «Формат» меняют внешний вид результата.\n"
             "• «Ещё вариант» — та же идея, но с другим результатом.\n"
             f"• Бесплатно {DAILY_FREE} генераций в сутки.\n"
             "• Не принимаются запросы 18+ и запрещённый контент.")


def handle_menu(call, data):
    uid = call.from_user.id
    u = get_user_data(uid)
    if data == "menu_main":
        edit_safe(call, WELCOME + f"\n\n🎁 Бесплатных генераций сегодня: <b>{free_left(u)}</b>", kb_main())
    elif data == "menu_new":
        send_menu(call.message.chat.id)
    elif data == "menu_help":
        edit_safe(call, HELP_TEXT, kb_back())
    elif data == "menu_profile":
        edit_safe(call,
                  f"👤 <b>Профиль</b>\n\n🆔 ID: <code>{uid}</code>\n"
                  f"🎁 Бесплатных сегодня: {free_left(u)} из {DAILY_FREE}\n"
                  f"💳 Дополнительных генераций: {u['credits']}\n"
                  f"🖼 Всего создано: {u['total_generated']}\n"
                  f"🎨 Стиль: {STYLES[u['style']][0]}\n📐 Формат: {SIZES[u['size']][0]}", kb_back())
    elif data == "menu_style":
        edit_safe(call, "🎨 <b>Выберите стиль:</b>", kb_choice(STYLES, u["style"], "set_style"))
    elif data == "menu_size":
        edit_safe(call, "📐 <b>Выберите формат:</b>", kb_choice(SIZES, u["size"], "set_size"))


@bot.callback_query_handler(func=lambda c: True)
def router(call):
    alert = None
    try:
        if not prepare(call.from_user):
            return bot.answer_callback_query(call.id, "🚫 Вы забанены.", show_alert=True)
        data = call.data
        uid = call.from_user.id

        if data.startswith("menu_"):
            handle_menu(call, data)
        elif data.startswith("set_style_"):
            key = data[len("set_style_"):]
            if key in STYLES:
                set_user_field(uid, "style", key)
                edit_safe(call, "🎨 <b>Выберите стиль:</b>", kb_choice(STYLES, key, "set_style"))
                alert = f"Стиль: {STYLES[key][0]}"
        elif data.startswith("set_size_"):
            key = data[len("set_size_"):]
            if key in SIZES:
                set_user_field(uid, "size", key)
                edit_safe(call, "📐 <b>Выберите формат:</b>", kb_choice(SIZES, key, "set_size"))
                alert = f"Формат: {SIZES[key][0]}"
        elif data == "again":
            u = get_user_data(uid)
            if not u["last_prompt"]:
                alert = "Сначала отправьте описание."
            else:
                start_generation(call.message.chat.id, call.from_user, u["last_prompt"])
    except Exception:
        log.exception("router: %s", call.data)
        alert = "⚠️ Произошла ошибка, попробуйте ещё раз."
    try:
        bot.answer_callback_query(call.id, alert)
    except Exception:
        pass


# ----- админ -----
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


@bot.message_handler(commands=["admin", "stats"])
@admin_only
def cmd_stats(message):
    with db() as c:
        users = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total = c.execute("SELECT COUNT(*) FROM generations").fetchone()[0]
        day = c.execute("SELECT COUNT(*) FROM generations WHERE created_at LIKE ?", (today() + "%",)).fetchone()[0]
    bot.send_message(message.chat.id,
                     f"📊 <b>Статистика</b>\n\n👥 Пользователей: {users}\n🖼 Генераций всего: {total}\n"
                     f"📅 Сегодня: {day}\n\n<b>Команды:</b>\n/give ID КОЛ-ВО — выдать генерации\n"
                     "/ban ID · /unban ID\n/broadcast ТЕКСТ", parse_mode="HTML")


@bot.message_handler(commands=["give"])
@admin_only
def cmd_give(message):
    _, uid, n = message.text.split()
    with db() as c:
        c.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (int(n), int(uid)))
    bot.send_message(message.chat.id, f"✅ Выдано {n} генераций пользователю {uid}")
    notify_user(int(uid), f"🎁 Вам начислено {n} генераций!")


@bot.message_handler(commands=["ban", "unban"])
@admin_only
def cmd_ban(message):
    cmd, uid = message.text.split()
    with db() as c:
        c.execute("UPDATE users SET banned=? WHERE user_id=?", (1 if cmd.startswith("/ban") else 0, int(uid)))
    bot.send_message(message.chat.id, "✅ Готово")


@bot.message_handler(commands=["broadcast"])
@admin_only
def cmd_broadcast(message):
    text = message.text.partition(" ")[2].strip()
    if not text:
        return bot.send_message(message.chat.id, "Формат: /broadcast ТЕКСТ")

    def run():
        with db() as c:
            ids = [r[0] for r in c.execute("SELECT user_id FROM users WHERE banned=0")]
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


# ----- текст = описание картинки -----
@bot.message_handler(content_types=["text"])
def text_handler(message):
    text = (message.text or "").strip()
    if text.startswith("/"):
        return bot.send_message(message.chat.id, "Неизвестная команда. Просто отправьте описание картинки текстом.")
    start_generation(message.chat.id, message.from_user, text)


# ===================== ВЕБХУК =====================
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
def index():
    return "Bot is running", 200


def setup_webhook():
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_URL не задан — вебхук Telegram не установлен!")
        return
    bot.remove_webhook()
    bot.set_webhook(url=f"{WEBHOOK_URL.rstrip('/')}/{TOKEN}", secret_token=TG_SECRET)
    log.info("Webhook установлен")


init_db()
setup_webhook()
log.info("Бот запущен (bot.py)")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
