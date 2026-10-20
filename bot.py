import os
import telebot
from telebot import types
from flask import Flask, request
import sqlite3
from datetime import datetime
import csv
import io
import threading
import time
import requests
from io import BytesIO
import hmac
import hashlib

# ===== НАСТРОЙКИ =====
TOKEN = os.environ.get("TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
SUPPORT_USERNAME = "coo_vernost"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
CHANNEL_USERNAME = "@defferent_shop"
REF_BONUS = 5
CRYPTO_PAY_TOKEN = os.environ.get("CRYPTO_PAY_TOKEN")
KRAB_API_KEY = os.environ.get("KRAB_API_KEY")
KRAB_WEBHOOK_SECRET = os.environ.get("KRAB_WEBHOOK_SECRET")
KRAB_API_URL = "https://api.krab.gg/v1"

IMG_START = "https://raw.githubusercontent.com/pidorasgejvork-gif/Reposut/refs/heads/main/Screenshot_20261012_231702_com_lemon_lvoverseas_EditActivity.jpg"
IMG_ADMIN = "https://raw.githubusercontent.com/pidorasgejvork-gif/Reposut/refs/heads/main/Screenshot_20261013_015539_com_lemon_lvoverseas_EditActivity.jpg"

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

STAR_RATE = 1.4
PRICES = [(50, 50*STAR_RATE), (100, 100*STAR_RATE), (150, 150*STAR_RATE), (250, 250*STAR_RATE), (500, 500*STAR_RATE), (1000, 1000*STAR_RATE), (2500, 2500*STAR_RATE), (5000, 5000*STAR_RATE)]
PREMIUM_PRICES = {"3": 1200, "6": 1600, "12": 2800}

premium_data = {}

# ===== ВСПОМОГАТЕЛЬНЫЕ =====
def send_typing(chat_id):
    try: bot.send_chat_action(chat_id, "typing")
    except: pass

def send_photo_safe(chat_id, photo_url, caption, reply_markup=None):
    try:
        response = requests.get(photo_url, timeout=10)
        photo = BytesIO(response.content)
        photo.name = 'image.jpg'
        bot.send_photo(chat_id, photo=photo, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
    except:
        bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=reply_markup)

# ===== БАЗА =====
def get_conn(): return sqlite3.connect("users.db")

def init_db():
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0, discount INTEGER DEFAULT 0, total_spent REAL DEFAULT 0, total_purchases INTEGER DEFAULT 0, referrer_id INTEGER DEFAULT 0, ref_count INTEGER DEFAULT 0, banned INTEGER DEFAULT 0, created_at TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER, price REAL, status TEXT, created_at TEXT, krab_order_id TEXT, payment_status TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, discount INTEGER, uses_left INTEGER)")
    conn.commit(); cursor.close(); conn.close()

def add_user(user_id, username):
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute("INSERT INTO users (user_id, username, balance, created_at) VALUES (?, ?, 0, ?) ON CONFLICT(user_id) DO UPDATE SET username = excluded.username", (user_id, username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit(); cursor.close(); conn.close()

def get_user_data(user_id):
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone(); cursor.close(); conn.close()
    if row:
        return {"user_id": row[0], "username": row[1], "balance": row[2], "discount": row[3], "total_spent": row[4], "total_purchases": row[5], "referrer_id": row[6], "ref_count": row[7], "banned": row[8]}
    return None

def add_balance(user_id, amount):
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit(); cursor.close(); conn.close()

def apply_promo(user_id, code):
    conn = get_conn(); cursor = conn.cursor()
    cursor.execute("SELECT discount, uses_left FROM promos WHERE code = ?", (code,))
    row = cursor.fetchone()
    if row and row[1] > 0:
        cursor.execute("UPDATE promos SET uses_left = uses_left - 1 WHERE code = ?", (code,))
        cursor.execute("UPDATE users SET discount = ? WHERE user_id = ?", (row[0], user_id))
        conn.commit(); cursor.close(); conn.close()
        return row[0]
    cursor.close(); conn.close()
    return None

def is_banned(user_id):
    user = get_user_data(user_id)
    return user["banned"] == 1 if user else False

# ===== CRYPTO PAY =====
def crypto_create_invoice(amount_usdt, description, payload, asset="USDT"):
    try:
        headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN, "Content-Type": "application/json"}
        data = {"asset": asset, "amount": str(amount_usdt), "description": description, "payload": payload}
        response = requests.post("https://pay.crypt.bot/api/createInvoice", headers=headers, json=data, timeout=10)
        if response.status_code == 200 and response.json().get("ok"): return response.json()["result"]["pay_url"]
        return None
    except: return None

# ===== KRAB.GG =====
def krab_create_stars_order(username, quantity, local_order_id):
    try:
        headers = {"Authorization": f"Bearer {KRAB_API_KEY}", "Content-Type": "application/json"}
        payload = {"type": "stars", "qty": quantity, "username": username.replace("@", ""), "orderId": str(local_order_id)}
        response = requests.post(f"{KRAB_API_URL}/orders", json=payload, headers=headers, timeout=15)
        if response.status_code == 200: return response.json()
        return None
    except requests.exceptions.Timeout: return "TIMEOUT"
    except: return None

def krab_create_premium_order(username, months, local_order_id):
    try:
        headers = {"Authorization": f"Bearer {KRAB_API_KEY}", "Content-Type": "application/json"}
        payload = {"type": "premium", "months": months, "username": username.replace("@", ""), "orderId": str(local_order_id)}
        response = requests.post(f"{KRAB_API_URL}/orders", json=payload, headers=headers, timeout=15)
        if response.status_code == 200: return response.json()
        return None
    except requests.exceptions.Timeout: return "TIMEOUT"
    except: return None

# ===== КЛАВИАТУРЫ =====
def get_start_inline():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton("👤 Профиль", callback_data="menu_profile"), types.InlineKeyboardButton("❓ Помощь", callback_data="menu_help"))
    markup.add(types.InlineKeyboardButton("⭐ Купить звёзды", callback_data="menu_pay"), types.InlineKeyboardButton("💎 Купить Premium", callback_data="menu_premium"))
    markup.add(types.InlineKeyboardButton("💰 Баланс", callback_data="menu_balance"), types.InlineKeyboardButton("🎁 Рефералы", callback_data="menu_ref"))
    markup.add(types.InlineKeyboardButton("🎟 Промокод", callback_data="menu_promo"), types.InlineKeyboardButton("⭐ Отзывы", callback_data="menu_reviews"))
    markup.add(types.InlineKeyboardButton("🛠 Тех поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"))
    return markup

def get_back_to_menu_inline():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="menu_main"))
    return markup

def get_price_inline(user_id):
    user = get_user_data(user_id)
    discount = user["discount"] if user else 0
    markup = types.InlineKeyboardMarkup(row_width=2)
    for amount, price in PRICES:
        final_price = price * (1 - discount / 100)
        markup.add(types.InlineKeyboardButton(f"⭐ {amount} — {final_price:.0f}₽", callback_data=f"buy_{amount}"))
    markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="menu_main"))
    return markup

def get_stars_pay_inline(amount):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("💎 USDT", callback_data=f"buy_pay_usdt_{amount}"))
    markup.add(types.InlineKeyboardButton("💎 TON", callback_data=f"buy_pay_ton_{amount}"))
    markup.add(types.InlineKeyboardButton("💳 Списать с баланса", callback_data=f"buy_pay_balance_{amount}"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="menu_pay"))
    return markup

def get_premium_recipient_inline():
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("🎁 Себе", callback_data="prem_self"))
    markup.add(types.InlineKeyboardButton("🎁 Другому", callback_data="prem_other"))
    markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="menu_main"))
    return markup

def get_premium_period_inline():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton("⭐ 3 месяца", callback_data="prem_3"))
    markup.add(types.InlineKeyboardButton("⭐ 6 месяцев", callback_data="prem_6"))
    markup.add(types.InlineKeyboardButton("⭐ 12 месяцев", callback_data="prem_12"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="prem_back"))
    return markup

def get_premium_pay_inline():
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("💎 USDT", callback_data="prem_pay_usdt"))
    markup.add(types.InlineKeyboardButton("💎 TON", callback_data="prem_pay_ton"))
    markup.add(types.InlineKeyboardButton("💳 Списать с баланса", callback_data="prem_pay_balance"))
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="prem_back"))
    return markup

# ===== КОМАНДЫ =====
@bot.message_handler(commands=['start'])
def start(message):
    try:
        if is_banned(message.from_user.id): return bot.send_message(message.chat.id, "🚫 Вы забанены.")
        add_user(message.from_user.id, message.from_user.username or "без_ника")
        caption = ("🌟 <b>Добро пожаловать в Defferent Shop!</b>\n\n⚡️ <b>У нас ты найдёшь Telegram Stars, Premium-подписки.</b>\n\n💎 <b>Выбирай, что нужно, и получай сразу!</b>")
        send_typing(message.chat.id)
        send_photo_safe(message.chat.id, IMG_START, caption, get_start_inline())
    except Exception as e: print(f"ОШИБКА start: {e}")

@bot.message_handler(commands=['admin'])
def admin_cmd(message):
    if message.from_user.id != ADMIN_ID: return
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("📊 Статистика", "🏠 В меню")
    send_photo_safe(message.chat.id, IMG_ADMIN, "🛠 <b>Админ-панель</b>", markup)

@bot.message_handler(commands=['give'])
def give_cmd(message):
    if message.from_user.id != ADMIN_ID: return
    try:
        _, user_id, amount = message.text.split()
        add_balance(int(user_id), float(amount))
        bot.send_message(message.chat.id, f"✅ Начислено {amount}₽ пользователю {user_id}")
    except: bot.send_message(message.chat.id, "Формат: /give ID СУММА")

@bot.message_handler(commands=['promo'])
def promo_cmd(message):
    try:
        _, code = message.text.split()
        discount = apply_promo(message.from_user.id, code.upper())
        if discount: bot.send_message(message.chat.id, f"🎟 Промокод активирован! Скидка: {discount}%")
        else: bot.send_message(message.chat.id, "❌ Промокод не найден.")
    except: bot.send_message(message.chat.id, "Формат: /promo КОД")

@bot.message_handler(content_types=['text'])
def text_handler(message):
    text = message.text
    if text == "📊 Статистика" and message.from_user.id == ADMIN_ID:
        bot.send_message(message.chat.id, "📊 Статистика в разработке.")
    elif text == "🏠 В меню":
        bot.send_message(message.chat.id, "Главное меню", reply_markup=types.ReplyKeyboardRemove())

# ===== INLINE МЕНЮ =====
@bot.callback_query_handler(func=lambda call: call.data in ["menu_main", "menu_profile", "menu_help", "menu_pay", "menu_premium", "menu_balance", "menu_ref", "menu_promo", "menu_reviews"])
def inline_menu(call):
    try:
        if call.data == "menu_main":
            caption = "🌟 <b>Главное меню</b>\n\nВыбери действие:"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=caption, parse_mode="HTML", reply_markup=get_start_inline())
        elif call.data == "menu_profile":
            user = get_user_data(call.from_user.id)
            text = f"👤 <b>Профиль</b>\n\n🆔 ID: <code>{call.from_user.id}</code>\n💰 Баланс: {user['balance']:.2f}₽\n💸 Потрачено: {user['total_spent']:.2f}₽"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_back_to_menu_inline())
        elif call.data == "menu_pay":
            text = "💎 <b>Прайс-лист:</b>\n\nВыберите количество звёзд:"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_price_inline(call.from_user.id))
        elif call.data == "menu_premium":
            text = "💎 <b>Покупка Premium</b>\n\nВыберите получателя:"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_premium_recipient_inline())
        elif call.data == "menu_balance":
            user = get_user_data(call.from_user.id)
            text = f"💰 <b>Баланс:</b> {user['balance']:.2f}₽\n\nПополнить: <code>/topup 500</code>"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_back_to_menu_inline())
        elif call.data == "menu_ref":
            bot_username = bot.get_me().username
            link = f"https://t.me/{bot_username}?start={call.from_user.id}"
            user = get_user_data(call.from_user.id)
            text = f"🎁 <b>Реферальная система</b>\n\nПриглашай друзей и получай <b>{REF_BONUS}₽</b> за каждого!\n\n👥 Приглашено: {user['ref_count']}\n\n🔗 Ваша ссылка:\n<code>{link}</code>"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_back_to_menu_inline())
        elif call.data == "menu_promo":
            text = "🎟 <b>Введите промокод:</b>\n\nНапример: <code>/promo SALE10</code>"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_back_to_menu_inline())
        elif call.data == "menu_reviews":
            text = "⭐ <b>Оставьте отзыв:</b>\n\nНапишите текст отзыва в чат."
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_back_to_menu_inline())
        bot.answer_callback_query(call.id)
    except Exception as e: print(f"ОШИБКА inline_menu: {e}")

# ===== ПОКУПКА ЗВЁЗД =====
@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_") and not call.data.startswith("buy_pay_"))
def buy_callback(call):
    amount = int(call.data.split("_")[1])
    user = get_user_data(call.from_user.id)
    discount = user["discount"] if user else 0
    price = amount * STAR_RATE * (1 - discount / 100)
    text = f"⭐ <b>Покупка {amount} звёзд</b>\n💵 К оплате: {price:.0f}₽\nВыберите способ:"
    bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_stars_pay_inline(amount))
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_pay_balance_"))
def buy_pay_balance(call):
    amount = int(call.data.split("_")[3])
    user = get_user_data(call.from_user.id)
    discount = user["discount"] if user else 0
    price = amount * STAR_RATE * (1 - discount / 100)
    if user["balance"] < price: return bot.answer_callback_query(call.id, "❌ Недостаточно средств!", show_alert=True)
    
    add_balance(call.from_user.id, -price)
    bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="⏳ Создаем заказ...")
    
    username = call.from_user.username or str(call.from_user.id)
    result = krab_create_stars_order(username, amount, int(time.time()))
    
    if result == "TIMEOUT":
        add_balance(call.from_user.id, price)
        return bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="⚠️ Поставщик не отвечает. Деньги возвращены.")
    
    if result:
        bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=f"✅ <b>Заказ оплачен!</b>\n⭐ {amount} звёзд\n⏳ Ожидайте зачисления.")
    else:
        add_balance(call.from_user.id, price)
        bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="⚠️ Ошибка. Деньги возвращены.")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_pay_usdt_") or call.data.startswith("buy_pay_ton_"))
def buy_pay_crypto(call):
    parts = call.data.split("_")
    asset = "USDT" if parts[2] == "usdt" else "TON"
    amount = int(parts[3])
    user = get_user_data(call.from_user.id)
    discount = user["discount"] if user else 0
    price = amount * STAR_RATE * (1 - discount / 100)
    amount_usdt = round(price / 90, 2)
    pay_url = crypto_create_invoice(amount_usdt, f"{amount} звёзд", f"{call.from_user.id}:stars:{amount}:{price}", asset=asset)
    if pay_url:
        markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton(f"💎 Оплатить {asset}", url=pay_url))
        bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=f"💳 Счет на {amount_usdt} {asset} создан.", reply_markup=markup)
    else:
        bot.answer_callback_query(call.id, "❌ Ошибка создания счета.", show_alert=True)

# ===== PREMIUM =====
@bot.callback_query_handler(func=lambda call: call.data.startswith("prem_"))
def premium_callback(call):
    try:
        if call.data == "prem_self":
            premium_data[call.from_user.id] = {"recipient": f"@{call.from_user.username or call.from_user.id}"}
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="💎 <b>Покупка Premium</b>\n\nВыберите срок:", parse_mode="HTML", reply_markup=get_premium_period_inline())
        elif call.data == "prem_other":
            bot.answer_callback_query(call.id, "Введите @username в чат.", show_alert=True)
        elif call.data == "prem_back":
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="💎 Покупка Premium\nВыберите получателя:", reply_markup=get_premium_recipient_inline())
        elif call.data.startswith("prem_") and call.data[5:].isdigit():
            months = int(call.data[5:])
            price = PREMIUM_PRICES[str(months)]
            if call.from_user.id not in premium_data:
                premium_data[call.from_user.id] = {"recipient": f"@{call.from_user.username or call.from_user.id}"}
            premium_data[call.from_user.id]["months"] = months
            premium_data[call.from_user.id]["price"] = price
            text = f"💎 <b>Premium {months} мес.</b>\n💵 Цена: {price}₽\n\nВыберите способ оплаты:"
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=text, parse_mode="HTML", reply_markup=get_premium_pay_inline())
        elif call.data == "prem_pay_balance":
            if call.from_user.id not in premium_data or "months" not in premium_data[call.from_user.id]:
                return bot.answer_callback_query(call.id, "⚠️ Выберите срок сначала.", show_alert=True)
            months = premium_data[call.from_user.id]["months"]
            price = premium_data[call.from_user.id]["price"]
            recipient = premium_data[call.from_user.id]["recipient"]
            user = get_user_data(call.from_user.id)
            if user["balance"] < price: return bot.answer_callback_query(call.id, "❌ Недостаточно средств!", show_alert=True)
            
            add_balance(call.from_user.id, -price)
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="⏳ Создаем заказ Premium...")
            result = krab_create_premium_order(recipient, months, int(time.time()))
            if result:
                bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=f"✅ <b>Premium оплачен с баланса!</b>\n👤 {recipient}\n📅 {months} мес.")
            else:
                add_balance(call.from_user.id, price)
                bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="⚠️ Ошибка. Деньги возвращены.")
        elif call.data in ["prem_pay_usdt", "prem_pay_ton"]:
            if call.from_user.id not in premium_data or "months" not in premium_data[call.from_user.id]:
                return bot.answer_callback_query(call.id, "⚠️ Выберите срок сначала.", show_alert=True)
            months = premium_data[call.from_user.id]["months"]
            price = premium_data[call.from_user.id]["price"]
            recipient = premium_data[call.from_user.id]["recipient"]
            asset = "USDT" if call.data == "prem_pay_usdt" else "TON"
            amount_usdt = round(price / 90, 2)
            pay_url = crypto_create_invoice(amount_usdt, f"Premium {months} мес.", f"{call.from_user.id}:premium:{months}:{price}", asset=asset)
            if pay_url:
                markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton(f"💎 Оплатить {asset}", url=pay_url))
                bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=f"💳 Счет на {amount_usdt} {asset} создан.", reply_markup=markup)
            else:
                bot.answer_callback_query(call.id, "❌ Ошибка создания счета.", show_alert=True)
        bot.answer_callback_query(call.id)
    except Exception as e: print(f"ОШИБКА prem_callback: {e}")

# ===== ВЕБХУКИ =====
@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        json_string = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

@app.route("/", methods=["GET"])
def index(): return "Bot is running", 200

@app.route("/crypto/webhook", methods=["POST"])
def crypto_webhook():
    raw_body = request.get_data()
    signature = request.headers.get("crypto-pay-api-signature")
    secret = hashlib.sha256(CRYPTO_PAY_TOKEN.encode()).digest()
    expected = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected): return "Forbidden", 403
    
    update = request.get_json()
    if update.get("update_type") == "invoice_paid":
        payload = update["payload"].get("payload", "")
        parts = payload.split(":")
        if len(parts) >= 3 and parts[1] == "stars":
            user_id = int(parts[0])
            amount = int(parts[2])
            price = float(parts[3])
            username = str(user_id)
            try:
                conn = get_conn(); cursor = conn.cursor()
                cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if row and row[0]: username = row[0]
                cursor.close(); conn.close()
            except: pass
            
            result = krab_create_stars_order(username, amount, int(time.time()))
            if result:
                try: bot.send_message(user_id, f"✅ Оплата прошла!\n⭐ {amount} звёзд\n⏳ Ожидайте зачисления.")
                except: pass
    return "OK", 200

@app.route("/krab/webhook", methods=["POST"])
def krab_webhook():
    raw_body = request.get_data()
    signature = request.headers.get("X-Signature") or request.headers.get("x-signature")
    expected = hmac.new(KRAB_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    if signature and not hmac.compare_digest(signature, expected): return "Forbidden", 403
    update = request.get_json()
    if not update: return "OK", 200
    order_id = str(update.get("orderId") or update.get("order_id"))
    if update.get("status") in ["completed", "success", "done"]:
        conn = get_conn(); cursor = conn.cursor()
        cursor.execute("SELECT user_id, amount FROM orders WHERE krab_order_id = ?", (order_id,))
        row = cursor.fetchone()
        if row:
            cursor.execute("UPDATE orders SET status = 'выполнен' WHERE krab_order_id = ?", (order_id,))
            conn.commit()
            try: bot.send_message(row[0], f"🎉 Звёзды ({row[1]}) зачислены!")
            except: pass
        cursor.close(); conn.close()
    return "OK", 200

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    print("Бот запущен...")
    app.run(host="0.0.0.0", port=port)
