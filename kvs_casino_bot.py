import logging
import sqlite3
import random
import asyncio
import json
import os
from contextlib import contextmanager
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request, jsonify
import threading

# ============= ПЕРЕМЕННЫЕ =============
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден")

SPREADSHEET_ID = "1uQXxwPm-HkrAD_hErpjtInBFwOaYJtTHkgqqfJ0_6V0"

# ============= НОВЫЙ КЛЮЧ =============
KEY_JSON = '''{
  "type": "service_account",
  "project_id": "kvs-casino",
  "private_key_id": "60dd8b97cbc10009e0171a2cf315161444ddd942",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC0X/Tt72Syi2Hv\\nfiUTK/POUfzA6eZTEepI1BpQIZSiaH5Fqw1Mg0eOJu0OJA2apx/RCZFXZ9E9ts5H\\n90LbocsNFns26S6w/mTACw3DgQqCvvcDO9tcBLxTXUzGWWRXr4gH+19eRxJrTBYU\\n+v0ngosfTk4C6BatopT3njt5XxI1U+6cLYcGIFzSUW8A1jiidO/enOktSHf2rvuu\\nCpR0yoxmkLE7L5vjof1C2M5qlDPGSv4w//u20kF8DTEFl9GC85qP7xtlCZIPpzp3\\nhyrS4DaXYWqrEapbVDCyyKFp/8/T+Wkg+ZFkha9GpONg/k1WrIV1mul9LVYHkAWI\\nK3pDNWu7AgMBAAECggEAK3fCbUKXOEXJtK7oHV4ms45jhFP5uwQiuylOLAhjqt3W\\nrFBj9I/ggxvTXeiHVME4tCYDnhY5QQ+YV5R5SreLWNyVle6M33925SbkyrwCve5q\\nVZ+rpdnITqi500UAnvcAp7fGzFABWtUrryOrNUXK5CD5QM8QhAidsKyztXSZTTtE\\nqeIrbn+plKdQPS7AK94iykD+b1uj0hdLlTy9NdwkiYiLklTe7ljt/ZhhKs+saQuY\\nMniApCBnIsYBqBX3MnjuHQ3vPmeV/tWDSqt2Y72RrCBnTcIJhgraEnfH/xTOwTOa\\nPf4ZEehQy5ahBZrAy9pqVXYWK4eKnqwfmxiN1PFEsQKBgQDfzoiGq77+ohm0B2bd\\n6M7LfLH61fXbf/lOeyZpMwMr2yh3XmjHwAHzaVE9gzcGvYKNUdvYDp6mTmDGmlqv\\nCvMcBv6qRD9isKKioVqNSSaRzzjQ0lBJwZx4+rgfftS+rs1rci9pJdR7MEvrdRKO\\nOxAZnAAmmAXfirjCwTlgIDtkUQKBgQDOUhWiTfZrGAb1aJkL/LGAkOU9EECTV6Bs\\naPv5GJG+F9WOLTMV7cOlBR5COrTGYtEgBvbEloZVa1S7sLnFTJ2ufXZbhAZ3+0hY\\ndKfWNc1+RhQbBfcllNteML6N6mjZ2wA4Ct5eexTYovSZawuRGKhS7Bm75BCQnxx8\\nX3rT70SISwKBgBZPW1K5aiet9wv4NOvoGj00p/VOQNzpq+uD4TdZa3aCuUz071MC\\nM++UzxFntCOK3qnBwD2Qb4Y6h/EkT+flGojvtZE5jmwDGaNGnGU7JoogcxR18qT3\\nlOaGb7ZMCV8cw8NzNYCw0baaAOdu2zOsdZVn9KfkPamkBXj8lACeFe2RAoGAcQmj\\n9LKGKZKWbWoLP/gIQAoirlvzJUbDC+JQ+t6tKtrgE+9Lp94GbKjrNRn45SBKtyNZ\\nm8dkffQ6DkL65M/fLDRs3iLtcFrp/hybv87mcSJv7YZNK/fsYCSQoiwlmgZUjl/W\\nCSmh+Db/j/aH9czum8/jIYAIW1PxRWsCXo8emxUCgYAPqXF0EbCoQowjfecGWKM0\\njObcquSY8wV7fJmq8ZuMpXY2+5PTuyJjZ7u/Xf4yraaL4+UL6uwQ4W/3Xbku7v+5\\nw8EPAab894p4+ZpXMgWKMiZSSCmRnF0Qvr9S674zp28rbnAMBoK+mnOy/UZ+3moC\\n3Vu3UbD80f2hq+b0gAgG3A==\\n-----END PRIVATE KEY-----\\n",
  "client_email": "kasik-416@kvs-casino.iam.gserviceaccount.com",
  "client_id": "106386327390576095601",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/kasik-416%40kvs-casino.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}'''
# =====================================

# ============= SQLite =============
@contextmanager
def get_db():
    conn = sqlite3.connect('kvs_casino.db', timeout=10)
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS freespins (user_id INTEGER PRIMARY KEY, spins INTEGER DEFAULT 0)')
        conn.execute('CREATE TABLE IF NOT EXISTS users_cache (user_id INTEGER PRIMARY KEY, name TEXT, cur_pts INTEGER DEFAULT 0)')
        conn.commit()

def get_freespins(user_id):
    with get_db() as conn:
        cur = conn.execute("SELECT spins FROM freespins WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return row[0] if row else 0

def update_freespins(user_id, delta):
    with get_db() as conn:
        cur = conn.execute("SELECT spins FROM freespins WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if row:
            new_val = max(0, row[0] + delta)
            conn.execute("UPDATE freespins SET spins=? WHERE user_id=?", (new_val, user_id))
        else:
            new_val = max(0, delta)
            conn.execute("INSERT INTO freespins (user_id, spins) VALUES (?, ?)", (user_id, new_val))
        conn.commit()
        return new_val

# ============= GOOGLE SHEETS (встроенный ключ) =============
def get_gs_client():
    try:
        creds_dict = json.loads(KEY_JSON)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds)
    except Exception as e:
        logging.error(f"❌ Ошибка подключения: {e}")
        return None

def sync_users_from_google():
    client = get_gs_client()
    if not client:
        return
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet("users")
        all_rows = sheet.get_all_values()
        if len(all_rows) < 2:
            return
        with get_db() as conn:
            conn.execute("DELETE FROM users_cache")
            for row in all_rows[1:]:
                if len(row) >= 4 and row[0].strip().isdigit():
                    uid = int(row[0])
                    name = f"{row[1]} {row[2]}" if len(row) > 2 else f"User{uid}"
                    pts = int(row[3]) if len(row) > 3 and row[3].isdigit() else 0
                    conn.execute("INSERT INTO users_cache (user_id, name, cur_pts) VALUES (?, ?, ?)",
                                (uid, name, pts))
            conn.commit()
        logging.info("✅ Кэш пользователей обновлён")
    except Exception as e:
        logging.error(f"❌ Ошибка синхронизации: {e}")

def get_user_coins(user_id):
    with get_db() as conn:
        cur = conn.execute("SELECT cur_pts FROM users_cache WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return row[0] if row else 0

def update_user_coins(user_id, new_value):
    with get_db() as conn:
        conn.execute("UPDATE users_cache SET cur_pts=? WHERE user_id=?", (new_value, user_id))
        if conn.rowcount == 0:
            conn.execute("INSERT INTO users_cache (user_id, name, cur_pts) VALUES (?, ?, ?)",
                        (user_id, f"User{user_id}", new_value))
        conn.commit()
    
    async def bg():
        client = get_gs_client()
        if not client:
            return
        try:
            sheet = client.open_by_key(SPREADSHEET_ID).worksheet("users")
            all_rows = sheet.get_all_values()
            row_num = None
            uid_str = str(user_id)
            for i, row in enumerate(all_rows, start=1):
                if row and row[0] == uid_str:
                    row_num = i
                    break
            if row_num:
                sheet.update_cell(row_num, 4, str(new_value))
            else:
                sheet.append_row([uid_str, "Новый", "Активист", str(new_value)])
            logging.info(f"✅ Google обновлен: {user_id} -> {new_value}")
        except Exception as e:
            logging.error(f"❌ Ошибка обновления Google: {e}")
    
    asyncio.create_task(bg())

# ============= ИГРА =============
def get_prize():
    rand = random.random() * 100
    if rand <= 1.0:
        return {"type": "meme", "delta": 0, "text": "💋 Поцелуй от председателя (только для мальчиков)"}
    elif rand <= 1.67:
        return {"type": "meme", "delta": 0, "text": "🍾 Чикушка водки от ВРИО"}
    elif rand <= 8.67:
        return {"type": "freespin", "delta": 1, "text": "🎡 ФРИСПИН (+1 бесплатное кручение)"}
    elif rand <= 28.67:
        return {"type": "coins", "delta": 10, "text": "10 коинов"}
    elif rand <= 38.67:
        return {"type": "coins", "delta": 5, "text": "5 коинов"}
    elif rand <= 63.67:
        return {"type": "coins", "delta": 2, "text": "2 коина"}
    else:
        return {"type": "coins", "delta": 0, "text": "0 коинов"}

# ============= HTTP-сервер =============
app = Flask(__name__)

@app.route('/balance/<int:user_id>', methods=['GET'])
def get_balance(user_id):
    coins = get_user_coins(user_id)
    spins = get_freespins(user_id)
    return jsonify({"balance": coins, "freespins": spins})

@app.route('/spin/<int:user_id>', methods=['POST'])
def spin_route(user_id):
    coins = get_user_coins(user_id)
    spins = get_freespins(user_id)
    
    if coins < 5 and spins == 0:
        return jsonify({"error": "Не хватает коинов"}), 400
    
    if spins > 0:
        update_freespins(user_id, -1)
    else:
        update_user_coins(user_id, coins - 5)
    
    prize = get_prize()
    final_coins = get_user_coins(user_id)
    final_spins = get_freespins(user_id)
    
    if prize["type"] == "coins":
        final_coins += prize["delta"]
        update_user_coins(user_id, final_coins)
    elif prize["type"] == "freespin":
        final_spins += prize["delta"]
        update_freespins(user_id, prize["delta"])
    
    return jsonify({
        "prizeText": prize["text"],
        "newBalance": final_coins,
        "newFreespins": final_spins
    })

def run_flask():
    app.run(host='0.0.0.0', port=8080)

# ============= БОТ =============
logging.basicConfig(level=logging.INFO)
bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "🎡 Добро пожаловать в КВС‑КАЗИНО!\n\n"
        "Крути колесо за 5 КВС‑коинов.\n"
        "Используй /casino, чтобы открыть игру."
    )

@dp.message(Command("casino"))
async def cmd_casino(m: Message):
    user_id = m.from_user.id
    coins = get_user_coins(user_id)
    freespins = get_freespins(user_id)
    
    text = (f"🎰 Колесо удачи\n\n"
            f"💰 Коинов: {coins}\n"
            f"🎡 Фриспинов: {freespins}\n\n"
            f"Крути за 5 коинов!")
    
    # Ссылка на твой HTML на Netlify (замени на свою)
    html_url = "https://69de2a2973d82a10b5d7c363--effulgent-tulumba-a161d4.netlify.app/"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎡 Открыть Колесо", web_app=WebAppInfo(url=html_url))]
    ])
    await m.answer(text, reply_markup=keyboard)

# ============= ФОН =============
async def bg_sync():
    while True:
        await asyncio.sleep(60)
        try:
            sync_users_from_google()
        except Exception as e:
            logging.error(f"Ошибка синхронизации: {e}")

async def main():
    init_db()
    sync_users_from_google()
    asyncio.create_task(bg_sync())
    
    # Запускаем Flask в отдельном потоке
    threading.Thread(target=run_flask, daemon=True).start()
    
    print("🚀 КВС‑Казино запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
