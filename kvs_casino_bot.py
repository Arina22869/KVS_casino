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

# ============= ПЕРЕМЕННЫЕ =============
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден")

SPREADSHEET_ID = "1uQXxwPm-HkrAD_hErpjtInBFwOaYJtTHkgqqfJ0_6V0"

# ============= ВСТРОЕННЫЙ КЛЮЧ (как в Снимочках) =============
KEY_JSON = '''{
  "type": "service_account",
  "project_id": "kvs-kasik",
  "private_key_id": "c36b9c1733e49d42a8b22038c7a0cf0349f3d652",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDTXi6J+Z9abMhW\\ng7+pVAOwi+z9hF49T8Z1cFEAZYxXf7Av1K0msCOfXxuQ1goWqIsfLhiyMYG8fSHI\\nneI2zT+cYwhGrKIj2jDC++tzv7MQ2hXvt1ZQr54NFgjVBChISpGYNNelLi658nrv\\n73Tk0oJIEW7EetA6tE/Hr6nJGeK12sIgy3dI/BHfOq9kEYl3nwlMoIu4Cj4OB+TJ\\ncQzghyMo1Gr45P0/gXWpHqYL13DBhYH02cf4mhxnkKxJoSN+GaPw8yXhoLszAXJJ\\n93X5NXM3aMRfEsrYHQOKON9Xg12LA/NLt1NWv6WPeBgLjw26skPAyTuKvdlODw2A\\npay0da6NAgMBAAECggEAClpQmT16yqkNXv9xAdA1pg4Ue45iN0qTA+mObiCLMTX1\\n4UHfgUebVxEHfp6dO+LhfZN6bi/ylNLkNrlU/S4scXKAU0XdYzgqr0I7lB3NnYOx\\nE169gZI4gZeUsypS9seAPh5IebS7YSw5TUtDRHnzG6iO7ly9mkJ+rd8yx8DW0+Ve\\nVfgzXmfwVdFADeCVOK83qEOX4y+jpt0vWc485YWz+XEp35Z5+0pc6wD8SVi1qRb7\\nlna7I7apWO3Ekfg+tDwJNOIGx85wSws+x67Jvt8jJGIVxIpF8feultj1ikPMwJ9G\\nQo9Bqdlac/yAF1DlJhKCI51gneSMFM42w1iyfxHrWQKBgQDth4dU21XFB8ZIi+eV\\nwT9COnMWQWsqVx6OUdBxtI/b3UNBgLOzdCjVbdkK+/+rcHfmwPn1PhSrWi6imq9f\\nY5JHYPID6BdR6wQmAIk3+yUnGiFd4zR1JsUAKVgye6ScuKG/ds4tOYKfRyKWN9c9\\nx1KgOwvRISmYvL86tDEjr8MtCwKBgQDjzdvfjH0W8xTe2/DsHWCMQZv7ORdvEZnj\\njBOM/sd32hIYBh763BD+OntqEYSXE8DU/IB5DaMvDSJvyQmjNUQYs3mVTsXaumON\\ncu975/tmyhSqs18sXNZ0GpLOvC6VFbDtBra6haF8hp+CeswILXWiE7wfBsE71Y4Q\\nPedjjdXhxwKBgQDUvDVwwqCmvrfP8b2QWmuVnVPF8wFQAobTYGMX82eEuz6pQ+Ou\\nLbMEtEVXmSr7GNfKS7uS8e8BKNvrti6reDpiw2j+Jrf8Hkiw4HoFMWGtC7ImrH8n\\nDXoTEvRzAloEIzh6iqVNy9w5WzSW5ZxZMFPIPhnvS4w9x45dvVTlaV2c0QKBgQDa\\nP6vbrPlbN2BTc1yKmkqZlXIfaj8tRiutedJxTtdD2EVlhte/d39AUj9TC52AMIia\\nhZ+AWrRwq6DGgFEfcDThhXGCvomIWPJv3iHbEBIoFItgT6FrGzbK3XqxXlEyvClZ\\nrQj8AnMInuHLXGKFoygEM+wTvaD79km4/dXl50l2mQKBgQCYmoU66V3rLxuYWhHV\\nhJiCDOhISV6YEfVOzAK2TEfUUHZypK1l3f+y2YoHYUfpWLI+2VVGBf7/jmuIkweB\\nRcSal1t6YnL2B/OwDrwCoB8tBNkQAG66VGJbXynwkGd0+KCsuOMZhSufsJDqRcUI\\njrP/RokGXbMTLMnVHYFhsTnXkQ==\\n-----END PRIVATE KEY-----\\n",
  "client_email": "csinokvs@kvs-kasik.iam.gserviceaccount.com",
  "client_id": "103287774838679768904",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/csinokvs%40kvs-kasik.iam.gserviceaccount.com",
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
            logging.info(f"✅ Google обновлён: {user_id} -> {new_value}")
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
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎡 Открыть Колесо", web_app=WebAppInfo(url="https://твой-бот.bothost.net/static/wheel.html"))]
    ])
    await m.answer(text, reply_markup=keyboard)

@dp.message(F.web_app_data)
async def handle_webapp(m: Message):
    data = json.loads(m.web_app_data.data)
    user_id = m.from_user.id
    
    if data["type"] == "getBalance":
        coins = get_user_coins(user_id)
        spins = get_freespins(user_id)
        await m.answer(json.dumps({"type": "balance", "balance": coins, "freespins": spins}))
        
    elif data["type"] == "spin":
        coins = get_user_coins(user_id)
        spins = get_freespins(user_id)
        
        if coins < 5 and spins == 0:
            await m.answer(json.dumps({"type": "result", "prizeText": "❌ Не хватает коинов (нужно 5)"}))
            return
        
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
        
        await m.answer(json.dumps({
            "type": "result",
            "prizeText": prize["text"],
            "newBalance": final_coins,
            "newFreespins": final_spins
        }))

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
    print("🚀 КВС‑Казино запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
