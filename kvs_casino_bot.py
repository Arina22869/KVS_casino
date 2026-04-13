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

# ============= ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ =============
TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден! Добавьте переменную окружения BOT_TOKEN")

SPREADSHEET_ID = "1uQXxwPm-HkrAD_hErpjtInBFwOaYJtTHkgqqfJ0_6V0"  # ID твоей новой таблицы
# ================================================

# ============= БАЗА ДАННЫХ SQLite (фриспины) =============
@contextmanager
def get_db():
    conn = sqlite3.connect('freespins.db', timeout=10)
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS freespins (user_id INTEGER PRIMARY KEY, spins INTEGER DEFAULT 0)')
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

# ============= GOOGLE SHEETS (кэш) =============
def get_gs_client():
    try:
        creds = Credentials.from_service_account_file(
            'google_key(3).json',  # <--- имя твоего файла
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

# ============= ВЕРОЯТНОСТИ ИГРЫ =============
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

# ============= ФОНОВАЯ СИНХРОНИЗАЦИЯ =============
async def bg_sync():
    while True:
        await asyncio.sleep(60)
        try:
            sync_users_from_google()
        except Exception as e:
            logging.error(f"Ошибка синхронизации: {e}")

# ============= ЗАПУСК =============
async def main():
    init_db()
    sync_users_from_google()
    asyncio.create_task(bg_sync())
    print("🚀 КВС‑Казино запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
