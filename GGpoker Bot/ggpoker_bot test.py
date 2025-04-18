import logging
import os
import re
import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton,
    Message, CallbackQuery, FSInputFile
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from openpyxl import load_workbook, Workbook
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = int(os.getenv("DATABASE_PORT"))
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
YOUTUBE_LINK = os.getenv("YOUTUBE_LINK")
TWITCH_LINK = os.getenv("TWITCH_LINK")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()
router = Router()
logging.basicConfig(level=logging.INFO)

EXCEL_FILE = 'participants.xlsx'
LOG_FILE = 'broadcast_log.xlsx'
user_states = {}
participants_set = set()
admin_states = {}
banned_users = set()
last_message_times = {}
broadcast_buffer = {}

SPAM_INTERVAL = timedelta(seconds=5)

# Инициализация файлов Excel
if not os.path.exists(EXCEL_FILE):
    wb = Workbook()
    ws = wb.active
    ws.title = "Participants"
    ws.append(["Telegram ID", "Username", "Full Name", "Дата участі", "GGPoker Нік", "Email"])
    wb.save(EXCEL_FILE)

if not os.path.exists(LOG_FILE):
    wb = Workbook()
    ws = wb.active
    ws.title = "Logs"
    ws.append(["Дата", "Повідомлення", "Успішно", "Не вдалося"])
    wb.save(LOG_FILE)


# Функции для работы с БД и Excel
def export_db_to_excel(pool):
    async def inner():
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM participants")
            df = pd.DataFrame(rows, columns=["telegram_id", "username", "full_name", "joined_at", "nickname", "email"])
            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop, f"participants_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            df.to_excel(filename, index=False)
            return filename
    return inner

def save_participant(user: types.User, nickname: str, email: str):
    if user.id in participants_set:
        return False
    wb = load_workbook(EXCEL_FILE)
    ws = wb.active
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    for row in ws.iter_rows(min_row=2, values_only=True):
        if nickname == row[4] or user.id == row[0]:
            return False
    ws.append([
        user.id,
        f"@{user.username}" if user.username else "(без username)",
        full_name,
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        nickname,
        email
    ])
    wb.save(EXCEL_FILE)
    participants_set.add(user.id)
    return True

async def save_participant_to_db(pool, user: types.User, nickname: str, email: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO participants (telegram_id, username, full_name, joined_at, nickname, email)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (telegram_id) DO NOTHING
            """,
            user.id,
            f"@{user.username}" if user.username else "(без username)",
            f"{user.first_name or ''} {user.last_name or ''}".strip(),
            datetime.now(),
            nickname,
            email
        )

# Главное меню пользователя
def user_menu(is_admin=False):
    buttons = [
        [KeyboardButton(text="📜 Умови"), KeyboardButton(text="🎁 Призи")],
        [KeyboardButton(text="📞 Підтримка"), KeyboardButton(text="📍 Мій статус")],
        [KeyboardButton(text="🎉 Взяти участь у розiграшi"), KeyboardButton(text="❓ FAQ")],
        [KeyboardButton(text="🚫 Поскаржитись")],
    ]
    if is_admin:
        buttons.append([KeyboardButton(text="🔐 Admin panel")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, input_field_placeholder="Оберіть опцію")

# Меню поддержки
def support_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✍️ Написати в підтримку")],
            [KeyboardButton(text="🔄 Змінити нікнейм")],
            [KeyboardButton(text="↩️ Назад до меню")],
        ], resize_keyboard=True
    )

# Меню админа
def admin_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👥 Учасники"), KeyboardButton(text="📥 Експорт Excel")],
            [KeyboardButton(text="📤 Список з бази PostgreSQL")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📣 Розсилка")],
            [KeyboardButton(text="🕒 Планувати розсилку"), KeyboardButton(text="⛔ Забанені")],
            [KeyboardButton(text="↩️ Повернутись")],
        ], resize_keyboard=True
    )

# Хендлер /start
@router.message(Command("start"))
async def welcome_user(message: Message):
    if message.from_user.id in banned_users:
        return
    await message.answer(
        "👋 Вітаємо у GGpoker Telegram боті! Це не просто бот для участі в розіграші, "
        "а також ваш персональний асистент для отримання новин, бонусів та корисної інформації про GGpoker. "
        "Натисніть кнопку нижче, щоб розпочати.",
        reply_markup=user_menu(message.from_user.id == ADMIN_ID)
    )

# Участие в розыгрыше
@router.message(F.text == "🎉 Взяти участь у розiграшi")
async def participate_command(message: Message):
    if message.from_user.id in banned_users:
        return
    await message.answer("🔄 Обробляємо запит...", disable_notification=True)
    await asyncio.sleep(1.2)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я підписався", callback_data="participate")]
    ])
    await message.answer(
        text=(
            "📋 Для участі в розіграші потрібно:\n"
            f"1. Підписатися на Telegram канал: {CHANNEL_USERNAME}\n"
            f"2. Підписатися на YouTube: {YOUTUBE_LINK}\n"
            f"3. Підписатися на Twitch: {TWITCH_LINK}\n\n"
            "Після цього натисніть кнопку нижче, щоб продовжити."
        ),
        reply_markup=kb
    )

# Проверка подписки
@router.callback_query(F.data == "participate")
async def check_subscription(callback: CallbackQuery):
    user = callback.from_user
    if user.id in banned_users:
        return
    await callback.message.answer("🔍 Перевіряємо підписку...", disable_notification=True)
    await asyncio.sleep(1.5)
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user.id)
        if member.status not in ["member", "administrator", "creator"]:
            await callback.answer("❌ Спочатку підпишіться на Telegram-канал!", show_alert=True)
            return
    except Exception as e:
        await callback.answer("⚠️ Неможливо перевірити підписку. Спробуйте пізніше.", show_alert=True)
        await bot.send_message(ADMIN_ID, f"❗ Помилка перевірки підписки: {e}")
        return

    user_states[user.id] = 'awaiting_nickname'
    await callback.message.answer("✅ Ви приєдналися! Введіть ваш GGPoker нікнейм.")
    await callback.answer()

# Единственный хендлер для Admin panel
@router.message(F.text == "🔐 Admin panel")
async def open_admin_panel(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас немає доступу до адмін‑панелі.")
        return
    await message.answer("🔐 Вхід в адмін‑панель.", reply_markup=admin_menu())

# Поддержка
@router.message(F.text == "📞 Підтримка")
async def show_support_options(message: Message):
    await message.answer("Оберіть варіант:", reply_markup=support_menu())

@router.message(F.text == "✍️ Написати в підтримку")
async def handle_write_support(message: Message):
    await message.answer(f"Зв'яжіться з підтримкою тут: https://t.me/{SUPPORT_USERNAME.strip('@')}")

@router.message(F.text == "🔄 Змінити нікнейм")
async def handle_change_nickname(message: Message):
    user_states[message.from_user.id] = "awaiting_new_nickname"
    await message.answer("Введіть новий нікнейм для заміни:")

@router.message(F.text == "🚫 Поскаржитись")
async def handle_complaint(message: Message):
    await message.answer("😔 Якщо у вас є скарга — зверніться до адміністратора.")

@router.message(F.text == "↩️ Назад до меню")
async def back_to_main_menu(message: Message):
    await message.answer("🔙 Повертаємося до головного меню:", reply_markup=user_menu(message.from_user.id == ADMIN_ID))

@router.callback_query(F.data == "confirm_participation")
async def confirm_participation(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = user_states.get(user_id)
    if not state or not isinstance(state, dict):
        await callback.answer("⚠️ Щось пішло не так. Спробуйте ще.")
        return
    try:
        nickname = state["nickname"]
        email = state["email"]
        if save_participant(callback.from_user, nickname, email):
            await save_participant_to_db(dp["db"], callback.from_user, nickname, email)
            await callback.message.answer(
                "✅ Участь підтверджено! Успіхів!",
                reply_markup=user_menu(callback.from_user.id == ADMIN_ID)
            )
        else:
            banned_users.add(user_id)
            await callback.message.answer("🚫 Ви вже брали участь або намагалися обдурити бота.")
    except Exception as e:
        await bot.send_message(ADMIN_ID, f"❌ Помилка при підтвердженні участі:\n{e}")
    finally:
        user_states.pop(user_id, None)
        await callback.answer()

@router.message()
async def handle_messages(message: Message):
    user_id = message.from_user.id
    text = message.text

    if user_id in banned_users:
        return

    # Антиспам
    now = datetime.now()
    last = last_message_times.get(user_id)
    if last and now - last < SPAM_INTERVAL:
        return
    last_message_times[user_id] = now

    # Регистрация
    if user_id in user_states:
        state = user_states[user_id]
        if state == 'awaiting_nickname':
            user_states[user_id] = {'step': 'awaiting_email', 'nickname': text.strip()}
            await message.reply("📧 Введіть вашу електронну пошту:")
            return
        elif isinstance(state, dict) and state.get('step') == 'awaiting_email':
            nickname = state['nickname']
            email = text.strip()
            if not re.match(r'^[\w.-]+@[\w.-]+\.\w{2,}$', email):
                await message.reply("❌ Невірний формат email. Спробуйте ще раз:")
                return
            await message.answer(
                "✅ Все готово! Підтвердіть участь:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Підтверджую участь", callback_data="confirm_participation")]
                ])
            )
            user_states[user_id] = {"step": "confirming", "nickname": nickname, "email": email}
            return

    # Admin commands from DB
    if text == "📤 Список з бази PostgreSQL" and user_id == ADMIN_ID:
        await message.answer("🔄 Експортуємо список...")
        export_func = export_db_to_excel(dp['db'])
        file_path = await export_func()
        await message.answer_document(FSInputFile(file_path))
        return

    # Основное меню
    if text == "📜 Умови":
        await message.answer(
            f"📜 Умови:\n1. Підписка на {CHANNEL_USERNAME}\n2. YouTube: {YOUTUBE_LINK}\n3. Twitch: {TWITCH_LINK}",
            reply_markup=user_menu(user_id == ADMIN_ID)
        )
    elif text == "🎁 Призи":
        await message.answer(
            "🎁 Призовий фонд: бонуси для 3 учасників!",
            reply_markup=user_menu(user_id == ADMIN_ID)
        )
    elif text == "📍 Мій статус":
        status = "✅ Ви берете участь!" if user_id in participants_set else "❌ Ви ще не брали участі."
        await message.answer(status, reply_markup=user_menu(user_id == ADMIN_ID))
    elif text == "❓ FAQ":
        await message.answer(
            "ℹ️ Часті питання:\n"
            "- Як дізнатися чи я зареєстрований?\n"
            "- Як змінити нікнейм?\n"
            "- Як зв'язатися з підтримкою?",
            reply_markup=user_menu(user_id == ADMIN_ID)
        )
    elif text == "👥 Учасники" and user_id == ADMIN_ID:
        wb = load_workbook(EXCEL_FILE)
        ws = wb.active
        info = "\n".join(f"{r[1]} | {r[2]} | {r[4]} | {r[3]}" for r in ws.iter_rows(min_row=2, values_only=True))
        await message.answer(f"👥 Список учасників:\n{info}")
    elif text == "📊 Статистика" and user_id == ADMIN_ID:
        wb = load_workbook(EXCEL_FILE)
        ws = wb.active
        count = ws.max_row - 1
        await message.answer(f"📊 Зареєстровано учасників: {count}")
    elif text == "📥 Експорт Excel" and user_id == ADMIN_ID:
        await message.answer_document(FSInputFile(EXCEL_FILE))
    elif text == "📣 Розсилка" and user_id == ADMIN_ID:
        admin_states[user_id] = "awaiting_broadcast"
        await message.answer("✉️ Введіть текст для розсилки.")
    elif text == "🕒 Планувати розсилку" and user_id == ADMIN_ID:
        admin_states[user_id] = "awaiting_schedule"
        await message.answer("🕒 Введіть дату, час (YYYY-MM-DD HH:MM) та текст:")
    elif text == "⛔ Забанені" and user_id == ADMIN_ID:
        banned_list = "\n".join(map(str, banned_users)) or "✅ Список порожній."
        await message.answer(f"🚫 Забанені:\n{banned_list}")
    elif text == "↩️ Повернутись":
        await message.answer("🔙 Повертаємося:", reply_markup=user_menu(user_id == ADMIN_ID))
    elif user_id in admin_states and admin_states[user_id] == "awaiting_broadcast":
        broadcast_buffer[user_id] = {"text": text.strip()}
        await confirm_broadcast_manual(user_id)
        del admin_states[user_id]
    elif user_id in admin_states and admin_states[user_id] == "awaiting_schedule":
        try:
            parts = text.split(" ", 2)
            run_dt = datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M")
            content = parts[2]
            scheduler.add_job(lambda: asyncio.create_task(bot.send_message(ADMIN_ID, content)),
                              trigger=DateTrigger(run_date=run_dt))
            await message.answer(f"🕒 Розсилку заплановано на {run_dt}")
        except Exception as e:
            await message.answer(f"❌ Помилка: {e}")
        finally:
            del admin_states[user_id]

async def confirm_broadcast_manual(user_id: int):
    data = broadcast_buffer.pop(user_id, None)
    if not data:
        await bot.send_message(user_id, "⚠️ Текст не знайдено.")
        return
    msg = data["text"]
    wb = load_workbook(EXCEL_FILE)
    ws = wb.active
    count, failed = 0, []
    for r in ws.iter_rows(min_row=2, values_only=True):
        try:
            await bot.send_message(r[0], msg)
            count += 1
        except Exception:
            failed.append(r[0])
    log_wb = load_workbook(LOG_FILE)
    log_ws = log_wb.active
    log_ws.append([datetime.now().strftime("%Y-%m-%d %H:%M"), msg, count, ", ".join(map(str, failed))])
    log_wb.save(LOG_FILE)
    await bot.send_message(user_id, f"✅ Розсилка: {count} успіх, {len(failed)} помилок.")

async def main():
    pool = await asyncpg.create_pool(
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        database=DATABASE_NAME,
        host=DATABASE_HOST,
        port=DATABASE_PORT
    )
    dp['db'] = pool
    dp.include_router(router)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())


