#!/usr/bin/env python3
# coding: utf-8

"""
Planning Poker Telegram Bot
- aiogram v3-based example
- uses aiosqlite for persistence (local file: poker.db)
- anonymous voting: votes stored but not shown per-user
- admin (creator) can see who hasn't voted (usernames/names), but not their choices
"""

import asyncio
import os
import csv
import statistics
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiosqlite
import logging
import secrets
import datetime

# --- Config ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN env variable or in .env file")

DATABASE = os.getenv("POKER_DB", "poker.db")
VOTE_OPTIONS = ["0", "½", "1", "2", "3", "5", "8", "13", "20", "40", "100", "?"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# --- DB helpers and models ---
async def init_db():
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            creator_id INTEGER,
            creator_name TEXT,
            title TEXT,
            description TEXT,
            created_at TEXT,
            status TEXT -- open|closed
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS session_members (
            session_id TEXT,
            user_id INTEGER,
            username TEXT,
            first_name TEXT,
            joined_at TEXT,
            PRIMARY KEY(session_id, user_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            session_id TEXT,
            user_id INTEGER,
            value TEXT,
            voted_at TEXT,
            PRIMARY KEY(session_id, user_id)
        )
        """)
        await db.commit()


# utility
def make_session_id() -> str:
    return secrets.token_urlsafe(8)

def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"

async def create_session(creator: types.User, title: str, description: Optional[str]) -> str:
    sid = make_session_id()
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute(
            "INSERT INTO sessions(id, creator_id, creator_name, title, description, created_at, status) VALUES (?,?,?,?,?,?,?)",
            (sid, creator.id, f"{creator.full_name}", title, description or "", now_iso(), "open")
        )
        await db.commit()
    return sid

async def add_member(session_id: str, user: types.User):
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute("""
        INSERT OR IGNORE INTO session_members(session_id, user_id, username, first_name, joined_at)
        VALUES (?,?,?,?,?)
        """, (session_id, user.id, user.username or "", user.full_name or "", now_iso()))
        await db.commit()

async def set_vote(session_id: str, user: types.User, value: str):
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute("""
        INSERT OR REPLACE INTO votes(session_id, user_id, value, voted_at) VALUES (?,?,?,?)
        """, (session_id, user.id, value, now_iso()))
        await db.commit()

async def get_session(session_id: str) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE) as db:
        cur = await db.execute("SELECT id, creator_id, creator_name, title, description, created_at, status FROM sessions WHERE id = ?", (session_id,))
        row = await cur.fetchone()
        if not row:
            return None
        keys = ["id", "creator_id", "creator_name", "title", "description", "created_at", "status"]
        return dict(zip(keys, row))

async def get_members(session_id: str) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE) as db:
        cur = await db.execute("SELECT user_id, username, first_name, joined_at FROM session_members WHERE session_id = ?", (session_id,))
        rows = await cur.fetchall()
        keys = ["user_id", "username", "first_name", "joined_at"]
        return [dict(zip(keys, r)) for r in rows]

async def get_votes(session_id: str) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE) as db:
        cur = await db.execute("SELECT user_id, value, voted_at FROM votes WHERE session_id = ?", (session_id,))
        rows = await cur.fetchall()
        keys = ["user_id", "value", "voted_at"]
        return [dict(zip(keys, r)) for r in rows]

async def clear_votes(session_id: str):
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute("DELETE FROM votes WHERE session_id = ?", (session_id,))
        await db.commit()

async def set_session_status(session_id: str, status: str):
    async with aiosqlite.connect(DATABASE) as db:
        await db.execute("UPDATE sessions SET status = ? WHERE id = ?", (status, session_id))
        await db.commit()

# --- Results helper ---
async def compose_and_broadcast_results(session_id: str) -> Optional[str]:
    session = await get_session(session_id)
    if not session:
        return None
    votes = await get_votes(session_id)
    members = await get_members(session_id)
    if not votes:
        return None

    counts: Dict[str, int] = {}
    numeric_values = []
    for v in votes:
        val = v["value"]
        counts[val] = counts.get(val, 0) + 1
        try:
            if val == "?":
                continue
            elif val == "½":
                numeric_values.append(0.5)
            else:
                numeric_values.append(float(val))
        except Exception:
            pass

    lines = [f"📊 *Результаты голосования для* _{session['title']}_ (`{session_id}`):"]
    total = len(votes)
    for opt in VOTE_OPTIONS:
        if opt in counts:
            lines.append(f"{opt} — {counts[opt]}")

    stats_lines = []
    if numeric_values:
        try:
            mean = statistics.mean(numeric_values)
            median = statistics.median(numeric_values)
            stats_lines.append(f"\n⚖️ Среднее: {mean:.2f}")
            stats_lines.append(f"🔹 Медиана: {median}")
        except Exception:
            pass

    lines.append(f"\nВсего голосов: {total}")
    if stats_lines:
        lines.extend(stats_lines)

    # кто не голосовал (для ведущего полезно видеть)
    voted_ids = {v["user_id"] for v in votes}
    not_voted = [m for m in members if m["user_id"] not in voted_ids]
    if not_voted:
        lines.append("\n❗Не голосовали:")
        for m in not_voted:
            display = m["username"] or m["first_name"] or str(m["user_id"])
            lines.append(f"- {display}")

    result_text = "\n".join(lines)

    # Рассылаем результаты всем участникам
    for m in members:
        try:
            await bot.send_message(
                m["user_id"],
                result_text,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Не удалось отправить сообщение пользователю {m['user_id']}: {e}")

    return result_text

# --- UI helpers ---
def build_vote_keyboard(session_id: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # 4 per row
    row = []
    for i, opt in enumerate(VOTE_OPTIONS, 1):
        cb = InlineKeyboardButton(text=opt, callback_data=f"vote|{session_id}|{opt}")
        row.append(cb)
        # create rows of 4
        if i % 4 == 0:
            kb.row(*row)
            row = []
    if row:
        kb.row(*row)
    # actions
    kb.row(
        InlineKeyboardButton(text="Показать результаты (только ведущий)", callback_data=f"reveal|{session_id}"),
        InlineKeyboardButton(text="Переголосовать (сброс голосов, только ведущий)", callback_data=f"revote|{session_id}")
    )
    kb.row(
        InlineKeyboardButton(text="Участники", callback_data=f"members|{session_id}")
    )
    return kb.as_markup()

def build_session_buttons(session_id: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="Присоединиться и проголосовать", callback_data=f"join|{session_id}"),
        InlineKeyboardButton(text="Инструкция", callback_data=f"info|{session_id}")
    )
    return kb.as_markup()

# --- Commands ---
@dp.message(Command(commands=["start"]))
async def cmd_start(event: types.Message):
    text = (
        "Привет! Я бот для анонимного голосования (Planning Poker).\n\n"
        "Команды:\n"
        "/new_session <название задачи> - создать новую сессию (в личке или в группе)\n"
        "/my_sessions - показать сессии, которые вы создали\n"
        "/session <id задачи> - присоединиться к созданной сессии \n"
        "/close_session <id задачи> - закрыть созданную сессию \n"
        "/help - краткая помощь\n\n"
        "Можно запускать голосование в личке или приглашать участников в чат."
    )
    await event.answer(text)

@dp.message(Command(commands=["help"]))
async def cmd_help(event: types.Message):
    await event.answer(
        "Как пользоваться:\n"
        "1) Создайте сессию: /new_session <название задачи>\n"
        "2) Пригласите участников: отправьте кнопку или ссылку (бот пришлёт).\n"
        "3) Участники нажимают 'Присоединиться и проголосовать' и выбирают карту.\n"
        "4) Ведущий (создатель) нажимает 'Показать результаты' — бот покажет распределение и среднее/медиану.\n"
        "Переголосовать — сброс голосов для повторного голосования."
    )

@dp.message(Command(commands=["new_session"]))
async def cmd_new_session(message: Message, command: CommandObject):
    # parse args: /new_session <title> | optional description in new message
    args = command.args
    if not args:
        await message.reply("Использование: /new_session <название задачи>\nПример: /new_session Оценить задачу: Авторизация через Oauth")
        return
    title = args
    description = ""
    sid = await create_session(message.from_user, title, description)
    # add creator as member automatically
    await add_member(sid, message.from_user)
    kb = build_session_buttons(sid)
    text = f"Создана сессия *{title}*\nID: `{sid}`\n\nПригласите участников — они смогут подключиться и проголосовать.\nВедущий: {message.from_user.full_name}"
    await message.reply(text, parse_mode="Markdown", reply_markup=kb)

@dp.message(Command(commands=["my_sessions"]))
async def cmd_my_sessions(event: types.Message):
    uid = event.from_user.id
    async with aiosqlite.connect(DATABASE) as db:
        cur = await db.execute("SELECT id, title, created_at, status FROM sessions WHERE creator_id = ?", (uid,))
        rows = await cur.fetchall()
    if not rows:
        await event.reply("У вас пока нет созданных сессий.")
        return
    lines = []
    for r in rows:
        sid, title, created_at, status = r
        lines.append(f"- *{title}* (`{sid}`) — {status}, создано {created_at}")
    await event.reply("Ваши сессии:\n" + "\n".join(lines), parse_mode="Markdown")

# --- Callback handling (join, vote, reveal, revote, info) ---
@dp.callback_query(lambda c: c.data and c.data.startswith("join|"))
async def cb_join(callback: types.CallbackQuery):
    await callback.answer()  # remove loading
    _, sid = callback.data.split("|", 1)
    session = await get_session(sid)
    if not session:
        await callback.message.edit_text("Сессия не найдена или уже удалена.")
        return
    if session["status"] != "open":
        await callback.message.edit_text("Эта сессия закрыта.")
        return
    await add_member(sid, callback.from_user)
    # show voting keyboard DM
    kb = build_vote_keyboard(sid)
    try:
        await bot.send_message(callback.from_user.id,
                               f"Вы присоединились к сессии *{session['title']}* (`{sid}`). Выберите вашу карту:",
                               parse_mode="Markdown",
                               reply_markup=kb)
        await callback.answer("Я отправил вам клавиатуру для голосования в личку.")
    except Exception as e:
        # can't send DM => ask to start bot
        logger.exception("Can't send DM")
        await callback.answer("Не удалось отправить сообщение в личку. Попросите пользователя начать чат с ботом (/start).", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith("vote|"))
async def cb_vote(callback: types.CallbackQuery):
    await callback.answer()
    _, sid, value = callback.data.split("|", 2)
    session = await get_session(sid)
    if not session:
        await callback.message.edit_text("Сессия не найдена.")
        return
    if session["status"] != "open":
        await callback.message.edit_text("Голосование закрыто для этой сессии.")
        return
    # save vote
    await set_vote(sid, callback.from_user, value)
    
    # проверяем, является ли голосующий ведущим (создателем)
    if callback.from_user.id == session["creator_id"]:
        # Ведущий: клавиатура остаётся, показываем сообщение с подтверждением
        await callback.message.edit_text(
            f"Вы проголосовали как ведущий. Ваш голос *{value}* сохранён (анонимно).",
            parse_mode="Markdown",
            reply_markup=build_vote_keyboard(sid)  # клавиатура остаётся
        )
    else:
        # Участник: клавиатура исчезает после голосования
        await callback.message.edit_text(
            f"Ваш голос *{value}* сохранён (анонимно).",
            parse_mode="Markdown",
            reply_markup=None  # убираем клавиатуру
        )

    # Автоматическое раскрытие результатов: если все участники проголосовали
    try:
        if session["status"] == "open":
            members = await get_members(sid)
            votes = await get_votes(sid)
            unique_voters = {v["user_id"] for v in votes}
            if members and len(unique_voters) == len(members):
                result_text = await compose_and_broadcast_results(sid)
                if result_text:
                    # Уведомим ведущего отдельно, чтобы было понятно, что всё произошло автоматически
                    try:
                        await bot.send_message(session["creator_id"], "✅ Все участники проголосовали. Результаты разосланы всем участникам.")
                    except Exception:
                        pass
    except Exception as e:
        logger.warning(f"Auto-reveal error for session {sid}: {e}")

@dp.callback_query(lambda c: c.data and c.data.startswith("reveal|"))
async def cb_reveal(callback: types.CallbackQuery):
    await callback.answer()
    _, sid = callback.data.split("|", 1)
    session = await get_session(sid)
    if not session:
        await callback.answer("Сессия не найдена", show_alert=True)
        return

    # Только создатель может показать результаты
    if callback.from_user.id != session["creator_id"]:
        await callback.answer("Только ведущий (создатель сессии) может показать результаты.", show_alert=True)
        return
    result_text = await compose_and_broadcast_results(sid)
    if not result_text:
        await callback.message.reply("Голосов ещё нет.")
        return
    await callback.message.reply("✅ Результаты разосланы всем участникам.", parse_mode="Markdown")

@dp.callback_query(lambda c: c.data and c.data.startswith("revote|"))
async def cb_revote(callback: types.CallbackQuery):
    await callback.answer()
    _, sid = callback.data.split("|", 1)
    session = await get_session(sid)
    if not session:
        await callback.answer("Сессия не найдена", show_alert=True)
        return
    if callback.from_user.id != session["creator_id"]:
        await callback.answer("Только ведущий может сделать переголосование.", show_alert=True)
        return
    await clear_votes(sid)
    # notify members if possible
    members = await get_members(sid)
    for m in members:
        try:
            await bot.send_message(m["user_id"], f"Ведущий сбросил голоса для сессии *{session['title']}* (`{sid}`). Пожалуйста, проголосуйте снова.", parse_mode="Markdown", reply_markup=build_vote_keyboard(sid))
        except Exception:
            pass
    await callback.message.reply("Голоса сброшены и отправлены запросы на переголосование участникам (если бот мог им писать).")

@dp.callback_query(lambda c: c.data and c.data.startswith("members|"))
async def cb_members(callback: types.CallbackQuery):
    await callback.answer()
    _, sid = callback.data.split("|", 1)
    session = await get_session(sid)
    if not session:
        await callback.answer("Сессия не найдена", show_alert=True)
        return
    if session["status"] != "open":
        await callback.answer("Сессия не активна.", show_alert=True)
        return
    # Только создатель может видеть список участников
    if callback.from_user.id != session["creator_id"]:
        await callback.answer("Только ведущий (создатель сессии) может смотреть участников.", show_alert=True)
        return
    members = await get_members(sid)
    if not members:
        await callback.message.reply("Пока никто не присоединился.")
        return
    lines = [f"👥 Участники для _{session['title']}_ (`{sid}`):"]
    for m in members:
        # username / first_name / user_id (if missing)
        username = f"@{m['username']}" if m['username'] else "—"
        first_name = m["first_name"] or "—"
        user_id = str(m["user_id"])
        lines.append(f"- {username} / {first_name} / {user_id}")
    await callback.message.reply("\n".join(lines), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data and c.data.startswith("info|"))
async def cb_info(callback: types.CallbackQuery):
    await callback.answer()
    _, sid = callback.data.split("|", 1)
    session = await get_session(sid)
    if not session:
        await callback.answer("Сессия не найдена", show_alert=True)
        return
    text = (
        f"*{session['title']}*\n\n"
        "Инструкция:\n"
        "1) Нажмите 'Присоединиться и проголосовать' — бот пришлёт вам клавиатуру в личку.\n"
        "2) Выберите карту — ваш голос будет сохранён (анонимно).\n"
        "3) Ведущий сможет в любой момент показать результаты.\n\n"
        "Опции карт: " + ", ".join(VOTE_OPTIONS)
    )
    await callback.message.reply(text, parse_mode="Markdown")

# --- Export command (creator only) ---
@dp.message(Command(commands=["export_csv"]))
async def cmd_export(event: types.Message):
    # usage: /export_csv <session_id>
    args = event.command.args
    if not args:
        await event.reply("Использование: /export_csv <session_id>")
        return
    sid = args.strip()
    session = await get_session(sid)
    if not session:
        await event.reply("Сессия не найдена.")
        return
    if event.from_user.id != session["creator_id"]:
        await event.reply("Только создатель сессии может экспортировать результаты.")
        return
    votes = await get_votes(sid)
    members = await get_members(sid)
    # produce CSV local file
    filename = f"poker_{sid}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "username", "name", "value", "voted_at"])
        # join votes with member info if available
        members_map = {m["user_id"]: m for m in members}
        for v in votes:
            m = members_map.get(v["user_id"], {})
            writer.writerow([v["user_id"], m.get("username", ""), m.get("first_name", ""), v["value"], v["voted_at"]])
    # send file
    await event.reply_document(types.InputFile(filename))
    # optionally remove file
    try:
        os.remove(filename)
    except Exception:
        pass

# --- Close session command ---
@dp.message(Command(commands=["close_session"]))
async def cmd_close_session(message: Message, command: CommandObject):
    args = command.args
    if not args:
        await message.reply("Использование: /close_session <session_id>")
        return
    sid = args.strip()
    session = await get_session(sid)
    if not session:
        await message.reply("Сессия не найдена.")
        return
    if message.from_user.id != session["creator_id"]:
        await message.reply("Только создатель может закрыть сессию.")
        return
    await set_session_status(sid, "closed")
    await message.reply(f"Сессия `{sid}` закрыта.", parse_mode="Markdown")

# --- Utility to show session details by id ---
@dp.message(Command(commands=["session"]))
async def cmd_session(message: Message, command: CommandObject):
    args = command.args
    if not args:
        await message.reply("Использование: /session <session_id>")
        return
    sid = args.strip()
    session = await get_session(sid)
    if not session:
        await message.reply("Сессия не найдена.")
        return
    members = await get_members(sid)
    votes = await get_votes(sid)
    text = f"*{session['title']}* (`{sid}`)\nСтатус: {session['status']}\nСоздатель: {session['creator_name']}\nСоздано: {session['created_at']}\n\nУчастников: {len(members)}\nГолосов: {len(votes)}"
    kb = build_session_buttons(sid)
    await message.reply(text, parse_mode="Markdown", reply_markup=kb)

# --- on startup ---
async def main():
    await init_db()
    logger.info("DB initialized")
    # start polling
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    import asyncio

    try:
        asyncio.get_event_loop().run_until_complete(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped.")