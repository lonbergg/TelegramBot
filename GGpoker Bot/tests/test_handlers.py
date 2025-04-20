import os
import sys

# 1) Путь до папки с вашим BotGGpokerMain.py — это ровно один уровень вверх от tests/
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# 2) Добавляем эту папку в sys.path
sys.path.insert(0, ROOT)

import pytest
import asyncio
from datetime import datetime

# Импортируем BotGGpokerMain как модуль бота
import BotGGpokerMain as bot_module

# Вспомогательная модель пользователя
class DummyUser:
    def __init__(self, user_id, username=None, first_name='', last_name=None):
        self.id = user_id
        self.username = username
        self.first_name = first_name
        self.last_name = last_name

@pytest.mark.asyncio
async def test_participate_and_full_registration_flow(monkeypatch):
    # 1) Тестируем participate_command
    user = DummyUser(1, None, "First")
    messages = []

    class DummyMessage:
        def __init__(self):
            self.from_user = user
        async def answer(self, text, **kwargs):
            messages.append(text)

    dm = DummyMessage()
    await bot_module.participate_command(dm)
    assert any("Обробляємо" in m for m in messages)
    assert any("Для участі" in m for m in messages)

    # 2) Проверяем check_subscription — успешная подписка
    class Member:
        status = "member"
    # get_chat_member должен вернуть Future с этим Member
    monkeypatch.setattr(bot_module.bot, "get_chat_member", lambda ch, uid: asyncio.Future())
    fut = bot_module.bot.get_chat_member(None, None)
    fut.set_result(Member())

    class DummyCallback:
        def __init__(self):
            self.from_user = user
            self.message = dm
            self.answers = []
        async def answer(self, text, **kw):
            self.answers.append(text)

    cb = DummyCallback()
    await bot_module.check_subscription(cb)
    assert bot_module.user_states[user.id] == "awaiting_nickname"

    # 3) Шаг ввода никнейма и email
    dm2 = DummyMessage(); dm2.text = "my_nick"
    await bot_module.handle_messages(dm2)
    dm3 = DummyMessage(); dm3.text = "my@mail.com"
    await bot_module.handle_messages(dm3)
    assert bot_module.user_states[user.id]["step"] == "confirming"

    # 4) Подтверждение участия
    cb2 = DummyCallback(); cb2.data = "confirm_participation"; cb2.message = dm
    # Устанавливаем состояние вручную, как будто пользователь нажал кнопку
    bot_module.user_states[user.id] = {
        "step": "confirming",
        "nickname": "my_nick",
        "email": "my@mail.com",
    }
    await bot_module.confirm_participation(cb2)
    assert user.id not in bot_module.user_states

@pytest.mark.asyncio
async def test_handle_spam_prevention():
    # Антиспам: два сообщения подряд должны пройти без ошибок, но второй игнорироваться
    user = DummyUser(2, None, "Spam")
    class M: pass

    m = type("X", (object,), {
        "from_user": user,
        "text": "hi",
        "answer": lambda *a, **k: None
    })()

    # Первый вызов — обрабатывается
    await bot_module.handle_messages(m)
    # Второй почти сразу — просто ничего не делает (но и не падает)
    await bot_module.handle_messages(m)

    # Состояние пользователя не меняется
    assert user.id not in bot_module.user_states
