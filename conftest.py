import os
import sys

# Директория, где лежит BotGGpokerMain.py
# (ваша структура: GGpoker Bot Telegram/ → GGpoker Bot/BotGGpokerMain.py)
BOT_DIR = os.path.join(os.path.dirname(__file__), "GGpoker Bot")

# Добавляем её в начало sys.path
sys.path.insert(0, BOT_DIR)
