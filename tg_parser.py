from pyrogram import Client
import pandas as pd
import time
import asyncio
from pyrogram.errors import FloodWait


"""

Чтобы взять сообщения с телеграм канала, используется Pyrogram.
Для его использования необходимо зарегистрировать приложение на https://my.telegram.org/apps.
Отсюда нам нужны API_ID и API_HASH.
Теперь c помощью Pyrogram мы можем выполнять любые действия от лица пользователя, с которого
зарегистрировали приложение.

Например:
 - вступать в каналы (также в приватные)
 - получать полную историю канала (что нам и нужно)
 - писать сообщения


Чтобы избежать возможной блокировки за флуд, я не вступал в закрытые каналы (можно делать это выборчно),
а также добавил исключение в случае ошибки из-за флуда. 

Хоть и тг говорит, сколько требуется подождать до следующего запроса, но факт, что после ожидания запрос пройдет.
В случае, когда тг беспрерывно велел ждать, я подключал другое приложение со второго аккаунта.

На выходе мы получаем файл data.csv, где сохраняются максимум последние 100 сообщений канала 
(можно было сохранять и всю историю, запрос к api в любом случае 1 отправляется).


Методы в Pyrogram тривиальны, поэтому код без комментариев.

"""

api_id = ""  # FROM https://my.telegram.org/apps
api_hash = ""  # FROM https://my.telegram.org/apps

app = Client("my_account", api_id=api_id, api_hash=api_hash)

df = pd.read_csv("users.csv")

channels = df[df["username"] != "приватный канал"]


async def main():
    async with app:
        texts = []
        users = 1
        flood = []
        for channel in channels:
            try:
                local_texts = [] # сообщения с одного канала
                async for message in app.get_chat_history(channel):
                    if (message.text):
                        info = {}
                        info["username"] = channel
                        info["doc"] = message.text
                        if (message.views):
                            info["views"] = message.views
                        if (message.forwards):
                            info["forwards"] = message.forwards
                        if (message.date):
                            info["date"] = int(time.mktime(message.date.timetuple()))
                        texts.append(info)
                        local_texts.append(info)
                        pd.DataFrame(texts).to_csv("data.csv")
                    if len(local_texts) > 100:
                        break
            except FloodWait as e:
                flood.append({"username": channel})
                pd.DataFrame(flood).to_csv("flood.csv")
                print(f"Жду {e.value}")
                await asyncio.sleep(e.value)
            finally:
                print(f"""{users}""")
                users += 1


app.run(main())
