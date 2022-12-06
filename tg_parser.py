from pyrogram import Client
import pandas as pd
import time
import asyncio
from pyrogram.errors import FloodWait

api_id = ""  # FROM TELEGRAM API
api_hash = ""  # FROM TELEGRAM API

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
                local_texts = []
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
                        pd.DataFrame(texts).to_csv("docs-buz-topor.csv")
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
