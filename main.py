import asyncio
import datetime
import re

from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.types import Chat, ChatPreview
import pyrogram

# to get api credentials, follow to https://my.telegram.org/auth
API_ID = ''
API_HASH = ''
PHONE = ''

SLEEP_THRESHOLD = 10000

# For the last hour
AFTER_DATE = datetime.datetime.now() - datetime.timedelta(seconds=3600 * 240)
AFTER_DATE = None
# Except for the last 10 minutes
BEFORE_DATE = datetime.datetime.now() - datetime.timedelta(seconds=600)
BEFORE_DATE = None
# No more than (messages)
PER_CHANNEL_MSG_LIMIT = 1000000

CHANNEL_IDS = [
    "t0digital",
    # "t.me/t0digital",
    # "https://t.me/t0digital",
    # "t.me/+bGMeG-WuAbczNGI6",  # test group
    # "https://t.me/+bGMeG-WuAbczNGI6",  # test group
    # "https://t.me/tarantoolru",
    # "https://t.me/+i-i7aa0XXENiOWVi", # private channel
]

CHAT_TYPES = [ChatType.CHANNEL, ChatType.GROUP, ChatType.SUPERGROUP]


class Worker:
    async def leave_a_channel(self):
        chat_id = self.remove_queue.pop()
        # print(f"Leaving {chat_id}")
        await self.app.leave_chat(chat_id)

    async def get_channel_chats(self):
        async for dialog in self.app.get_dialogs():
            try:
                chat = dialog.chat
                assert chat.type in CHAT_TYPES
            except Exception:
                continue
            yield chat

    def __init__(self, channel_queue):
        self.channel_queue = channel_queue
        self.remove_queue = []

    async def main(self, session_name, api_id, api_hash, phone_number):
        client = Client(
            session_name,
            api_id,
            api_hash,
            phone_number=phone_number,
            sleep_threshold=SLEEP_THRESHOLD,
        )

        async with client as app:
            self.app = app

            # We're looking at channels we've already subscribed to
            async for chat in self.get_channel_chats():
                # print(chat.id, chat.title)
                if chat.id in self.channel_queue or chat.username in self.channel_queue:
                    if chat.id in self.channel_queue:
                        # print("id", chat.id)
                        self.channel_queue.remove(chat.id)
                    if chat.username in self.channel_queue:
                        # print("username", chat.username)
                        self.channel_queue.remove(chat.username)

                    await self.dump_channel_history(chat)
                self.remove_queue.append(chat.id)

            # Channels you need to join to
            while True:
                try:
                    chat_id = self.channel_queue.pop()
                except KeyError:
                    break
                if isinstance(chat_id, str) and chat_id.startswith("+"):
                    # for groups
                    chat_id = "t.me/" + chat_id
                try:
                    chat = await self.app.get_chat(chat_id)
                    if isinstance(chat, ChatPreview):
                        # private channel
                        chat = await self.join_channel(chat_id)
                        if chat is None:
                            continue
                        self.remove_queue.append(chat.id)
                except pyrogram.errors.UserAlreadyParticipant:
                    pass
                except pyrogram.errors.BadRequest as ex:
                    # Wrong ID
                    print(f"Skipping chat_id={chat_id}:", ex)
                    continue

                if chat.type in CHAT_TYPES:
                    await self.dump_channel_history(chat)

    async def join_channel(self, chat_id):
        while True:
            try:
                print("joining")
                chat = await self.app.join_chat(chat_id)
                return chat
            except pyrogram.errors.ChannelsTooMuch:
                await self.leave_a_channel()

    async def dump_channel_history(self, chat: Chat):
        kwargs = {}
        if BEFORE_DATE is not None:
            kwargs["date_offset"] = BEFORE_DATE

        async for message in self.app.get_chat_history(
            chat.id, limit=PER_CHANNEL_MSG_LIMIT, **kwargs
        ):
            if AFTER_DATE is not None and message.date < AFTER_DATE:
                break

            # print(message)
            reactions = ""
            if message.reactions is not None:
                reactions = " ".join(
                    "%s %s" % (r.emoji, r.count) for r in message.reactions.reactions
                )

            print(
                ">",
                chat.title,
                message.caption,
                message.text,
                message.date,
                "Views:",
                message.views,
                reactions,
            )


if __name__ == "__main__":
    queue = set()
    for channel in CHANNEL_IDS:
        if isinstance(channel, int):
            queue.add(channel)
            continue
        assert isinstance(channel, str)
        if m := re.match(r"(https?://)?t.me/", channel):
            channel = channel[m.end() :]
        queue.add(channel)

    asyncio.run(Worker(queue).main("client" + PHONE, API_ID, API_HASH, PHONE))
