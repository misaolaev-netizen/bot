import asyncio
import json
import logging
import aiohttp
import traceback
import sys
from pathlib import Path
from urllib.parse import quote_plus
from aiogram import Bot, Dispatcher, types
from aiogram.client.bot import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

TOKEN = "8664666780:AAGt52dKdAik8KL4rVfytfMnCKph7vCM7ps"
TIMEPAD_TOKEN = "91a933aa403fbf4aefab3be8756f7281b8dc74b7"
ADMIN_ID = [7708240066]

CITIES = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург"]
broadcast_mode = {}
CATEGORY_LIST = ["Концерты", "Искусство и культура", "Экскурсии и путешествия"]
CACHED_CATEGORIES = []
DATA_FILE = Path("bot_data.json")
GLOBAL_AIO_SESSION = None


class Database:
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.data = {"users": {}, "history": []}

    async def connect(self):
        if self.filepath.exists():
            try:
                self.data = json.loads(self.filepath.read_text(encoding="utf-8"))
            except Exception:
                self.data = {"users": {}, "history": []}
                self._save()
        else:
            self._save()

    async def close(self):
        self._save()

    def _save(self):
        self.filepath.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    async def execute(self, query, params=None):
        params = params or ()
        q = " ".join(query.lower().split())

        if q.startswith("insert ignore into users"):
            user_id, username = params
            key = str(user_id)
            if key not in self.data["users"]:
                self.data["users"][key] = {
                    "id": user_id,
                    "username": username,
                    "city": None,
                }
                self._save()
            return ()

        if q.startswith("update users set city="):
            city, user_id = params
            key = str(user_id)
            if key not in self.data["users"]:
                self.data["users"][key] = {
                    "id": user_id,
                    "username": None,
                    "city": city,
                }
            else:
                self.data["users"][key]["city"] = city
            self._save()
            return ()

        if q.startswith("select id from users"):
            return [(int(uid),) for uid in self.data["users"].keys()]

        if q.startswith("insert into history"):
            user_id, event_name, event_date, city = params
            self.data["history"].append({
                "user_id": user_id,
                "event_name": event_name,
                "event_date": event_date,
                "city": city,
            })
            self._save()
            return ()

        raise NotImplementedError(f"Unsupported execute query: {query}")

    async def execute_one(self, query, params=None):
        params = params or ()
        q = " ".join(query.lower().split())

        if q.startswith("select city from users where id="):
            user_id = params[0]
            user = self.data["users"].get(str(user_id))
            if not user:
                return None
            return (user.get("city"),)

        if q.startswith("select count(*) from users"):
            return (len(self.data["users"]),)

        result = await self.execute(query, params)
        return result[0] if result else None


db = Database(DATA_FILE)

logging.basicConfig(level=logging.ERROR)
for _log in ("aiogram", "aiohttp", "asyncio"):
    logging.getLogger(_log).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


async def create_aio_session(timeout_seconds: int = 10, limit: int = 20):
    global GLOBAL_AIO_SESSION
    if GLOBAL_AIO_SESSION and not GLOBAL_AIO_SESSION.closed:
        return GLOBAL_AIO_SESSION
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    connector = aiohttp.TCPConnector(limit=limit)
    GLOBAL_AIO_SESSION = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return GLOBAL_AIO_SESSION


async def close_aio_session():
    global GLOBAL_AIO_SESSION
    try:
        if GLOBAL_AIO_SESSION and not GLOBAL_AIO_SESSION.closed:
            await GLOBAL_AIO_SESSION.close()
    except Exception:
        logger.exception("Ошибка при закрытии GLOBAL_AIO_SESSION")
    finally:
        GLOBAL_AIO_SESSION = None


async def notify_admins(text: str):
    if not text:
        return
    for aid in ADMIN_ID:
        try:
            await bot.send_message(aid, text[:4000])
        except Exception:
            logger.exception(f"Не удалось отправить уведомление админу {aid}")


def handle_loop_exception(loop, context):
    try:
        exc = context.get("exception")
        msg = context.get("message")
        if exc:
            tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            text = f"⚠️ Unhandled exception in event loop:\n{tb}"
        else:
            text = f"⚠️ Event loop error: {msg}"
        logger.error(text)
    except Exception:
        logger.exception("Ошибка в глобальном обработчике исключений")


async def safe_edit(message: types.Message, text: str, reply_markup=None):
    try:
        if message.text != text or message.reply_markup != reply_markup:
            await message.edit_text(text, reply_markup=reply_markup)
    except Exception as e:
        if "message is not modified" in str(e):
            pass
        else:
            await message.answer(text, reply_markup=reply_markup)


def main_menu(user_id: int):
    buttons = [
        [InlineKeyboardButton(text="🌆 Выбрать город", callback_data="choose_city")],
        [InlineKeyboardButton(text="🗂 Категории", callback_data="categories")]
    ]
    if user_id in ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="🛠 Админ панель", callback_data="admin")])
        buttons.append([InlineKeyboardButton(text="/refresh", callback_data="refresh")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def fetch_categories_from_api(max_events: int = 200):
    headers = {"Authorization": f"Bearer {TIMEPAD_TOKEN}", "Accept": "application/json"}
    session = GLOBAL_AIO_SESSION or await create_aio_session()
    try:
        async with session.get("https://api.timepad.ru/v1/categories", headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                values = data.get("values") if isinstance(data, dict) else data
                cats = []
                if values:
                    for it in values:
                        if isinstance(it, dict):
                            name = it.get("name") or it.get("title")
                            slug = it.get("slug") or it.get("id")
                            if name:
                                cats.append({"name": name, "slug": str(slug) if slug else name})
                if cats:
                    return cats
    except asyncio.TimeoutError:
        logger.warning("Timeout при запросе /v1/categories")
    except Exception:
        logger.exception("Ошибка при вызове /v1/categories")

    try:
        cats_map = {}
        per_page = 100
        fetched = 0
        while fetched < max_events:
            params = {"limit": per_page, "skip": fetched, "fields": ["category", "categories", "tags"]}
            async with session.get("https://api.timepad.ru/v1/events", headers=headers, params=params) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
                values = data.get("values", []) if isinstance(data, dict) else data
                if not values:
                    break
                for item in values:
                    raw = item.get("category")
                    if raw:
                        if isinstance(raw, dict):
                            n = raw.get("name") or raw.get("title")
                            sid = raw.get("id")
                            if n:
                                cats_map[n] = str(sid) if sid else n
                        elif isinstance(raw, str):
                            cats_map[raw] = raw
                    raw_list = item.get("categories") or item.get("tags")
                    if raw_list and isinstance(raw_list, (list, tuple)):
                        for rc in raw_list:
                            if isinstance(rc, dict):
                                n = rc.get("name") or rc.get("title")
                                sid = rc.get("id")
                                if n:
                                    cats_map[n] = str(sid) if sid else n
                            elif isinstance(rc, str):
                                cats_map[rc] = rc
                fetched += len(values)
        return [{"name": name, "slug": slug or name} for name, slug in cats_map.items()]
    except asyncio.TimeoutError:
        logger.warning("Timeout при сборе категорий через /v1/events")
        return []
    except Exception:
        logger.exception("Ошибка при сборе категорий через /v1/events")
        return []


@dp.callback_query(lambda c: c.data == "refresh")
async def refresh_menu(callback: types.CallbackQuery):
    await safe_edit(callback.message, "Обновляем меню...", reply_markup=main_menu(callback.from_user.id))


@dp.message(Command("start"))
async def start(message: types.Message):
    result = await db.execute_one("SELECT city FROM users WHERE id=%s", (message.from_user.id,))
    if not result:
        await db.execute("INSERT IGNORE INTO users (id, username) VALUES (%s,%s)",
                         (message.from_user.id, message.from_user.username))
    if result and result[0]:
        await message.answer(f"👋 Привет! Ваш город: {result[0]}", reply_markup=main_menu(message.from_user.id))
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=city, callback_data=f"city_{city}")] for city in CITIES
        ])
        await message.answer("👋 Сначала выберите свой город:", reply_markup=kb)


@dp.callback_query(lambda c: c.data.startswith("city_"))
async def set_city(callback: types.CallbackQuery):
    city = callback.data.split("_", 1)[1]
    await db.execute("UPDATE users SET city=%s WHERE id=%s", (city, callback.from_user.id))
    await safe_edit(callback.message, f"🌆 Город установлен: {city}")
    await callback.message.answer("Выберите действие:", reply_markup=main_menu(callback.from_user.id))


@dp.callback_query(lambda c: c.data == "choose_city")
async def choose_city(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=city, callback_data=f"city_{city}")] for city in CITIES
    ])
    await safe_edit(callback.message, "🌆 Выберите город:", reply_markup=kb)


@dp.callback_query(lambda c: c.data == "categories")
async def show_categories(callback: types.CallbackQuery):
    global CACHED_CATEGORIES
    if not CACHED_CATEGORIES:
        cats = await fetch_categories_from_api()
        if cats:
            CACHED_CATEGORIES = [{'name': c.get('name') or str(c.get('slug') or c), 'slug': c.get('slug') or c.get('name') or str(c), 'url': None} for c in cats]
        else:
            CACHED_CATEGORIES = [{'name': n, 'slug': n.replace(' ', '-').lower(), 'url': None} for n in CATEGORY_LIST]

    kb_rows = []
    for i, c in enumerate(CACHED_CATEGORIES[:30]):
        kb_rows.append([InlineKeyboardButton(text=c.get('name'), callback_data=f"cat_{i}")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Главное меню", callback_data="menu")])
    await safe_edit(callback.message, "Выберите категорию событий:", reply_markup=kb)


@dp.callback_query(lambda c: c.data.startswith("cat_"))
async def category_selected(callback: types.CallbackQuery):
    try:
        idx = int(callback.data.split('_', 1)[1])
    except Exception:
        await safe_edit(callback.message, "❌ Неизвестная категория.")
        return
    if idx < 0 or idx >= len(CACHED_CATEGORIES):
        await safe_edit(callback.message, "❌ Неизвестная категория.")
        return
    category_obj = CACHED_CATEGORIES[idx]
    category = category_obj.get('slug') or category_obj.get('name')
    result = await db.execute_one("SELECT city FROM users WHERE id=%s", (callback.from_user.id,))
    city = result[0] if result else None
    events = await get_timepad_events(city=city, category=category, limit=10)
    await send_events(callback.message, events, city, selected_category=category_obj.get('name'))


@dp.callback_query(lambda c: c.data == "menu")
async def back_to_menu(callback: types.CallbackQuery):
    await safe_edit(callback.message, "Выберите действие:", reply_markup=main_menu(callback.from_user.id))


@dp.callback_query(lambda c: c.data == "admin")
async def admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_ID:
        await safe_edit(callback.message, "❌ У вас нет доступа к админ-панели.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Количество пользователей", callback_data="admin_users")],
        [InlineKeyboardButton(text="✉️ Рассылка всем", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🔙 Главное меню", callback_data="menu")]
    ])
    await safe_edit(callback.message, "🛠 Админ-панель:", reply_markup=kb)


@dp.callback_query(lambda c: c.data == "admin_users")
async def show_users(callback: types.CallbackQuery):
    result = await db.execute_one("SELECT COUNT(*) FROM users")
    total = result[0]
    await safe_edit(callback.message, f"📊 Всего пользователей: {total}", reply_markup=main_menu(callback.from_user.id))


@dp.callback_query(lambda c: c.data == "admin_broadcast")
async def broadcast_prompt(callback: types.CallbackQuery):
    broadcast_mode[callback.from_user.id] = {"text": None, "confirm_msg": None}
    await safe_edit(callback.message, "✉️ Отправьте сообщение для рассылки всем пользователям:")


@dp.message(lambda m: m.from_user.id in broadcast_mode)
async def receive_broadcast_text(message: types.Message):
    broadcast_mode[message.from_user.id]["text"] = message.text
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, отправить", callback_data="confirm_broadcast"),
            InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_broadcast")
        ]
    ])
    confirm_msg = await message.answer(f"Вы уверены, что хотите отправить это сообщение всем?\n\n{message.text}", reply_markup=kb)
    broadcast_mode[message.from_user.id]["confirm_msg"] = confirm_msg.message_id


@dp.callback_query(lambda c: c.data == "confirm_broadcast")
async def confirm_broadcast(callback: types.CallbackQuery):
    data = broadcast_mode.get(callback.from_user.id)
    if not data:
        return
    users = await db.execute("SELECT id FROM users")
    for user_id in users:
        try:
            await bot.send_message(user_id[0], f"📢 Админ: {data['text']}")
        except Exception:
            continue
    await safe_edit(callback.message, "✅ Рассылка завершена.", reply_markup=main_menu(callback.from_user.id))
    broadcast_mode.pop(callback.from_user.id, None)


@dp.callback_query(lambda c: c.data == "cancel_broadcast")
async def cancel_broadcast(callback: types.CallbackQuery):
    await safe_edit(callback.message, "❌ Рассылка отменена.", reply_markup=main_menu(callback.from_user.id))
    broadcast_mode.pop(callback.from_user.id, None)


async def get_timepad_events(city=None, limit=10, category=None):
    global GLOBAL_AIO_SESSION
    url = "https://api.timepad.ru/v1/events"
    headers = {"Authorization": f"Bearer {TIMEPAD_TOKEN}", "Accept": "application/json"}
    params = {
        "limit": limit,
        "skip": 0,
        "fields": ["name", "starts_at", "url", "location", "category", "categories", "tags"],
        "sort": "+starts_at",
        "status": "public",
    }
    # город в API не фильтруем жёстко, иначе часто пустой результат
    if category:
        params["categories"] = category

    try:
        session = GLOBAL_AIO_SESSION or await create_aio_session()
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error(f"Timepad API returned status {resp.status}: {text}")
                return []
            data = await resp.json()

            events = []
            for item in data.get("values", []):
                location = item.get("location", {}) or {}
                address = location.get("address", "Не указано")
                cats = []
                raw_cat = item.get("category")
                if raw_cat:
                    if isinstance(raw_cat, dict):
                        n = raw_cat.get("name")
                        if n:
                            cats.append(n)
                    elif isinstance(raw_cat, str):
                        cats.append(raw_cat)
                raw_cats = item.get("categories") or item.get("tags")
                if raw_cats and isinstance(raw_cats, (list, tuple)):
                    for rc in raw_cats:
                        if isinstance(rc, dict):
                            n = rc.get("name")
                            if n:
                                cats.append(n)
                        elif isinstance(rc, str):
                            cats.append(rc)

                cats = [c.strip() for c in cats if c and isinstance(c, str)]
                events.append({
                    "name": item.get("name", "Без названия"),
                    "date": item.get("starts_at", "")[:16].replace("T", " "),
                    "url": item.get("url", ""),
                    "address": address,
                    "categories": cats,
                    "location_obj": location,
                })

            if category:
                cat_l = str(category).lower()
                filtered = [
                    ev for ev in events
                    if any(cat_l in (c or "").lower() for c in ev.get("categories", []))
                    or cat_l in ev.get("name", "").lower()
                ]
                if filtered:
                    return filtered
            return events
    except asyncio.TimeoutError:
        logger.warning("Timeout при запросе к Timepad API")
        return []
    except aiohttp.ClientError:
        logger.exception("HTTP ошибка при запросе к Timepad API")
        return []
    except Exception:
        logger.exception("Неожиданная ошибка при запросе к Timepad API")
        return []


async def send_events(message: types.Message, events, city=None, selected_category=None):
    if not events:
        await safe_edit(message, "😕 Событий не найдено.", reply_markup=main_menu(message.from_user.id))
        return

    header_parts = []
    if selected_category:
        header_parts.append(f"Категория: {selected_category}")
    if city:
        header_parts.append(f"Город: {city}")
    header = ' | '.join(header_parts)
    text = f"🎫 Подборка событий{(' — ' + header) if header else ''}:\n\n"
    for e in events[:10]:
        addr = e.get('address', '') or ''
        location_obj = e.get('location_obj') or {}
        lat = location_obj.get('latitude') or location_obj.get('lat')
        lon = location_obj.get('longitude') or location_obj.get('lon') or location_obj.get('lng')

        if lat and lon:
            try:
                map_link = f"https://yandex.ru/maps/?ll={float(lon)}%2C{float(lat)}&z=16"
            except Exception:
                map_link = f"https://yandex.ru/maps/?text={quote_plus(addr)}"
        elif addr and addr != "Не указано":
            map_link = f"https://yandex.ru/maps/?text={quote_plus(addr)}"
        else:
            fallback_q = quote_plus(f"{e.get('name')} {city or ''}".strip())
            map_link = f"https://yandex.ru/maps/?text={fallback_q}"

        text += f"• <b>{e.get('name')}</b>\n"
        if e.get('date'):
            text += f"  🕒 {e.get('date')}\n"
        text += f"  📌 <a href='{map_link}'>Адрес</a>\n"
        if e.get('url'):
            text += f"  🌐 <a href='{e.get('url')}'>Ссылка</a>\n"
        cats = e.get('categories') or []
        shown_type = selected_category or (cats[0] if cats else None)
        if shown_type:
            text += f"  🏷 Тип: {shown_type}\n"
        if city:
            text += f"  📍 Город: {city}\n"
        text += "\n"
        try:
            await db.execute(
                "INSERT INTO history (user_id, event_name, event_date, city) VALUES (%s,%s,%s,%s)",
                (message.chat.id, e['name'], e['date'], city)
            )
        except Exception:
            pass

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Главное меню", callback_data="menu")]])
    await safe_edit(message, text, reply_markup=kb)


async def main():
    await db.connect()
    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(handle_loop_exception)

        def _excepthook(exc_type, exc, tb):
            try:
                text = ''.join(traceback.format_exception(exc_type, exc, tb))
                logger.error(f"Uncaught exception: {text}")
            except Exception:
                logger.exception("Ошибка в custom excepthook")

        sys.excepthook = _excepthook
        await create_aio_session(timeout_seconds=10, limit=20)
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await db.close()
        try:
            await close_aio_session()
        except Exception:
            logger.exception("Ошибка при закрытии aio session")
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
