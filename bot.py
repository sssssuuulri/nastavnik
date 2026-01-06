import os
import json
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils import executor
from dotenv import load_dotenv
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from datetime import datetime, date
import shutil
import hashlib

# --- ЛОГИ ---
logger = logging.getLogger("bot_logger")
logger.setLevel(logging.INFO)
executor_log = ThreadPoolExecutor(max_workers=1)
file_handler = logging.FileHandler("bot.log", encoding="utf-8")
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def log_info(message: str):
    executor_log.submit(logger.info, message)

def log_error(message: str):
    executor_log.submit(logger.error, message)

# --- TOKEN ---
load_dotenv()
API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise ValueError("Не найден BOT_TOKEN в .env")

bot = Bot(token=API_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

USERS_FILE = "users.json"
LEVELS_ORDER = ["НП", "СВ", "ВТ", "АВТ", "ГТ"]
OLGA_ID = 64434196
YOUR_ADMIN_ID = 911511438
REPORT_GROUP_ID = "-1003632130674"

# --- УЛУЧШЕННАЯ БЕЗОПАСНАЯ ЗАГРУЗКА И СОХРАНЕНИЕ ---
def recover_corrupted_file():
    """Восстановление поврежденного файла из backup"""
    backups = [f for f in os.listdir('.') 
              if f.startswith('users_backup_') and f.endswith('.json')]
    
    if backups:
        backups.sort(reverse=True)  # Сортируем по времени (новые сначала)
        latest_backup = backups[0]
        
        try:
            shutil.copy2(latest_backup, USERS_FILE)
            log_info(f"🔄 Восстановлено из backup: {latest_backup}")
            
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            return data
        except Exception as e:
            log_error(f"❌ Не удалось восстановить из backup: {e}")
    
    # Создаем новый файл
    log_info("Создаю новый файл users.json")
    return {"users": {}}

def load_users():
    """Загрузка пользователей с автоматическим исправлением проблем"""
    if not os.path.exists(USERS_FILE):
        log_info("Файл users.json не найден, создается новый")
        return {"users": {}}
    
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            
        if not content:  # Если файл пустой
            log_error("Файл users.json пустой")
            return {"users": {}}
            
        data = json.loads(content)
        
        # Проверяем структуру
        if "users" not in data:
            log_error("Некорректная структура users.json: отсутствует ключ 'users'")
            return {"users": {}}
        
        # Исправляем данные при загрузке
        users = data["users"]
        fixed_count = 0
        duplicates_removed = 0
        
        for user_id in list(users.keys()):
            user = users[user_id]
            
            # Проверяем что это словарь
            if not isinstance(user, dict):
                log_error(f"❌ Некорректный формат пользователя {user_id}, удаляю")
                del users[user_id]
                continue
                
            # Проверяем обязательные поля
            if not user.get("name"):
                log_error(f"❌ Пользователь {user_id} без имени, удаляю")
                del users[user_id]
                continue
            
            # Исправляем chat_id если не совпадает
            chat_id_in_data = user.get("chat_id")
            if chat_id_in_data and chat_id_in_data != user_id:
                # Проверяем, есть ли уже пользователь с таким chat_id
                if chat_id_in_data in users:
                    # Уже есть пользователь с таким chat_id, удаляем дубликат
                    log_info(f"Удаляю дубликат: {user_id} (совпадает с {chat_id_in_data})")
                    del users[user_id]
                    duplicates_removed += 1
                else:
                    # Исправляем chat_id
                    user["chat_id"] = user_id
                    fixed_count += 1
            elif not chat_id_in_data:
                # Добавляем отсутствующий chat_id
                user["chat_id"] = user_id
                fixed_count += 1
        
        if fixed_count > 0:
            log_info(f"Исправлено {fixed_count} chat_id")
        if duplicates_removed > 0:
            log_info(f"Удалено {duplicates_removed} дубликатов")
        
        user_count = len(users)
        log_info(f"✅ Загружено пользователей: {user_count}")
        return data
        
    except json.JSONDecodeError as e:
        log_error(f"❌ Файл users.json поврежден (невалидный JSON): {e}")
        
        # Создаем backup поврежденного файла
        try:
            backup_name = f"users_corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            shutil.copy2(USERS_FILE, backup_name)
            log_info(f"📂 Создан backup поврежденного файла: {backup_name}")
        except:
            pass
            
        # Пытаемся восстановить из backup
        return recover_corrupted_file()
        
    except Exception as e:
        log_error(f"❌ Ошибка загрузки users.json: {e}")
        return {"users": {}}

def save_users(data):
    """Сохранение пользователей с атомарной операцией"""
    if "users" not in data:
        log_error("❌ Попытка сохранить данные без ключа 'users'")
        return False
    
    user_count = len(data["users"])
    log_info(f"🔄 Сохранение {user_count} пользователей...")
    
    # Backup текущего файла
    backup_name = None
    if os.path.exists(USERS_FILE):
        try:
            backup_name = f"users_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            shutil.copy2(USERS_FILE, backup_name)
            log_info(f"📂 Создан backup: {backup_name}")
        except Exception as e:
            log_error(f"⚠️ Не удалось создать backup: {e}")
    
    # Сохраняем во временный файл
    temp_file = f"{USERS_FILE}.tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Проверяем что сохранили корректно
        with open(temp_file, "rb") as f:
            temp_hash = hashlib.md5(f.read()).hexdigest()
        
        # Проверяем что можем загрузить обратно
        with open(temp_file, "r", encoding="utf-8") as f:
            temp_data = json.load(f)
        
        if "users" not in temp_data:
            raise ValueError("Временный файл не содержит ключ 'users'")
        
        # Атомарная замена
        if os.name == 'nt':  # Windows
            os.replace(temp_file, USERS_FILE)
        else:  # Unix/Linux
            os.rename(temp_file, USERS_FILE)
        
        log_info(f"✅ Сохранено {user_count} пользователей")
        return True
        
    except Exception as e:
        log_error(f"❌ Ошибка сохранения: {e}")
        
        # Восстанавливаем из backup если есть
        if backup_name and os.path.exists(backup_name):
            try:
                shutil.copy2(backup_name, USERS_FILE)
                log_info(f"🔄 Восстановлено из backup: {backup_name}")
            except Exception as restore_error:
                log_error(f"❌ Не удалось восстановить из backup: {restore_error}")
        
        # Удаляем временный файл
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        
        return False

# --- МЕНЮ КОМАНД ---
async def set_bot_commands():
    commands = [
        types.BotCommand("start", "🔄 Перезапустить бота"),
        types.BotCommand("profile", "👤 Мой профиль"),
        types.BotCommand("students", "👥 Мои ученики"),
        types.BotCommand("help", "❓ Помощь"),
        types.BotCommand("menu", "📋 Главное меню")
    ]
    
    admin_commands = commands + [
        types.BotCommand("admin", "👑 Админ-панель"),
        types.BotCommand("stats", "📊 Статистика"),
        types.BotCommand("broadcast", "📢 Рассылка"),
        types.BotCommand("check_data", "🔧 Проверить данные"),
        types.BotCommand("fix_data", "🛠 Исправить данные")
    ]
    
    await bot.set_my_commands(commands)
    
    admin_ids = [YOUR_ADMIN_ID, OLGA_ID]
    for admin_id in admin_ids:
        await bot.set_my_commands(
            admin_commands,
            scope=types.BotCommandScopeChat(chat_id=admin_id)
        )
    
    print(f"✅ Команды настроены для {len(admin_ids)} администраторов")

# --- STATES ---
class Form(StatesGroup):
    get_name = State()
    get_surname = State()
    choose_level = State()
    choose_mentor = State()
    sending = State()
    admin_message = State()
    admin_choose_levels = State()

# --- АДМИН МЕНЮ ---
async def admin_main_menu(user_id):
    kb = InlineKeyboardMarkup(row_width=1)
    
    kb.add(InlineKeyboardButton("👑 Админ-панель", callback_data="admin_panel"))
    kb.add(InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile"))
    kb.add(InlineKeyboardButton("👥 Мои ученики", callback_data="show_my_students"))
    kb.add(InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast"))
    
    if user_id == YOUR_ADMIN_ID:
        kb.add(InlineKeyboardButton("🌐 Все пользователи", callback_data="all_users"))
        kb.add(InlineKeyboardButton("🗺 Полная иерархия", callback_data="full_hierarchy"))
    
    await bot.send_message(
        user_id,
        "🛠 <b>Панель администратора</b>\n\n"
        "Доступны все функции управления ботом.",
        reply_markup=kb
    )

# --- ОБРАБОТЧИК АДМИН-ПАНЕЛИ ---
@dp.callback_query_handler(lambda c: c.data == "admin_panel")
async def admin_panel_handler(callback):
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    kb.add(
        InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton("🗺 Иерархия", callback_data="full_hierarchy")
    )
    kb.add(
        InlineKeyboardButton("📈 Активность", callback_data="admin_activity"),
        InlineKeyboardButton("🆕 Новые", callback_data="admin_new_today")
    )
    
    if callback.from_user.id == YOUR_ADMIN_ID:
        kb.add(
            InlineKeyboardButton("👥 Все юзеры", callback_data="all_users"),
            InlineKeyboardButton("🔍 Поиск", callback_data="admin_search")
        )
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_to_admin_main"))
    
    await callback.message.answer(
        "👑 <b>Админ-панель управления</b>\n\n"
        "Выберите действие:",
        reply_markup=kb
    )

@dp.callback_query_handler(lambda c: c.data == "back_to_admin_main")
async def back_to_admin_main(callback):
    if callback.from_user.id in [OLGA_ID, YOUR_ADMIN_ID]:
        await admin_main_menu(callback.from_user.id)

@dp.callback_query_handler(lambda c: c.data == "admin_stats")
async def admin_stats(callback):
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        return
    
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    total = len(users)
    new_today = sum(1 for u in users.values() if u.get("registration_date") == today_str)
    active_today = sum(1 for u in users.values() if u.get("active_today") == today_str)
    with_mentor = sum(1 for u in users.values() if u.get("mentor"))
    without_mentor = total - with_mentor
    
    text = f"📊 <b>Статистика бота</b>\n\n"
    text += f"• Всего пользователей: {total}\n"
    text += f"• Новых сегодня: {new_today}\n"
    text += f"• Активных сегодня: {active_today}\n"
    text += f"• С наставником: {with_mentor}\n"
    text += f"• Без наставника: {without_mentor}\n\n"
    
    text += "<b>По уровням:</b>\n"
    for level in LEVELS_ORDER:
        level_users = [u for u in users.values() if u.get("level") == level]
        level_active = sum(1 for u in level_users if u.get("active_today") == today_str)
        text += f"• {level}: {len(level_users)} чел. (активных: {level_active})\n"
    
    await callback.message.answer(text)

@dp.callback_query_handler(lambda c: c.data == "admin_activity")
async def admin_activity(callback):
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        return
    
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    active_users = []
    inactive_users = []
    
    for uid, u in users.items():
        full_name = f"{u['name']} {u.get('surname','')}".strip()
        if u.get("active_today") == today_str:
            active_users.append(full_name)
        else:
            inactive_users.append(full_name)
    
    text = f"📈 <b>Активность пользователей ({today_str})</b>\n\n"
    text += f"<b>✅ Активны ({len(active_users)}):</b>\n"
    text += ", ".join(active_users) if active_users else "—"
    text += f"\n\n<b>❌ Не активны ({len(inactive_users)}):</b>\n"
    text += ", ".join(inactive_users) if inactive_users else "—"
    
    await callback.message.answer(text[:4000])

@dp.callback_query_handler(lambda c: c.data == "admin_new_today")
async def admin_new_today(callback):
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        return
    
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    new_users = []
    for uid, u in users.items():
        if u.get("registration_date") == today_str:
            full_name = f"{u['name']} {u.get('surname','')}".strip()
            mentor_info = ""
            if u.get("mentor") and u["mentor"] in users:
                mentor = users[u["mentor"]]
                mentor_name = f"{mentor['name']} {mentor.get('surname', '')}".strip()
                if mentor_name.strip():
                    mentor_info = f" → {mentor_name}"
            new_users.append(f"{full_name}{mentor_info}")
    
    text = f"🆕 <b>Новые пользователи сегодня ({today_str})</b>\n\n"
    if new_users:
        for i, user_info in enumerate(new_users, 1):
            text += f"{i}. {user_info}\n"
    else:
        text += "Сегодня новых пользователей не зарегистрировано."
    
    await callback.message.answer(text)

@dp.callback_query_handler(lambda c: c.data == "admin_search")
async def admin_search(callback):
    if callback.from_user.id != YOUR_ADMIN_ID:
        await callback.answer("Доступ только для суперадмина", show_alert=True)
        return
    
    await callback.message.answer("🔍 <b>Поиск пользователя</b>\n\nВведите имя, фамилию или ID пользователя:")
    await callback.answer("Функция в разработке", show_alert=True)

# --- НОВЫЕ КОМАНДЫ ДЛЯ АДМИНА ---
@dp.message_handler(commands=["check_data"], state="*")
async def check_data_command(message: types.Message, state=None):
    """Проверка целостности данных"""
    if message.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await message.answer("⚠️ Команда только для администраторов")
        return
    
    data = load_users()
    users = data["users"]
    
    issues = []
    
    # Проверяем каждого пользователя
    for user_id, user in users.items():
        # 1. Проверка chat_id
        if user.get("chat_id") != user_id:
            issues.append(f"❌ {user.get('name')}: chat_id не совпадает (ключ: {user_id}, значение: {user.get('chat_id')})")
        
        # 2. Проверка обязательных полей
        if not user.get("name"):
            issues.append(f"❌ ID {user_id}: нет имени")
        
        # 3. Проверка наставников
        mentor_id = user.get("mentor")
        if mentor_id and mentor_id not in users:
            issues.append(f"⚠️ {user.get('name')}: наставник {mentor_id} не существует")
        
        # 4. Проверка на дублирование
        for other_id, other_user in users.items():
            if user_id != other_id and user.get("chat_id") == other_user.get("chat_id"):
                issues.append(f"🚫 Дубликат: {user.get('name')} (ID: {user_id}) и {other_user.get('name')} (ID: {other_id}) имеют одинаковый chat_id")
                break
    
    if not issues:
        await message.answer(f"✅ Всего пользователей: {len(users)}\n✅ Данные в порядке")
    else:
        text = f"🔍 Найдено проблем: {len(issues)}\n\n"
        text += "\n".join(issues[:20])  # Показываем первые 20 проблем
        if len(issues) > 20:
            text += f"\n... и еще {len(issues)-20} проблем"
        
        await message.answer(text)

@dp.message_handler(commands=["fix_data"], state="*")
async def fix_data_command(message: types.Message, state=None):
    """Автоматическое исправление данных"""
    if message.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await message.answer("⚠️ Команда только для администраторов")
        return
    
    await message.answer("🔄 Начинаю проверку и исправление данных...")
    
    data = load_users()
    users = data["users"]
    original_count = len(users)
    
    # Просто загружаем и сохраняем - в load_users уже есть исправления
    if save_users(data):
        new_count = len(data["users"])
        await message.answer(f"✅ Данные исправлены\n\n• Было: {original_count}\n• Стало: {new_count}")
    else:
        await message.answer("❌ Не удалось исправить данные")

# --- BUTTON: ОБЫЧНОЕ МЕНЮ НАСТАВНИКА ---
async def mentor_main_menu(user_id):
    if user_id in [OLGA_ID, YOUR_ADMIN_ID]:
        await admin_main_menu(user_id)
        return
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile"))
    kb.add(InlineKeyboardButton("👥 Мои ученики", callback_data="show_my_students"))
    await bot.send_message(user_id, "Ваши функции наставника:", reply_markup=kb)

# --- КОМАНДЫ МЕНЮ ---
@dp.message_handler(commands=["help"], state="*")
async def help_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    user_id = message.from_user.id
    today_str = str(date.today())
    data = load_users()
    users = data["users"]
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)
    
    help_text = """
<b>📚 Справка по командам бота:</b>

<b>Основные команды:</b>
/start - 🔄 Перезапустить бота
/menu - 📋 Открыть главное меню
/profile - 👤 Показать мой профиль
/students - 👥 Показать моих учеников
/help - ❓ Показать эту справку

<b>Для наставников:</b>
• Вы можете просматривать своих учеников
• Подтверждать заявки новых учеников

<b>Для администраторов:</b>
• Доступны дополнительные команды (/admin, /stats, /broadcast, /check_data, /fix_data)
• Управление статистикой и рассылками
• Проверка и исправление данных
    """
    await message.answer(help_text)

@dp.message_handler(commands=["menu"], state="*")
async def menu_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    user_id = message.from_user.id
    today_str = str(date.today())
    data = load_users()
    users = data["users"]
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)
    
    if user_id in [OLGA_ID, YOUR_ADMIN_ID]:
        await admin_main_menu(user_id)
    else:
        if str(user_id) in users:
            has_students = any(u.get("mentor") == str(user_id) for u in users.values())
            if has_students:
                await mentor_main_menu(user_id)
            else:
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile"))
                await message.answer("📋 <b>Главное меню</b>", reply_markup=kb)
        else:
            await message.answer("Вы не зарегистрированы. Используйте /start для регистрации.")

@dp.message_handler(commands=["profile"], state="*")
async def profile_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    user_id = str(message.from_user.id)
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    if user_id in users:
        users[user_id]["active_today"] = today_str
        save_users(data)
    else:
        await message.answer("Вы не зарегистрированы. Используйте /start для регистрации.")
        return
    
    u = users[user_id]
    
    mentor_name = "не выбран"
    if u.get("mentor") and u["mentor"] in users:
        mentor = users[u["mentor"]]
        mentor_name = f"{mentor['name']} {mentor.get('surname','')}"
    
    student_count = 0
    if any(u.get("mentor") == user_id for u in users.values()):
        student_count = sum(1 for u in users.values() if u.get("mentor") == user_id)
    
    text = f"👤 <b>Ваш профиль</b>\n\n"
    text += f"• Имя: <b>{u['name']} {u.get('surname','')}</b>\n"
    text += f"• Уровень: <b>{u.get('level','—')}</b>\n"
    text += f"• Наставник: <b>{mentor_name}</b>\n"
    text += f"• Дата регистрации: <b>{u.get('registration_date','—')}</b>\n"
    
    if student_count > 0:
        text += f"• Ваших учеников: <b>{student_count}</b>\n"
    
    if message.from_user.id in [OLGA_ID, YOUR_ADMIN_ID]:
        text += f"• Ваш ID: <b>{user_id}</b>"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅ Назад в меню", callback_data="back_main"))
    await message.answer(text, reply_markup=kb)

@dp.message_handler(commands=["students"], state="*")
async def students_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    user_id = message.from_user.id
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)
    else:
        await message.answer("Вы не зарегистрированы. Используйте /start для регистрации.")
        return
    
    has_students = any(u.get("mentor") == str(user_id) for u in users.values())
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for lvl in LEVELS_ORDER:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"show_students:{lvl}"))
    
    if user_id != YOUR_ADMIN_ID:
        if has_students:
            kb.add(InlineKeyboardButton("🌳 Вся моя ветка", callback_data="my_full_branch"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
    
    await message.answer("Выберите уровень или просмотр ветки:", reply_markup=kb)

@dp.message_handler(commands=["admin"], state="*")
async def admin_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    if message.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await message.answer("⚠️ Эта команда доступна только администраторам")
        return
    
    await admin_main_menu(message.from_user.id)

@dp.message_handler(commands=["stats"], state="*")
async def stats_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    if message.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await message.answer("⚠️ Эта команда доступна только администраторам")
        return
    
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    total = len(users)
    new_today = sum(1 for u in users.values() if u.get("registration_date") == today_str)
    active_today = sum(1 for u in users.values() if u.get("active_today") == today_str)
    with_mentor = sum(1 for u in users.values() if u.get("mentor"))
    without_mentor = total - with_mentor
    
    text = f"📊 <b>Статистика бота</b>\n\n"
    text += f"• Всего пользователей: {total}\n"
    text += f"• Новых сегодня: {new_today}\n"
    text += f"• Активных сегодня: {active_today}\n"
    text += f"• С наставником: {with_mentor}\n"
    text += f"• Без наставника: {without_mentor}\n\n"
    
    text += "<b>По уровням:</b>\n"
    for level in LEVELS_ORDER:
        level_users = [u for u in users.values() if u.get("level") == level]
        level_active = sum(1 for u in level_users if u.get("active_today") == today_str)
        text += f"• {level}: {len(level_users)} чел. (активных: {level_active})\n"
    
    await message.answer(text)

@dp.message_handler(commands=["broadcast"], state="*")
async def broadcast_command(message: types.Message, state=None):
    if state:
        await state.finish()
    
    if message.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await message.answer("⚠️ Эта команда доступна только администраторам")
        return
    
    if message.from_user.id == YOUR_ADMIN_ID:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("📋 По уровням", callback_data="broadcast_by_level"))
        kb.add(InlineKeyboardButton("✅ Только активные", callback_data="broadcast_active"))
        kb.add(InlineKeyboardButton("❌ Только неактивные", callback_data="broadcast_inactive"))
        kb.add(InlineKeyboardButton("👥 Всем пользователям", callback_data="broadcast_all"))
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
        
        await message.answer(
            "📢 <b>Расширенная рассылка</b>\n\n"
            "Выберите тип рассылки:",
            reply_markup=kb
        )
    else:
        await message.answer("Выберите уровни для рассылки:")
        await Form.admin_choose_levels.set()
        await show_level_selection(message, [])

# --- START ---
@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state=None):
    user_id = message.from_user.id
    data = load_users()
    users = data["users"]

    today_str = str(date.today())
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)

    if state:
        await state.finish()
    else:
        state = dp.current_state(user=user_id, chat=user_id)
        await state.finish()

    if user_id in [OLGA_ID, YOUR_ADMIN_ID]:
        await message.answer(f"Привет, администратор! 👑")
        await admin_main_menu(user_id)
        return

    if str(user_id) in users:
        await message.answer(
            f"🔄 <b>Бот перезапущен</b>\n\n"
            f"Вы уже зарегистрированы как <b>{users[str(user_id)]['name']} {users[str(user_id)].get('surname','')}</b>"
        )
        if any(u.get("mentor") == str(user_id) for u in users.values()):
            await mentor_main_menu(user_id)
        else:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile"))
            await message.answer("📋 <b>Главное меню</b>", reply_markup=kb)
        return

    await message.answer("Введите ваше имя:")
    await Form.get_name.set()

# --- Регистрация имени ---
@dp.message_handler(state=Form.get_name)
async def get_name(message, state):
    await state.update_data(name=message.text)
    await message.answer("Введите вашу фамилию:")
    await Form.get_surname.set()

# --- Регистрация фамилии ---
@dp.message_handler(state=Form.get_surname)
async def get_surname(message, state):
    await state.update_data(surname=message.text)
    kb = InlineKeyboardMarkup()
    for lvl in LEVELS_ORDER:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"level:{lvl}"))
    await message.answer("Выберите ваш уровень:", reply_markup=kb)
    await Form.choose_level.set()

# --- Выбор уровня ---
@dp.callback_query_handler(lambda c: c.data.startswith("level:"), state=Form.choose_level)
async def choose_level(callback, state):
    await state.update_data(level=callback.data.split(":")[1])
    await send_mentor_selection(callback.message, state)
    await Form.choose_mentor.set()

async def send_mentor_selection(message, state):
    data_user = await state.get_data()
    level_idx = LEVELS_ORDER.index(data_user["level"])
    higher_levels = LEVELS_ORDER[level_idx:]

    kb = InlineKeyboardMarkup()
    for lvl in higher_levels:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"choose_mentor_level:{lvl}"))
    
    kb.add(InlineKeyboardButton("⬅ Назад к выбору уровня", callback_data="back_to_level"))
    
    await message.answer("Выберите уровень наставника:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "back_to_level", state=Form.choose_mentor)
async def back_to_level(callback, state):
    await callback.message.delete()
    
    kb = InlineKeyboardMarkup()
    for lvl in LEVELS_ORDER:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"level:{lvl}"))
    
    await callback.message.answer("Выберите ваш уровень:", reply_markup=kb)
    await Form.choose_level.set()

# --- Выбор наставника ---
@dp.callback_query_handler(lambda c: c.data.startswith("choose_mentor_level:"), state=Form.choose_mentor)
async def choose_mentor_level(callback, state):
    level = callback.data.split(":")[1]
    await state.update_data(mentor_level=level)

    data_users = load_users()
    users = data_users["users"]
    
    mentors = [
        (uid, u) for uid, u in users.items() 
        if u.get("level") == level 
        and int(uid) != YOUR_ADMIN_ID
    ]

    if not mentors:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("⬅ Назад к выбору уровня наставника", 
                                   callback_data="back_to_mentor_level"))
        await callback.message.answer("Нет наставников на этом уровне.", reply_markup=kb)
        return

    kb = InlineKeyboardMarkup()
    for uid, u in sorted(mentors, key=lambda x: x[1]["name"]):
        full_name = f"{u['name']} {u.get('surname','')}".strip()
        kb.add(InlineKeyboardButton(f"{full_name} — {u['level']}", callback_data=f"mentor:{uid}"))
    
    kb.add(InlineKeyboardButton("⬅ Назад к выбору уровня наставника", 
                               callback_data="back_to_mentor_level"))
    
    await callback.message.answer("Выберите наставника:", reply_markup=kb)
    await callback.message.delete()

@dp.callback_query_handler(lambda c: c.data == "back_to_mentor_level", state=Form.choose_mentor)
async def back_to_mentor_level(callback, state):
    await callback.message.delete()
    await send_mentor_selection(callback.message, state)

@dp.callback_query_handler(lambda c: c.data.startswith("mentor:"), state=Form.choose_mentor)
async def choose_mentor(callback, state):
    """ИСПРАВЛЕННАЯ версия: не перезаписывает существующих пользователей"""
    mentor_id = callback.data.split(":")[1]
    await state.update_data(mentor=mentor_id)

    user_id = str(callback.from_user.id)
    data_user = await state.get_data()

    data = load_users()
    users = data["users"]

    # ПРОВЕРЯЕМ, существует ли уже пользователь
    if user_id in users:
        # Обновляем только нужные поля, сохраняем существующие данные
        existing_user = users[user_id]
        users[user_id] = {
            "name": data_user["name"],
            "surname": data_user.get("surname", existing_user.get("surname", "")),
            "level": data_user["level"],
            "pending_mentor": mentor_id,
            "chat_id": user_id,
            # Сохраняем существующие поля
            "registration_date": existing_user.get("registration_date", str(date.today())),
            "active_today": existing_user.get("active_today"),
            "mentor": existing_user.get("mentor")  # Сохраняем старого наставника если есть
        }
        log_info(f"🔄 Обновлен существующий пользователь: {data_user['name']} (ID: {user_id})")
    else:
        # Создаем нового пользователя
        users[user_id] = {
            "name": data_user["name"],
            "surname": data_user.get("surname", ""),
            "level": data_user["level"],
            "pending_mentor": mentor_id,
            "chat_id": user_id,
            "registration_date": str(date.today())
        }
        log_info(f"🆕 Создан новый пользователь: {data_user['name']} (ID: {user_id})")
    
    # СОХРАНЯЕМ ВСЕХ пользователей
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return

    mentor_name = users[mentor_id]["name"]

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Принять", callback_data=f"mentor_accept:{user_id}"))
    kb.add(InlineKeyboardButton("Отклонить", callback_data=f"mentor_decline:{user_id}"))

    await bot.send_message(
        mentor_id,
        f"Пользователь <b>{data_user['name']} {data_user.get('surname','')}</b> выбрал вас наставником.",
        reply_markup=kb
    )

    await callback.message.answer(f"Вы выбрали наставника <b>{mentor_name}</b>. Ждём подтверждения.")

# --- Подтверждение наставником ---
@dp.callback_query_handler(lambda c: c.data.startswith("mentor_accept:"))
async def mentor_accept(callback):
    chosen_user_id = callback.data.split(":")[1]

    data = load_users()
    users = data["users"]

    mentor_id = users[chosen_user_id].get("pending_mentor")
    users[chosen_user_id]["mentor"] = mentor_id
    users[chosen_user_id].pop("pending_mentor", None)
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return

    await callback.message.edit_text(
        f"Вы приняли ученика <b>{users[chosen_user_id]['name']} {users[chosen_user_id].get('surname','')}</b>"
    )
    await mentor_main_menu(mentor_id)
    await bot.send_message(chosen_user_id, "Наставник подтвердил ваш выбор ✅")

@dp.callback_query_handler(lambda c: c.data.startswith("mentor_decline:"))
async def mentor_decline(callback):
    chosen_user_id = callback.data.split(":")[1]
    data = load_users()
    users = data["users"]

    users[chosen_user_id].pop("pending_mentor", None)
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return

    await callback.message.edit_text("Отказано.")
    await bot.send_message(chosen_user_id, "Наставник отклонил ваш выбор.")

# --- Кнопка "Мой профиль" ---
@dp.callback_query_handler(lambda c: c.data == "my_profile")
async def show_my_profile(callback):
    user_id = str(callback.from_user.id)
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    if user_id in users:
        users[user_id]["active_today"] = today_str
        save_users(data)
    
    if user_id not in users:
        await callback.answer("Вы не зарегистрированы", show_alert=True)
        return
    
    u = users[user_id]
    
    mentor_name = "не выбран"
    if u.get("mentor") and u["mentor"] in users:
        mentor = users[u["mentor"]]
        mentor_name = f"{mentor['name']} {mentor.get('surname','')}"
    
    student_count = 0
    if any(u.get("mentor") == user_id for u in users.values()):
        student_count = sum(1 for u in users.values() if u.get("mentor") == user_id)
    
    text = f"👤 <b>Ваш профиль</b>\n\n"
    text += f"• Имя: <b>{u['name']} {u.get('surname','')}</b>\n"
    text += f"• Уровень: <b>{u.get('level','—')}</b>\n"
    text += f"• Наставник: <b>{mentor_name}</b>\n"
    text += f"• Дата регистрации: <b>{u.get('registration_date','—')}</b>\n"
    
    if student_count > 0:
        text += f"• Ваших учеников: <b>{student_count}</b>\n"
    
    if callback.from_user.id in [OLGA_ID, YOUR_ADMIN_ID]:
        text += f"• Ваш ID: <b>{user_id}</b>"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅ Назад в меню", callback_data="back_main"))
    await callback.message.answer(text, reply_mup=kb)

# --- Меню "Мои ученики" ---
@dp.callback_query_handler(lambda c: c.data == "show_my_students")
async def my_students(callback):
    user_id = callback.from_user.id
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for lvl in LEVELS_ORDER:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"show_students:{lvl}"))
    
    if user_id != YOUR_ADMIN_ID:
        has_students = any(u.get("mentor") == str(user_id) for u in users.values())
        if has_students:
            kb.add(InlineKeyboardButton("🌳 Вся моя ветка", callback_data="my_full_branch"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
    
    await callback.message.answer("Выберите уровень или просмотр ветки:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main(callback):
    user_id = callback.from_user.id
    if user_id in [OLGA_ID, YOUR_ADMIN_ID]:
        await admin_main_menu(user_id)
    else:
        data = load_users()
        users = data["users"]
        
        if str(user_id) in users:
            has_students = any(u.get("mentor") == str(user_id) for u in users.values())
            if has_students:
                await mentor_main_menu(user_id)
            else:
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile"))
                await callback.message.answer("📋 <b>Главное меню</b>", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("show_students:"))
async def show_students(callback):
    user_id = callback.from_user.id
    level = callback.data.split(":")[1]

    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)

    if user_id == YOUR_ADMIN_ID:
        students = [(uid, u) for uid, u in users.items() if u.get("level") == level]
        title = f"👑 Все ученики уровня {level} (админ-просмотр):"
    else:
        students = [(uid, u) for uid, u in users.items() 
                   if u.get("mentor") == str(user_id) and u.get("level") == level]
        title = f"👥 Ваши ученики уровня {level}:"

    if not students:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data="show_my_students"))
        await callback.message.answer(f"На уровне {level} пока нет учеников.", reply_markup=kb)
        return

    kb = InlineKeyboardMarkup()
    text = f"{title}\n\n"
    for i, (uid, u) in enumerate(students, 1):
        full_name = f"{u['name']} {u.get('surname','')}".strip()
        text += f"{i}. {full_name}"
        
        if user_id == YOUR_ADMIN_ID and u.get("mentor"):
            mentor = users.get(u["mentor"], {})
            mentor_name = f"{mentor.get('name', '?')} {mentor.get('surname', '')}".strip()
            if mentor_name.strip():
                text += f" → {mentor_name}"
        
        text += "\n"
        
        kb.add(InlineKeyboardButton(f"Профиль: {full_name}", 
                                   callback_data=f"student_profile:{uid}:{level}"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="show_my_students"))
    await callback.message.answer(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "my_full_branch")
async def my_full_branch(callback):
    user_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    if user_id in users:
        users[user_id]["active_today"] = today_str
        save_users(data)
    
    if not any(u.get("mentor") == user_id for u in users.values()):
        await callback.answer("У вас пока нет учеников", show_alert=True)
        return
    
    def collect_branch(root_id):
        branch = []
        direct_students = [uid for uid, u in users.items() if u.get("mentor") == root_id]
        
        for student_id in direct_students:
            student = users[student_id]
            branch.append({
                "id": student_id,
                "name": f"{student['name']} {student.get('surname','')}".strip(),
                "level": student.get("level", "?"),
                "mentor_id": root_id,
                "mentor_name": f"{users[root_id]['name']} {users[root_id].get('surname','')}".strip()
            })
            branch.extend(collect_branch(student_id))
        
        return branch
    
    full_branch = collect_branch(user_id)
    
    if not full_branch:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data="show_my_students"))
        await callback.message.answer("В вашей ветке пока нет учеников.", reply_markup=kb)
        return
    
    text = "🌳 <b>Вся ваша ветка учеников:</b>\n\n"
    
    for level in LEVELS_ORDER:
        level_users = [p for p in full_branch if p["level"] == level]
        if level_users:
            text += f"<b>{level}</b> ({len(level_users)} чел.):\n"
            for i, person in enumerate(level_users, 1):
                text += f"{i}. {person['name']}"
                if person["mentor_id"] != user_id:
                    text += f" ← ученик {person['mentor_name']}"
                text += "\n"
            text += "\n"
    
    text += f"<i>Всего в ветке: {len(full_branch)} учеников</i>"
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for level in LEVELS_ORDER:
        if any(p["level"] == level for p in full_branch):
            kb.add(InlineKeyboardButton(f"📋 {level}", callback_data=f"branch_level:{level}"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="show_my_students"))
    
    await callback.message.answer(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("branch_level:"))
async def branch_level_detail(callback):
    user_id = str(callback.from_user.id)
    selected_level = callback.data.split(":")[1]
    
    data = load_users()
    users = data["users"]
    
    def collect_branch(root_id):
        branch = []
        direct_students = [uid for uid, u in users.items() if u.get("mentor") == root_id]
        
        for student_id in direct_students:
            student = users[student_id]
            branch.append({
                "id": student_id,
                "name": f"{student['name']} {student.get('surname','')}".strip(),
                "level": student.get("level", "?"),
                "mentor_id": root_id,
                "mentor_name": f"{users[root_id]['name']} {users[root_id].get('surname','')}".strip()
            })
            branch.extend(collect_branch(student_id))
        
        return branch
    
    full_branch = collect_branch(user_id)
    level_users = [p for p in full_branch if p["level"] == selected_level]
    
    if not level_users:
        await callback.answer(f"На уровне {selected_level} нет учеников", show_alert=True)
        return
    
    text = f"<b>👥 Ученики уровня {selected_level} в вашей ветке:</b>\n\n"
    
    kb = InlineKeyboardMarkup()
    
    for i, person in enumerate(level_users, 1):
        def get_generation(student_id, current_id=user_id, generation=1):
            if student_id == current_id:
                return generation
            student_data = users.get(student_id, {})
            mentor_id = student_data.get("mentor")
            if mentor_id and mentor_id in users:
                return get_generation(mentor_id, current_id, generation + 1)
            return generation
        
        generation = get_generation(person["id"])
        generation_text = f"{generation}-е поколение" if generation > 1 else "Прямой ученик"
        
        text += f"{i}. <b>{person['name']}</b>\n"
        text += f"   📊 {generation_text}\n"
        text += f"   👤 Наставник: {person['mentor_name']}\n\n"
        
        kb.add(InlineKeyboardButton(
            f"👤 {person['name']}", 
            callback_data=f"student_profile:{person['id']}:BRANCH"
        ))
    
    kb.add(InlineKeyboardButton("⬅ Назад к ветке", callback_data="my_full_branch"))
    
    await callback.message.answer(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("student_profile:"))
async def student_profile(callback):
    parts = callback.data.split(":")
    user_id = parts[1]
    source = parts[2] if len(parts) > 2 else "NONE"

    data = load_users()
    users = data["users"]

    u = users.get(user_id)
    if not u:
        await callback.answer("Не найдено", show_alert=True)
        return

    mentor_name = "не выбран"
    if (mid := u.get("mentor")) and mid in users:
        mentor = users[mid]
        mentor_name = f"{mentor['name']} {mentor.get('surname','')}"

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👥 Его ученики", callback_data=f"child_students:{user_id}"))
    
    if source == "BRANCH":
        kb.add(InlineKeyboardButton("⬅ Назад к ветке", callback_data="my_full_branch"))
    elif source in LEVELS_ORDER:
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data=f"show_students:{source}"))
    else:
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data="show_my_students"))

    await callback.message.answer(
        f"👤 <b>Профиль</b>\n\n"
        f"Имя: <b>{u['name']} {u.get('surname','')}</b>\n"
        f"Уровень: <b>{u.get('level','—')}</b>\n"
        f"Наставник: <b>{mentor_name}</b>\n"
        f"ID: <code>{user_id}</code>",
        reply_markup=kb
    )

@dp.callback_query_handler(lambda c: c.data.startswith("child_students:"))
async def child_students(callback):
    user_id = callback.data.split(":")[1]

    data = load_users()
    users = data["users"]

    children = [(uid, u) for uid, u in users.items() if u.get("mentor") == user_id]

    kb = InlineKeyboardMarkup()
    if not children:
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data=f"student_profile:{user_id}:NONE"))
        await callback.message.answer("У этого ученика пока нет своих учеников.", reply_markup=kb)
        return

    text = "🌿 <b>Ученики этого ученика:</b>\n\n"
    for i, (uid, u) in enumerate(children, 1):
        full_name = f"{u['name']} {u.get('surname','')}".strip()
        text += f"{i}. {full_name} — {u.get('level','—')}\n"
        kb.add(InlineKeyboardButton(f"Профиль: {full_name}", callback_data=f"student_profile:{uid}:NONE"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data=f"student_profile:{user_id}:NONE"))
    await callback.message.answer(text, reply_markup=kb)

# --- ВСЕ ПОЛЬЗОВАТЕЛИ ---
@dp.callback_query_handler(lambda c: c.data == "all_users")
async def all_users(callback):
    if callback.from_user.id != YOUR_ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    
    data = load_users()
    users = data["users"]
    
    text = "👥 <b>Все пользователи по уровням:</b>\n\n"
    
    for level in LEVELS_ORDER:
        level_users = [u for u in users.values() if u.get("level") == level]
        text += f"<b>{level}</b> ({len(level_users)} чел.):\n"
        
        for u in sorted(level_users, key=lambda x: x['name']):
            full_name = f"{u['name']} {u.get('surname','')}".strip()
            mentor_info = ""
            
            if u.get("mentor") and u["mentor"] in users:
                mentor = users[u["mentor"]]
                mentor_info = f" → {mentor['name']}"
            
            user_id = [uid for uid, usr in users.items() if usr == u][0]
            text += f"  • {full_name} (ID: {user_id}){mentor_info}\n"
        
        text += "\n"
    
    text += f"<b>Всего пользователей:</b> {len(users)}"
    
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await callback.message.answer(part)
    else:
        await callback.message.answer(text)

# --- ПОЛНАЯ ИЕРАРХИЯ ---
@dp.callback_query_handler(lambda c: c.data == "full_hierarchy")
async def full_hierarchy(callback):
    if callback.from_user.id != YOUR_ADMIN_ID:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    
    data = load_users()
    users = data["users"]
    
    roots = [uid for uid, u in users.items() if not u.get("mentor")]
    
    text = "🌳 <b>Полная иерархия пользователей:</b>\n\n"
    
    def build_tree(user_id, depth=0):
        result = ""
        if user_id in users:
            u = users[user_id]
            full_name = f"{u['name']} {u.get('surname','')}".strip()
            indent = "  " * depth
            result = f"{indent}• {full_name} [{u.get('level','?')}] (ID: {user_id})\n"
            
            students = [uid for uid, usr in users.items() if usr.get("mentor") == user_id]
            for student_id in students:
                result += build_tree(student_id, depth + 1)
        
        return result
    
    for root_id in roots:
        text += build_tree(root_id)
    
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await callback.message.answer(part)
    else:
        await callback.message.answer(text)

# --- РАССЫЛКА ---
@dp.callback_query_handler(lambda c: c.data == "admin_broadcast")
async def admin_broadcast(callback):
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    
    if callback.from_user.id == YOUR_ADMIN_ID:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("📋 По уровням", callback_data="broadcast_by_level"))
        kb.add(InlineKeyboardButton("✅ Только активные", callback_data="broadcast_active"))
        kb.add(InlineKeyboardButton("❌ Только неактивные", callback_data="broadcast_inactive"))
        kb.add(InlineKeyboardButton("👥 Всем пользователям", callback_data="broadcast_all"))
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
        
        await callback.message.answer(
            "📢 <b>Расширенная рассылка</b>\n\n"
            "Выберите тип рассылки:",
            reply_markup=kb
        )
    else:
        await callback.message.answer("Выберите уровни для рассылки:")
        await Form.admin_choose_levels.set()
        await show_level_selection(callback.message, [])

@dp.callback_query_handler(lambda c: c.data == "broadcast_by_level")
async def broadcast_by_level(callback):
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        return
    
    await callback.message.answer("Выберите уровни для рассылки:")
    await Form.admin_choose_levels.set()
    await show_level_selection(callback.message, [])

async def show_level_selection(message, selected_levels):
    kb = InlineKeyboardMarkup()
    for lvl in LEVELS_ORDER:
        mark = "✅" if lvl in selected_levels else ""
        kb.add(InlineKeyboardButton(f"{lvl} {mark}", callback_data=f"lvl_select:{lvl}"))
    kb.add(InlineKeyboardButton("Готово", callback_data="lvl_done"))
    await message.answer("Выберите уровни (отметка ✅ — выбранные):", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("lvl_select:"), state=Form.admin_choose_levels)
async def lvl_select(callback, state):
    lvl = callback.data.split(":")[1]
    data = await state.get_data()
    selected = data.get("selected_levels", [])
    if lvl in selected:
        selected.remove(lvl)
    else:
        selected.append(lvl)
    await state.update_data(selected_levels=selected)
    await callback.message.delete()
    await show_level_selection(callback.message, selected)

@dp.callback_query_handler(lambda c: c.data == "lvl_done", state=Form.admin_choose_levels)
async def lvl_done(callback, state):
    data = await state.get_data()
    selected_levels = data.get("selected_levels", [])
    if not selected_levels:
        await callback.message.answer("Вы должны выбрать хотя бы один уровень.")
        return
    await state.update_data(selected_levels=selected_levels)
    await callback.message.answer("Отправьте сообщение для рассылки (текст, фото, видео, документ или голос).")
    await Form.admin_message.set()

@dp.callback_query_handler(lambda c: c.data == "broadcast_all")
async def broadcast_all(callback):
    if callback.from_user.id != YOUR_ADMIN_ID:
        return
    
    data = load_users()
    users = data["users"]
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Подтверждаю", callback_data="confirm_broadcast_all"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast")
    )
    
    await callback.message.answer(
        f"📢 <b>Рассылка ВСЕМ пользователям</b>\n\n"
        f"• Получателей: {len(users)}\n"
        f"• Это затронет всех пользователей бота.\n\n"
        f"<b>Вы уверены?</b>",
        reply_markup=kb
    )

@dp.callback_query_handler(lambda c: c.data == "confirm_broadcast_all")
async def confirm_broadcast_all(callback):
    await callback.message.edit_text("Отправьте сообщение для рассылки ВСЕМ пользователям:")
    await Form.admin_message.set()
    
    state = dp.current_state(user=callback.from_user.id, chat=callback.from_user.id)
    await state.update_data(broadcast_to_all=True)

@dp.callback_query_handler(lambda c: c.data == "cancel_broadcast")
async def cancel_broadcast(callback):
    await callback.message.edit_text("❌ Рассылка отменена.")
    await admin_main_menu(callback.from_user.id)

@dp.message_handler(state=Form.admin_message, content_types=types.ContentTypes.ANY)
async def admin_send_message(message, state):
    data = await state.get_data()
    selected_levels = data.get("selected_levels", [])
    broadcast_to_all = data.get("broadcast_to_all", False)
    
    users_data = load_users()["users"]
    recipients = []
    recipient_names = []
    
    today_str = str(date.today())
    
    for uid, u in users_data.items():
        if broadcast_to_all:
            should_send = True
        elif selected_levels:
            should_send = u.get("level") in selected_levels
        else:
            should_send = False
        
        if should_send:
            recipients.append(uid)
            full_name = f"{u['name']} {u.get('surname','')}".strip()
            recipient_names.append(full_name)
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Отправить", callback_data="confirm_send"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_send")
    )
    
    if broadcast_to_all:
        target = "ВСЕМ пользователям"
    elif selected_levels:
        target = f"уровням: {', '.join(selected_levels)}"
    else:
        target = "не выбранным пользователям"
    
    preview_text = f"📢 <b>Подтверждение рассылки</b>\n\n"
    preview_text += f"• Кому: {target}\n"
    preview_text += f"• Получателей: {len(recipients)}\n"
    preview_text += f"• Тип: {message.content_type}\n\n"
    
    if message.content_type == "text":
        preview_text += f"<b>Текст:</b>\n{message.text[:200]}..."
    elif message.caption:
        preview_text += f"<b>Подпись:</b>\n{message.caption[:200]}..."
    
    await state.update_data(
        message_to_send=message,
        recipients=recipients,
        recipient_names=recipient_names,
        selected_levels=selected_levels,
        broadcast_to_all=broadcast_to_all
    )
    
    await message.answer(preview_text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "confirm_send", state=Form.admin_message)
async def confirm_send(callback, state):
    data = await state.get_data()
    message = data.get("message_to_send")
    recipients = data.get("recipients", [])
    
    await callback.message.edit_text(f"🔄 Отправляю {len(recipients)} сообщений...")
    
    sent_count = 0
    failed_count = 0
    
    for i, uid in enumerate(recipients):
        try:
            if message.content_type == "text":
                await bot.send_message(uid, message.text)
            elif message.content_type == "photo":
                await bot.send_photo(uid, message.photo[-1].file_id, caption=message.caption)
            elif message.content_type == "video":
                await bot.send_video(uid, message.video.file_id, caption=message.caption)
            elif message.content_type == "document":
                await bot.send_document(uid, message.document.file_id, caption=message.caption)
            elif message.content_type == "voice":
                await bot.send_voice(uid, message.voice.file_id)
            
            sent_count += 1
            
            if i % 10 == 0:
                await asyncio.sleep(0.5)
                
        except Exception as e:
            failed_count += 1
            log_info(f"Ошибка отправки {uid}: {e}")
    
    await callback.message.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"• Отправлено: {sent_count}\n"
        f"• Не отправлено: {failed_count}"
    )
    
    await state.finish()
    await admin_main_menu(callback.from_user.id)

@dp.callback_query_handler(lambda c: c.data == "cancel_send", state=Form.admin_message)
async def cancel_send(callback, state):
    await callback.message.edit_text("❌ Рассылка отменена.")
    await state.finish()
    await admin_main_menu(callback.from_user.id)

# --- ЕЖЕДНЕВНЫЙ ОТЧЕТ ---
async def daily_report():
    await asyncio.sleep(5)
    while True:
        now = datetime.now()
        target_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
        if now > target_time:
            target_time = target_time.replace(day=now.day + 1)
        wait_seconds = (target_time - now).total_seconds()
        await asyncio.sleep(wait_seconds)

        today_str = str(date.today())
        data = load_users()
        users = data["users"]

        new_users = [f"{u.get('name','')} {u.get('surname','')}".strip() for u in users.values() if u.get("registration_date") == today_str]

        text = f"📊 <b>Ежедневный отчет по активности пользователей ({today_str})</b>\n\n"
        text += f"Всего пользователей: {len(users)}\n"
        text += f"Новых сегодня: {len(new_users)} — " + (", ".join(new_users) if new_users else "—") + "\n\n"

        for level in LEVELS_ORDER:
            level_users = [u for u in users.values() if u.get("level") == level]
            active = [f"{u.get('name','')} {u.get('surname','')}".strip() for u in level_users if u.get("active_today") == today_str]
            inactive = [f"{u.get('name','')} {u.get('surname','')}".strip() for u in level_users if u.get("active_today") != today_str]

            text += f"🔹 <b>{level}</b> ({len(level_users)} чел.)\n"
            text += f"✅ Были сегодня ({len(active)}): " + (", ".join(active) if active else "—") + "\n"
            text += f"❌ Не были сегодня ({len(inactive)}): " + (", ".join(inactive) if inactive else "—") + "\n\n"

        try:
            await bot.send_message(REPORT_GROUP_ID, text)
        except Exception as e:
            log_info(f"Ошибка отправки отчета: {e}")

# --- RUN ---
if __name__ == "__main__":
    print("=== Бот запускается ===")
    print("="*50)
    print(f"👑 ID Ольги: {OLGA_ID}")
    print(f"👑 ID Суперадмина: {YOUR_ADMIN_ID}")
    print(f"📊 Уровни: {LEVELS_ORDER}")
    print("="*50)
    
    # Загружаем данные
    data = load_users()
    user_count = len(data.get('users', {}))
    print(f"✅ Загружено пользователей: {user_count}")
    
    # Проверяем backup файлы
    backup_files = [f for f in os.listdir('.') if f.startswith('users_backup_')]
    corrupted_files = [f for f in os.listdir('.') if f.startswith('users_corrupted_')]
    
    if backup_files:
        print(f"📂 Найдено backup файлов: {len(backup_files)}")
    if corrupted_files:
        print(f"⚠️ Найдено поврежденных файлов: {len(corrupted_files)}")
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(set_bot_commands())
    print("✅ Меню команд настроено")
    
    loop.create_task(daily_report())
    print("✅ Задача ежедневного отчета запущена")
    
    print("="*50)
    print("🚀 Бот запущен и готов к работе!")
    print("🛡️  Данные защищены от потери")
    print("🔧 Новые команды для админа: /check_data, /fix_data")
    print("="*50)
    
    executor.start_polling(dp, skip_updates=True)
