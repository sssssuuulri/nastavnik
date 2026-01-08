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
from datetime import datetime, date, timedelta
import shutil
import hashlib
from typing import Dict, List, Optional

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

def log_debug(message: str):
    """Логи для отладки"""
    executor_log.submit(logger.debug, message)

def log_warning(message: str):
    """Логи для предупреждений"""
    executor_log.submit(logger.warning, message)

# --- TOKEN ---
load_dotenv()
API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise ValueError("Не найден BOT_TOKEN в .env")

bot = Bot(token=API_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

USERS_FILE = "users.json"
ASSIGNMENTS_FILE = "assignments.json"  # НОВЫЙ ФАЙЛ ДЛЯ ЗАДАНИЙ
BROADCAST_HISTORY_FILE = "broadcast_history.json"  # НОВЫЙ ФАЙЛ ДЛЯ ИСТОРИИ РАССЫЛОК
LEVELS_ORDER = ["НП", "СВ", "ВТ", "АВТ", "ГТ"]
OLGA_ID = 64434196
YOUR_ADMIN_ID = 911511438
REPORT_GROUP_ID = "-1003632130674"

# НОВЫЕ КОНСТАНТЫ ДЛЯ ТИПОВ ОШИБОК РАССЫЛКИ
BROADCAST_ERROR_TYPES = {
    "user_blocked": "Пользователь заблокировал бота",
    "chat_not_found": "Чат не найден/удален",
    "bot_blocked": "Бот заблокирован пользователем",
    "user_deactivated": "Пользователь деактивирован",
    "peer_id_invalid": "Неверный ID пользователя",
    "message_too_long": "Сообщение слишком длинное",
    "network_error": "Ошибка сети",
    "unknown": "Неизвестная ошибка"
}

# --- Функция для разбивки длинных сообщений на части ---
async def safe_send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    """Безопасная отправка сообщений с разбивкой на части"""
    if len(text) <= 4096:
        # Если сообщение короткое, отправляем как есть
        await bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    else:
        # Разбиваем на части по 4000 символов (с запасом)
        parts = []
        current_part = ""
        
        # Разбиваем по строкам, чтобы не обрезать слова
        lines = text.split('\n')
        
        for line in lines:
            # Если добавление строки не превысит лимит
            if len(current_part) + len(line) + 1 <= 4000:
                current_part += line + '\n'
            else:
                # Сохраняем текущую часть и начинаем новую
                if current_part:
                    parts.append(current_part)
                current_part = line + '\n'
        
        # Добавляем последнюю часть
        if current_part:
            parts.append(current_part)
        
        # Отправляем все части
        for i, part in enumerate(parts):
            if i == 0 and reply_markup is not None:
                # Клавиатуру добавляем только к первой части
                await bot.send_message(chat_id, part, reply_markup=reply_markup, parse_mode=parse_mode)
            else:
                await bot.send_message(chat_id, part, parse_mode=parse_mode)
            
            # Небольшая задержка между отправками
            if i < len(parts) - 1:
                await asyncio.sleep(0.1)
        
        # Уведомляем, если сообщение было разбито
        if len(parts) > 1:
            await bot.send_message(chat_id, f"📄 *Сообщение разбито на {len(parts)} части*", parse_mode="Markdown")

# --- НОВЫЕ ФУНКЦИИ ДЛЯ УЛУЧШЕННОЙ РАССЫЛКИ ---

def classify_error(error_message: str) -> str:
    """Определяет тип ошибки рассылки по тексту ошибки"""
    error_msg = str(error_message).lower()
    
    if "blocked" in error_msg or "bot was blocked" in error_msg:
        return "user_blocked"
    elif "chat not found" in error_msg or "chat not found" in error_msg:
        return "chat_not_found"
    elif "bot blocked" in error_msg:
        return "bot_blocked"
    elif "user is deactivated" in error_msg or "deactivated" in error_msg:
        return "user_deactivated"
    elif "peer id invalid" in error_msg:
        return "peer_id_invalid"
    elif "message is too long" in error_msg:
        return "message_too_long"
    elif "network" in error_msg or "connection" in error_msg:
        return "network_error"
    else:
        return "unknown"

def load_broadcast_history() -> dict:
    """Загрузка истории рассылок"""
    if not os.path.exists(BROADCAST_HISTORY_FILE):
        return {
            "broadcasts": {},
            "failed_deliveries": {},
            "stats": {
                "total_broadcasts": 0,
                "total_sent": 0,
                "total_failed": 0
            }
        }
    
    try:
        with open(BROADCAST_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"❌ Ошибка загрузки истории рассылок: {e}")
        return {
            "broadcasts": {},
            "failed_deliveries": {},
            "stats": {
                "total_broadcasts": 0,
                "total_sent": 0,
                "total_failed": 0
            }
        }

def save_broadcast_history(data: dict) -> bool:
    """Сохранение истории рассылок"""
    try:
        with open(BROADCAST_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log_error(f"❌ Ошибка сохранения истории рассылок: {e}")
        return False

def add_broadcast_to_history(
    broadcast_id: str,
    admin_id: str,
    target: str,
    recipients_count: int,
    sent_count: int,
    failed_count: int,
    message_type: str,
    timestamp: str
) -> None:
    """Добавление рассылки в историю"""
    history = load_broadcast_history()
    
    history["broadcasts"][broadcast_id] = {
        "admin_id": admin_id,
        "target": target,
        "recipients_count": recipients_count,
        "sent_count": sent_count,
        "failed_count": failed_count,
        "message_type": message_type,
        "timestamp": timestamp,
        "failed_users": []
    }
    
    # Обновляем статистику
    history["stats"]["total_broadcasts"] += 1
    history["stats"]["total_sent"] += sent_count
    history["stats"]["total_failed"] += failed_count
    
    save_broadcast_history(history)

def add_failed_delivery(
    broadcast_id: str,
    user_id: str,
    user_name: str,
    error_type: str,
    error_message: str,
    timestamp: str
) -> None:
    """Добавление информации о неудачной доставке"""
    history = load_broadcast_history()
    
    if broadcast_id not in history["failed_deliveries"]:
        history["failed_deliveries"][broadcast_id] = []
    
    failed_delivery = {
        "user_id": user_id,
        "user_name": user_name,
        "error_type": error_type,
        "error_message": error_message,
        "timestamp": timestamp
    }
    
    history["failed_deliveries"][broadcast_id].append(failed_delivery)
    
    # Также добавляем в информацию о рассылке
    if broadcast_id in history["broadcasts"]:
        history["broadcasts"][broadcast_id]["failed_users"].append({
            "user_id": user_id,
            "user_name": user_name,
            "error_type": error_type
        })
    
    save_broadcast_history(history)

def get_failed_deliveries_by_broadcast(broadcast_id: str) -> List[dict]:
    """Получение списка неудачных доставок по ID рассылки"""
    history = load_broadcast_history()
    return history.get("failed_deliveries", {}).get(broadcast_id, [])

def get_broadcast_stats(broadcast_id: str) -> Optional[dict]:
    """Получение статистики по рассылке"""
    history = load_broadcast_history()
    return history.get("broadcasts", {}).get(broadcast_id)

def group_errors_by_type(failed_deliveries: List[dict]) -> Dict[str, List[dict]]:
    """Группировка ошибок по типам"""
    grouped = {}
    for delivery in failed_deliveries:
        error_type = delivery.get("error_type", "unknown")
        if error_type not in grouped:
            grouped[error_type] = []
        grouped[error_type].append(delivery)
    return grouped

def cleanup_old_data(days_to_keep: int = 7) -> int:
    """Автоматическая очистка старых данных"""
    try:
        # Очистка истории рассылок старше days_to_keep дней
        history = load_broadcast_history()
        current_time = datetime.now()
        cutoff_date = current_time - timedelta(days=days_to_keep)
        
        broadcasts_to_remove = []
        for broadcast_id, broadcast_data in history.get("broadcasts", {}).items():
            try:
                broadcast_time = datetime.fromisoformat(broadcast_data.get("timestamp", "").replace('Z', '+00:00'))
                if broadcast_time < cutoff_date:
                    broadcasts_to_remove.append(broadcast_id)
            except:
                pass
        
        # Удаляем старые рассылки
        for broadcast_id in broadcasts_to_remove:
            history["broadcasts"].pop(broadcast_id, None)
            history["failed_deliveries"].pop(broadcast_id, None)
        
        # Очистка старых backup файлов
        backup_files = [f for f in os.listdir('.') if f.startswith('users_backup_')]
        for backup_file in backup_files:
            try:
                # Пытаемся извлечь дату из имени файла
                date_str = backup_file.replace('users_backup_', '').replace('.json', '')
                backup_date = datetime.strptime(date_str[:15], '%Y%m%d_%H%M%S')
                if backup_date < cutoff_date:
                    os.remove(backup_file)
                    log_info(f"🗑️ Удален старый backup: {backup_file}")
            except:
                pass
        
        # Очистка поврежденных файлов старше 3 дней
        corrupted_files = [f for f in os.listdir('.') if f.startswith('users_corrupted_')]
        for corrupted_file in corrupted_files:
            try:
                date_str = corrupted_file.replace('users_corrupted_', '').replace('.json', '')
                corrupted_date = datetime.strptime(date_str[:15], '%Y%m%d_%H%M%S')
                if corrupted_date < cutoff_date - timedelta(days=3):
                    os.remove(corrupted_file)
                    log_info(f"🗑️ Удален старый поврежденный файл: {corrupted_file}")
            except:
                pass
        
        save_broadcast_history(history)
        log_info(f"🧹 Очищены данные старше {days_to_keep} дней")
        return len(broadcasts_to_remove)
        
    except Exception as e:
        log_error(f"❌ Ошибка при очистке данных: {e}")
        return 0

# НОВЫЕ АСИНХРОННЫЕ ФУНКЦИИ ДЛЯ УВЕДОМЛЕНИЙ

async def send_admin_notification(admin_id: int, title: str, message: str, 
                                 broadcast_id: str = None, is_error: bool = False):
    """Отправка улучшенного уведомления администратору"""
    try:
        emoji = "⚠️" if is_error else "📢"
        text = f"{emoji} <b>{title}</b>\n\n{message}"
        
        if broadcast_id:
            text += f"\n\n🔍 ID рассылки: <code>{broadcast_id}</code>"
        
        kb = None
        if broadcast_id:
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("📊 Статус рассылки", callback_data=f"broadcast_status:{broadcast_id}"),
                InlineKeyboardButton("📋 Список ошибок", callback_data=f"failed_list:{broadcast_id}:1")
            )
        
        await bot.send_message(admin_id, text, reply_markup=kb, parse_mode="HTML")
        
    except Exception as e:
        log_error(f"Ошибка отправки уведомления администратору: {e}")

async def send_broadcast_progress_update(admin_id: int, broadcast_id: str, 
                                        current: int, total: int, sent: int, failed: int):
    """Отправка обновления о ходе рассылки"""
    if current % 10 == 0 or current == total:
        progress_percent = (current / total) * 100
        progress_bar = "█" * int(progress_percent / 10) + "░" * (10 - int(progress_percent / 10))
        
        text = (
            f"📊 <b>Ход рассылки</b>\n\n"
            f"🔹 Прогресс: {current}/{total}\n"
            f"🔹 {progress_bar} {progress_percent:.1f}%\n\n"
            f"✅ Отправлено: {sent}\n"
            f"❌ Ошибок: {failed}"
        )
        
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except:
            pass

async def send_broadcast_summary(admin_id: int, broadcast_id: str, 
                                total: int, sent: int, failed: int, 
                                target: str, failed_deliveries: List[dict]):
    """Отправка сводки по завершении рассылки"""
    success_rate = (sent / total * 100) if total > 0 else 0
    
    # Группируем ошибки по типам
    error_groups = group_errors_by_type(failed_deliveries)
    
    text = f"📊 <b>СВОДКА ПО РАССЫЛКЕ</b>\n\n"
    text += f"🔹 Целевая аудитория: {target}\n"
    text += f"🔹 Всего получателей: {total}\n"
    text += f"🔹 Успешно отправлено: {sent} ({success_rate:.1f}%)\n"
    text += f"🔹 Не отправлено: {failed}\n\n"
    
    if error_groups:
        text += f"<b>Группировка ошибок:</b>\n"
        for error_type, errors in error_groups.items():
            error_name = BROADCAST_ERROR_TYPES.get(error_type, "Неизвестная ошибка")
            text += f"• {error_name}: {len(errors)} ошибок\n"
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📋 Детальный отчет", callback_data=f"broadcast_report:{broadcast_id}"),
        InlineKeyboardButton("❌ Список ошибок", callback_data=f"failed_list:{broadcast_id}:1")
    )
    if failed > 0:
        kb.add(InlineKeyboardButton("🔄 Повторить ошибки", callback_data=f"retry_failed:{broadcast_id}"))
    
    await send_admin_notification(admin_id, "Рассылка завершена", text, broadcast_id)

async def enhanced_broadcast(
    admin_id: int,
    message: types.Message,
    recipients: List[str],
    target_description: str,
    broadcast_type: str = "regular"
) -> str:
    """Улучшенная функция рассылки с отслеживанием ошибок"""
    broadcast_id = f"broadcast_{admin_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Уведомляем о начале рассылки
    await send_admin_notification(
        admin_id,
        "Начинаю рассылку",
        f"🔹 Тип: {'Задание' if broadcast_type == 'assignment' else 'Обычная рассылка'}\n"
        f"🔹 Получателей: {len(recipients)}\n"
        f"🔹 Целевая аудитория: {target_description}",
        broadcast_id
    )
    
    sent_count = 0
    failed_count = 0
    failed_deliveries = []
    
    users_data = load_users()["users"]
    
    # Отправляем сообщения
    for i, user_id in enumerate(recipients, 1):
        try:
            user_name = "Неизвестный"
            if user_id in users_data:
                user = users_data[user_id]
                user_name = f"{user['name']} {user.get('surname', '')}".strip()
            
            # Отправляем сообщение в зависимости от типа
            if message.content_type == "text":
                await bot.send_message(user_id, message.text)
            elif message.content_type == "photo":
                await bot.send_photo(user_id, message.photo[-1].file_id, caption=message.caption)
            elif message.content_type == "video":
                await bot.send_video(user_id, message.video.file_id, caption=message.caption)
            elif message.content_type == "document":
                await bot.send_document(user_id, message.document.file_id, caption=message.caption)
            elif message.content_type == "voice":
                await bot.send_voice(user_id, message.voice.file_id)
            
            sent_count += 1
            
            # Периодически отправляем обновление о прогрессе
            if i % 10 == 0:
                await send_broadcast_progress_update(
                    admin_id, broadcast_id, i, len(recipients), sent_count, failed_count
                )
            
            await asyncio.sleep(0.1)
            
        except Exception as e:
            failed_count += 1
            error_type = classify_error(str(e))
            error_message = str(e)
            
            # Добавляем информацию о неудачной доставке
            failed_delivery = {
                "user_id": user_id,
                "user_name": user_name,
                "error_type": error_type,
                "error_message": error_message,
                "timestamp": str(datetime.now())
            }
            failed_deliveries.append(failed_delivery)
            
            # Сохраняем в историю
            add_failed_delivery(
                broadcast_id, user_id, user_name, error_type, error_message, str(datetime.now())
            )
            
            log_error(f"Ошибка отправки {user_id} ({user_name}): {error_type} - {error_message}")
    
    # Сохраняем информацию о рассылке
    add_broadcast_to_history(
        broadcast_id=broadcast_id,
        admin_id=str(admin_id),
        target=target_description,
        recipients_count=len(recipients),
        sent_count=sent_count,
        failed_count=failed_count,
        message_type=message.content_type,
        timestamp=str(datetime.now())
    )
    
    # Отправляем итоговую сводку
    await send_broadcast_summary(
        admin_id, broadcast_id, len(recipients), sent_count, failed_count, 
        target_description, failed_deliveries
    )
    
    return broadcast_id

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

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ЗАДАНИЯМИ ---
def load_assignments():
    """Загрузка заданий и решений"""
    if not os.path.exists(ASSIGNMENTS_FILE):
        return {"assignments": {}, "solutions": {}, "conversations": {}, "assignment_recipients": {}, "active_dialogues": {}}
    
    try:
        with open(ASSIGNMENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"❌ Ошибка загрузки assignments.json: {e}")
        return {"assignments": {}, "solutions": {}, "conversations": {}, "assignment_recipients": {}, "active_dialogues": {}}

def save_assignments(data):
    """Сохранение заданий"""
    try:
        with open(ASSIGNMENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log_error(f"❌ Ошибка сохранения assignments.json: {e}")
        return False

# --- НОВЫЕ ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ ДИАЛОГАМИ ---
def save_dialogue_state(mentor_id: str, student_id: str, assignment_id: str = None):
    """Сохранение состояния активного диалога"""
    assignments_data = load_assignments()
    
    # Сохраняем диалог для наставника
    assignments_data.setdefault("active_dialogues", {})[mentor_id] = {
        "with_student": student_id,
        "assignment_id": assignment_id,
        "started_at": str(datetime.now())
    }
    
    # Сохраняем диалог для ученика
    assignments_data.setdefault("active_dialogues", {})[student_id] = {
        "with_mentor": mentor_id,
        "assignment_id": assignment_id,
        "started_at": str(datetime.now())
    }
    
    return save_assignments(assignments_data)

def end_dialogue(user_id: str):
    """Завершение диалога для пользователя"""
    assignments_data = load_assignments()
    
    if user_id in assignments_data.get("active_dialogues", {}):
        # Находим собеседника
        dialogue_info = assignments_data["active_dialogues"][user_id]
        partner_id = dialogue_info.get("with_student") or dialogue_info.get("with_mentor")
        
        # Удаляем диалог для обоих участников
        assignments_data["active_dialogues"].pop(user_id, None)
        if partner_id:
            assignments_data["active_dialogues"].pop(partner_id, None)
        
        save_assignments(assignments_data)
        return partner_id
    
    return None

def get_active_dialogue(user_id: str):
    """Получение информации об активном диалоге"""
    assignments_data = load_assignments()
    return assignments_data.get("active_dialogues", {}).get(user_id)

def save_dialogue_message(sender_id: str, receiver_id: str, message_data: dict):
    """Сохранение сообщения в истории диалога"""
    assignments_data = load_assignments()
    
    dialogue_id = f"{min(sender_id, receiver_id)}_{max(sender_id, receiver_id)}"
    
    if dialogue_id not in assignments_data.get("conversations", {}):
        assignments_data["conversations"][dialogue_id] = {
            "participants": [sender_id, receiver_id],
            "messages": []
        }
    
    message_record = {
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "timestamp": str(datetime.now()),
        "content_type": message_data.get("content_type"),
        "text": message_data.get("text"),
        "photo_id": message_data.get("photo_id"),
        "document_id": message_data.get("document_id"),
        "voice_id": message_data.get("voice_id"),
        "caption": message_data.get("caption")
    }
    
    assignments_data["conversations"][dialogue_id]["messages"].append(message_record)
    
    # Ограничиваем историю последними 100 сообщениями
    if len(assignments_data["conversations"][dialogue_id]["messages"]) > 100:
        assignments_data["conversations"][dialogue_id]["messages"] = \
            assignments_data["conversations"][dialogue_id]["messages"][-100:]
    
    return save_assignments(assignments_data)

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
    change_level = State()            # Для смены уровня
    change_mentor = State()           # Для смены наставника

# НОВЫЕ СОСТОЯНИЯ ДЛЯ ЗАДАНИЙ
class AssignmentStates(StatesGroup):
    waiting_for_solution = State()  # Ученик отправляет решение
    mentor_reply = State()          # Наставник отвечает ученику

# НОВЫЕ СОСТОЯНИЯ ДЛЯ ДИАЛОГОВ
class DialogueStates(StatesGroup):
    in_dialogue_with_mentor = State()    # Ученик в диалоге с наставником
    in_dialogue_with_student = State()   # Наставник в диалоге с учеником

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
    
    # Используем безопасную отправку
    await safe_send_message(callback.from_user.id, text)

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
    
    # Используем безопасную отправку
    await safe_send_message(callback.from_user.id, text)

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
    data = load_users()
    users = data["users"]
    
    # Проверяем, есть ли ученики или является ли админом
    has_students = any(u.get("mentor") == str(user_id) for u in users.values())
    is_admin = user_id in [OLGA_ID, YOUR_ADMIN_ID]
    
    # Если это админ Ольга, показываем ей обе роли
    if is_admin and user_id == OLGA_ID:
        await admin_main_menu(user_id)
        return
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile"))
    
    if has_students or is_admin:
        kb.add(InlineKeyboardButton("👥 Мои ученики", callback_data="show_my_students"))
        # ДОБАВЛЕНО: Кнопка для просмотра решений учеников
        kb.add(InlineKeyboardButton("📥 Ответы учеников", callback_data="view_student_solutions"))
    
    await bot.send_message(user_id, "📋 <b>Главное меню</b>", reply_markup=kb)

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
• Изменять свой уровень (требуется подтверждение наставника)
• Изменять наставника (требуется подтверждение нового наставника)
• Просматривать решения заданий от своих учеников
• Общаться с учениками в режиме диалога

<b>Для учеников:</b>
• Вы можете отправлять решения заданий наставнику
• Общаться с наставником в режиме диалога
• Менять уровень и наставника (с подтверждением)

<b>Для администраторов:</b>
• Доступны дополнительные команды (/admin, /stats, /broadcast, /check_data, /fix_data)
• Управление статистикой и рассылками
• Проверка и исправление данных
• Отправка заданий ученикам через рассылку
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
    
    kb = InlineKeyboardMarkup(row_width=2)
    # Добавляем кнопки для изменения данных (только для обычных пользователей, не суперадмина)
    if message.from_user.id != YOUR_ADMIN_ID:
        kb.add(InlineKeyboardButton("🔄 Изменить наставника", callback_data="change_mentor_btn"))
        kb.add(InlineKeyboardButton("📊 Изменить уровень", callback_data="change_level_btn"))
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
    
    is_admin = user_id in [OLGA_ID, YOUR_ADMIN_ID]
    
    if not is_admin and str(user_id) not in users:
        await message.answer("Вы не зарегистрированы. Используйте /start для регистрации.")
        return
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)
    
    has_students = any(u.get("mentor") == str(user_id) for u in users.values())
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for lvl in LEVELS_ORDER:
        if is_admin or has_students:
            kb.add(InlineKeyboardButton(lvl, callback_data=f"show_students:{lvl}"))
    
    if is_admin or (not is_admin and has_students):
        kb.add(InlineKeyboardButton("🌳 Вся моя ветка", callback_data="my_full_branch"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
    
    if is_admin:
        await message.answer("👑 <b>Администраторская панель учеников</b>\nВыберите уровень или просмотр ветки:", reply_markup=kb)
    elif has_students:
        await message.answer("Выберите уровень или просмотр ветки:", reply_markup=kb)
    else:
        await message.answer("У вас пока нет учеников.", reply_markup=kb)

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
        and int(uid) != YOUR_ADMIN_ID  # Исключаем суперадмина
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

# --- ПОДТВЕРЖДЕНИЕ НАСТАВНИКОМ ---
@dp.callback_query_handler(lambda c: c.data.startswith("mentor_accept:"))
async def mentor_accept(callback):
    """ИСПРАВЛЕННЫЙ обработчик принятия наставника"""
    try:
        callback_data = callback.data
        
        # Проверяем, что callback_data существует и имеет правильный формат
        if not callback_data or ':' not in callback_data:
            await callback.answer("Ошибка: некорректные данные", show_alert=True)
            return
            
        # Разбираем callback_data - формат: "mentor_accept:user_id"
        parts = callback_data.split(':')
        
        # Проверяем, что есть все необходимые части
        if len(parts) < 2:
            await callback.answer("Ошибка: недостаточно данных", show_alert=True)
            return
            
        # Получаем user_id (вторая часть после разделения)
        user_id_str = parts[1]
        
        # Проверяем, что user_id_str не пустой и состоит из цифр
        if not user_id_str or not user_id_str.isdigit():
            await callback.answer("Ошибка: неверный ID пользователя", show_alert=True)
            return
            
        # Преобразуем в str только после всех проверок
        chosen_user_id = user_id_str
        
        data = load_users()
        users = data["users"]
        
        # Проверяем, что пользователь существует
        if chosen_user_id not in users:
            await callback.answer("Ошибка: пользователь не найден", show_alert=True)
            return
        
        # Получаем mentor_id из данных пользователя
        mentor_id = users[chosen_user_id].get("pending_mentor")
        if not mentor_id:
            await callback.answer("Ошибка: не найден запрос на наставничество", show_alert=True)
            return
        
        # Проверяем, что текущий пользователь действительно является ожидаемым наставником
        if str(callback.from_user.id) != mentor_id:
            await callback.answer("Ошибка: вы не являетесь ожидаемым наставником", show_alert=True)
            return
        
        # Принимаем наставника
        users[chosen_user_id]["mentor"] = mentor_id
        users[chosen_user_id].pop("pending_mentor", None)
        
        if not save_users(data):
            await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
            return

        await callback.message.edit_text(
            f"Вы приняли ученика <b>{users[chosen_user_id]['name']} {users[chosen_user_id].get('surname','')}</b>"
        )
        
        # Получаем ID наставника как число для передачи в mentor_main_menu
        try:
            mentor_id_int = int(mentor_id)
        except ValueError:
            mentor_id_int = callback.from_user.id
        
        # Для Ольги - показываем админ-меню, для остальных - обычное меню наставника
        if mentor_id_int == OLGA_ID or mentor_id_int == YOUR_ADMIN_ID:
            await admin_main_menu(mentor_id_int)
        else:
            await mentor_main_menu(mentor_id_int)
            
        await bot.send_message(chosen_user_id, "Наставник подтвердил ваш выбор ✅")
        
    except Exception as e:
        # Логируем ошибку для отладки
        log_error(f"Ошибка в mentor_accept: {e}")
        log_error(f"Callback data: {callback.data if callback else 'No callback'}")
        
        # Уведомляем пользователя об ошибке
        await callback.answer("Произошла ошибка при обработке запроса", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("mentor_decline:"))
async def mentor_decline(callback):
    """ИСПРАВЛЕННЫЙ обработчик отклонения наставника"""
    try:
        callback_data = callback.data
        
        # Проверяем, что callback_data существует и имеет правильный формат
        if not callback_data or ':' not in callback_data:
            await callback.answer("Ошибка: некорректные данные", show_alert=True)
            return
            
        # Разбираем callback_data - формат: "mentor_decline:user_id"
        parts = callback.data.split(':')
        
        # Проверяем, что есть все необходимые части
        if len(parts) < 2:
            await callback.answer("Ошибка: недостаточно данных", show_alert=True)
            return
            
        # Получаем user_id (вторая часть после разделения)
        user_id_str = parts[1]
        
        # Проверяем, что user_id_str не пустой и состоит из цифр
        if not user_id_str or not user_id_str.isdigit():
            await callback.answer("Ошибка: неверный ID пользователя", show_alert=True)
            return
            
        # Преобразуем в str только после всех проверок
        chosen_user_id = user_id_str
        
        data = load_users()
        users = data["users"]
        
        # Проверяем, что пользователь существует
        if chosen_user_id not in users:
            await callback.answer("Ошибка: пользователь не найден", show_alert=True)
            return
        
        # Получаем mentor_id из данных пользователя
        mentor_id = users[chosen_user_id].get("pending_mentor")
        if not mentor_id:
            await callback.answer("Ошибка: не найден запрос на наставничество", show_alert=True)
            return
        
        # Проверяем, что текущий пользователь действительно является ожидаемым наставником
        if str(callback.from_user.id) != mentor_id:
            await callback.answer("Ошибка: вы не являетесь ожидаемым наставником", show_alert=True)
            return
        
        # Отклоняем наставника
        users[chosen_user_id].pop("pending_mentor", None)
        
        if not save_users(data):
            await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
            return

        await callback.message.edit_text("Отказано.")
        await bot.send_message(chosen_user_id, "Наставник отклонил ваш выбор.")
        
    except Exception as e:
        # Логируем ошибку для отладки
        log_error(f"Ошибка в mentor_decline: {e}")
        log_error(f"Callback data: {callback.data if callback else 'No callback'}")
        
        # Уведомляем пользователя об ошибке
        await callback.answer("Произошла ошибка при обработке запроса", show_alert=True)

# --- ИЗМЕНЕНИЕ НАСТАВНИКА ---
@dp.callback_query_handler(lambda c: c.data == "change_mentor_btn")
async def change_mentor_btn(callback):
    user_id = str(callback.from_user.id)
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Вы не зарегистрированы", show_alert=True)
        return
    
    # Проверяем, есть ли текущий наставник
    if not users[user_id].get("mentor"):
        await callback.answer("У вас нет наставника для изменения", show_alert=True)
        return
    
    await callback.message.answer("Выберите уровень нового наставника:")
    await Form.change_mentor.set()
    
    # Показываем только уровни, которые выше или равны текущему
    current_level = users[user_id].get("level", "НП")
    current_level_idx = LEVELS_ORDER.index(current_level) if current_level in LEVELS_ORDER else 0
    available_levels = LEVELS_ORDER[current_level_idx:]
    
    kb = InlineKeyboardMarkup()
    for lvl in available_levels:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"change_mentor_level:{lvl}"))
    
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_change"))
    
    await callback.message.answer("Выберите уровень наставника (только равный или выше вашего текущего уровня):", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("change_mentor_level:"), state=Form.change_mentor)
async def change_mentor_level(callback, state):
    level = callback.data.split(":")[1]
    await state.update_data(new_mentor_level=level)
    
    data_users = load_users()
    users = data_users["users"]
    user_id = str(callback.from_user.id)
    
    # Исключаем текущего наставника и суперадмина
    current_mentor = users[user_id].get("mentor")
    
    mentors = [
        (uid, u) for uid, u in users.items() 
        if u.get("level") == level 
        and int(uid) != YOUR_ADMIN_ID  # Исключаем суперадмина
        and uid != current_mentor  # Исключаем текущего наставника
        and uid != user_id  # Исключаем самого себя
    ]

    if not mentors:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("⬅ Назад", callback_data="change_mentor_btn"))
        await callback.message.answer("Нет наставников на этом уровне.", reply_markup=kb)
        return

    kb = InlineKeyboardMarkup()
    for uid, u in sorted(mentors, key=lambda x: x[1]["name"]):
        full_name = f"{u['name']} {u.get('surname','')}".strip()
        kb.add(InlineKeyboardButton(f"{full_name} — {u['level']}", callback_data=f"select_new_mentor:{uid}"))
    
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_change"))
    
    await callback.message.answer("Выберите нового наставника:", reply_markup=kb)
    await callback.message.delete()

@dp.callback_query_handler(lambda c: c.data.startswith("select_new_mentor:"), state=Form.change_mentor)
async def select_new_mentor(callback, state):
    new_mentor_id = callback.data.split(":")[1]
    user_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    # Сохраняем данные о запросе
    users[user_id]["pending_new_mentor"] = new_mentor_id
    users[user_id]["mentor_change_request"] = str(datetime.now())
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return
    
    user_name = f"{users[user_id]['name']} {users[user_id].get('surname','')}".strip()
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Принять", callback_data=f"accept_new_mentor:{user_id}"))
    kb.add(InlineKeyboardButton("❌ Отклонить", callback_data=f"decline_new_mentor:{user_id}"))
    
    await bot.send_message(
        new_mentor_id,
        f"<b>Запрос на смену наставника</b>\n\n"
        f"Пользователь <b>{user_name}</b> хочет выбрать вас своим новым наставником.\n"
        f"Текущий уровень пользователя: <b>{users[user_id].get('level','—')}</b>\n\n"
        f"Вы согласны стать наставником этого пользователя?",
        reply_markup=kb
    )
    
    await callback.message.answer(f"Запрос на смену наставника отправлен. Ожидайте подтверждения.")
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("accept_new_mentor:"))
async def accept_new_mentor(callback):
    user_id = callback.data.split(":")[1]
    new_mentor_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    # Проверяем, что запрос еще актуален
    if users[user_id].get("pending_new_mentor") != new_mentor_id:
        await callback.answer("Запрос устарел или недействителен", show_alert=True)
        return
    
    # Сохраняем старого наставника для уведомления
    old_mentor_id = users[user_id].get("mentor")
    
    # Меняем наставника
    users[user_id]["mentor"] = new_mentor_id
    users[user_id].pop("pending_new_mentor", None)
    users[user_id].pop("mentor_change_request", None)
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return
    
    user_name = f"{users[user_id]['name']} {users[user_id].get('surname','')}".strip()
    
    # Уведомляем нового наставника
    await callback.message.edit_text(f"✅ Вы приняли пользователя <b>{user_name}</b> как своего ученика.")
    
    # Уведомляем ученика
    new_mentor_name = f"{users[new_mentor_id]['name']} {users[new_mentor_id].get('surname','')}".strip()
    await bot.send_message(user_id, f"✅ Наставник <b>{new_mentor_name}</b> принял ваш запрос на смену наставника.")
    
    # Уведомляем старого наставника (если был)
    if old_mentor_id and old_mentor_id in users:
        old_mentor_name = f"{users[old_mentor_id]['name']} {users[old_mentor_id].get('surname','')}".strip()
        await bot.send_message(old_mentor_id, f"ℹ️ Ваш ученик <b>{user_name}</b> сменил наставника на <b>{new_mentor_name}</b>.")
    
    log_info(f"Пользователь {user_id} ({user_name}) сменил наставника с {old_mentor_id} на {new_mentor_id}")

@dp.callback_query_handler(lambda c: c.data.startswith("decline_new_mentor:"))
async def decline_new_mentor(callback):
    user_id = callback.data.split(":")[1]
    declined_mentor_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    # Проверяем, что запрос еще актуален
    if users[user_id].get("pending_new_mentor") != declined_mentor_id:
        await callback.answer("Запрос устарел или недействителен", show_alert=True)
        return
    
    # Удаляем запрос
    users[user_id].pop("pending_new_mentor", None)
    users[user_id].pop("mentor_change_request", None)
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return
    
    user_name = f"{users[user_id]['name']} {users[user_id].get('surname','')}".strip()
    
    # Уведомляем наставника, который отклонил
    await callback.message.edit_text(f"❌ Вы отклонили запрос от пользователя <b>{user_name}</b>.")
    
    # Уведомляем ученика
    declined_mentor_name = f"{users[declined_mentor_id]['name']} {users[declined_mentor_id].get('surname','')}".strip()
    await bot.send_message(user_id, f"❌ Наставник <b>{declined_mentor_name}</b> отклонил ваш запрос на смену наставника.")

# --- ИЗМЕНЕНИЕ УРОВНЯ ---
@dp.callback_query_handler(lambda c: c.data == "change_level_btn")
async def change_level_btn(callback):
    user_id = str(callback.from_user.id)
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Вы не зарегистрированы", show_alert=True)
        return
    
    # Проверяем, есть ли текущий наставник
    if not users[user_id].get("mentor"):
        await callback.answer("Для смены уровня требуется наличие наставника", show_alert=True)
        return
    
    await callback.message.answer("Выберите новый уровень:")
    await Form.change_level.set()
    
    # Показываем все уровни (можно менять на любой)
    kb = InlineKeyboardMarkup()
    for lvl in LEVELS_ORDER:
        kb.add(InlineKeyboardButton(lvl, callback_data=f"select_new_level:{lvl}"))
    
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_change"))
    
    current_level = users[user_id].get("level", "—")
    await callback.message.answer(f"Ваш текущий уровень: <b>{current_level}</b>\nВыберите новый уровень:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("select_new_level:"), state=Form.change_level)
async def select_new_level(callback, state):
    new_level = callback.data.split(":")[1]
    user_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    current_level = users[user_id].get("level", "—")
    
    # Если уровень не изменился
    if new_level == current_level:
        await callback.answer("Вы выбрали тот же уровень", show_alert=True)
        return
    
    # Сохраняем запрос на изменение уровня
    users[user_id]["pending_level"] = new_level
    users[user_id]["level_change_request"] = str(datetime.now())
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return
    
    user_name = f"{users[user_id]['name']} {users[user_id].get('surname','')}".strip()
    mentor_id = users[user_id].get("mentor")
    
    if mentor_id and mentor_id in users:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_level:{user_id}:{new_level}"))
        kb.add(InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_level:{user_id}"))
        
        await bot.send_message(
            mentor_id,
            f"<b>Запрос на смену уровня</b>\n\n"
            f"Ваш ученик <b>{user_name}</b> запрашивает смену уровня.\n"
            f"Текущий уровень: <b>{current_level}</b>\n"
            f"Новый уровень: <b>{new_level}</b>\n\n"
            f"Вы подтверждаете смену уровня?",
            reply_markup=kb
        )
        
        await callback.message.answer(f"Запрос на смену уровня отправлен вашему наставнику. Ожидайте подтверждения.")
    else:
        await callback.answer("Ошибка: наставник не найден", show_alert=True)
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_level:"))
async def confirm_level(callback):
    parts = callback.data.split(":")
    user_id = parts[1]
    new_level = parts[2]
    mentor_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    # Проверяем, что запрос еще актуален и что это действительно наставник
    if users[user_id].get("mentor") != mentor_id:
        await callback.answer("Вы не являетесь наставником этого пользователя", show_alert=True)
        return
    
    if users[user_id].get("pending_level") != new_level:
        await callback.answer("Запрос устарел или недействителен", show_alert=True)
        return
    
    # Меняем уровень
    old_level = users[user_id].get("level", "—")
    users[user_id]["level"] = new_level
    users[user_id].pop("pending_level", None)
    users[user_id].pop("level_change_request", None)
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return
    
    user_name = f"{users[user_id]['name']} {users[user_id].get('surname','')}".strip()
    
    # Уведомляем наставника
    await callback.message.edit_text(f"✅ Вы подтвердили смену уровня для <b>{user_name}</b> с {old_level} на {new_level}.")
    
    # Уведомляем ученика
    mentor_name = f"{users[mentor_id]['name']} {users[mentor_id].get('surname','')}".strip()
    await bot.send_message(user_id, f"✅ Ваш наставник <b>{mentor_name}</b> подтвердил смену уровня.\nВаш новый уровень: <b>{new_level}</b>")
    
    log_info(f"Пользователь {user_id} ({user_name}) сменил уровень с {old_level} на {new_level}")

@dp.callback_query_handler(lambda c: c.data.startswith("reject_level:"))
async def reject_level(callback):
    user_id = callback.data.split(":")[1]
    mentor_id = str(callback.from_user.id)
    
    data = load_users()
    users = data["users"]
    
    if user_id not in users:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    # Проверяем, что это действительно наставник
    if users[user_id].get("mentor") != mentor_id:
        await callback.answer("Вы не являетесь наставником этого пользователя", show_alert=True)
        return
    
    # Удаляем запрос
    new_level = users[user_id].get("pending_level", "—")
    users[user_id].pop("pending_level", None)
    users[user_id].pop("level_change_request", None)
    
    if not save_users(data):
        await callback.answer("❌ Ошибка сохранения данных", show_alert=True)
        return
    
    user_name = f"{users[user_id]['name']} {users[user_id].get('surname','')}".strip()
    
    # Уведомляем наставника
    await callback.message.edit_text(f"❌ Вы отклонили смену уровня для <b>{user_name}</b>.")
    
    # Уведомляем ученика
    mentor_name = f"{users[mentor_id]['name']} {users[mentor_id].get('surname','')}".strip()
    await bot.send_message(user_id, f"❌ Ваш наставник <b>{mentor_name}</b> отклонил смену уровня на <b>{new_level}</b>.")

# --- ОБЩИЙ ОБРАБОТЧИК ОТМЕНЫ ---
@dp.callback_query_handler(lambda c: c.data == "cancel_change", state=[Form.change_level, Form.change_mentor])
async def cancel_change(callback, state):
    await state.finish()
    await callback.message.answer("❌ Изменение отменено.")
    await callback.message.delete()

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
    
    kb = InlineKeyboardMarkup(row_width=2)
    # Добавляем кнопки для изменения данных (только для обычных пользователей, не суперадмина)
    if callback.from_user.id != YOUR_ADMIN_ID:
        kb.add(InlineKeyboardButton("🔄 Изменить наставника", callback_data="change_mentor_btn"))
        kb.add(InlineKeyboardButton("📊 Изменить уровень", callback_data="change_level_btn"))
    kb.add(InlineKeyboardButton("⬅ Назад в меню", callback_data="back_main"))
    await callback.message.answer(text, reply_markup=kb)

# --- Меню "Мои ученики" ---
@dp.callback_query_handler(lambda c: c.data == "show_my_students")
async def my_students(callback):
    user_id = callback.from_user.id
    data = load_users()
    users = data["users"]
    
    today_str = str(date.today())
    
    is_admin = user_id in [OLGA_ID, YOUR_ADMIN_ID]
    
    if not is_admin and str(user_id) not in users:
        await callback.answer("Вы не зарегистрированы", show_alert=True)
        return
    
    if str(user_id) in users:
        users[str(user_id)]["active_today"] = today_str
        save_users(data)
    
    has_students = any(u.get("mentor") == str(user_id) for u in users.values())
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for lvl in LEVELS_ORDER:
        if is_admin or has_students:
            kb.add(InlineKeyboardButton(lvl, callback_data=f"show_students:{lvl}"))
    
    if is_admin:
        kb.add(InlineKeyboardButton("🌳 Вся моя ветка", callback_data="my_full_branch"))
    elif has_students:
        kb.add(InlineKeyboardButton("🌳 Вся моя ветка", callback_data="my_full_branch"))
    
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
    
    if is_admin:
        await callback.message.answer("👑 <b>Администраторская панель учеников</b>\nВыберите уровень или просмотр ветки:", reply_markup=kb)
    elif has_students:
        await callback.message.answer("Выберите уровень или просмотр ветки:", reply_markup=kb)
    else:
        await callback.message.answer("У вас пока нет учеников.", reply_markup=kb)

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

    is_admin = user_id in [OLGA_ID, YOUR_ADMIN_ID]
    
    if is_admin:
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
        
        if is_admin and u.get("mentor"):
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
    
    if callback.from_user.id == YOUR_ADMIN_ID:
        await full_hierarchy(callback)
        return
    
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
    
    # Используем безопасную отправку
    await safe_send_message(callback.from_user.id, text, reply_markup=kb)

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
    
    # Используем безопасную отправку
    await safe_send_message(callback.from_user.id, text, reply_markup=kb)

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
    
    # Используем безопасную отправку
    await safe_send_message(callback.from_user.id, text)

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
    
    # Используем безопасную отправку
    await safe_send_message(callback.from_user.id, text)

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

# --- НОВЫЙ ОБРАБОТЧИК РАССЫЛКИ С УЛУЧШЕННЫМИ УВЕДОМЛЕНИЯМИ ---
@dp.message_handler(state=Form.admin_message, content_types=types.ContentTypes.ANY)
async def admin_send_message_enhanced(message, state):
    data = await state.get_data()
    selected_levels = data.get("selected_levels", [])
    broadcast_to_all = data.get("broadcast_to_all", False)
    
    users_data = load_users()["users"]
    recipients = []
    recipient_names = []
    
    today_str = str(date.today())
    
    # Проверяем, является ли отправитель Ольгой или суперадмином
    is_assignment_admin = message.from_user.id in [OLGA_ID, YOUR_ADMIN_ID]
    
    # Проверяем, является ли сообщение заданием (содержит ключевые слова)
    is_assignment = False
    if is_assignment_admin and message.content_type == "text":
        assignment_keywords = ["задание", "упражнение", "задача", "домашнее", "homework", "exercise", "task"]
        if any(keyword in message.text.lower() for keyword in assignment_keywords):
            is_assignment = True
    
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
    
    # Если это задание от администратора, предлагаем отправить как задание
    if is_assignment and is_assignment_admin:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Отправить как ЗАДАНИЕ", callback_data="send_as_assignment"),
            InlineKeyboardButton("📢 Просто рассылка", callback_data="confirm_send")
        )
        kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_send"))
        
        preview_text = f"📚 <b>ОБНАРУЖЕНО ЗАДАНИЕ ОТ АДМИНИСТРАТОРА</b>\n\n"
        
        if broadcast_to_all:
            target = "ВСЕМ ученикам"
        elif selected_levels:
            target = f"ученикам уровней: {', '.join(selected_levels)}"
        else:
            target = "не выбранным ученикам"
            
        preview_text += f"• Кому: {target}\n"
        preview_text += f"• Получателей-учеников: {len(recipients)}\n"
        preview_text += f"• Тип: задание\n\n"
        preview_text += f"<b>Текст задания:</b>\n{message.text[:300]}..."
        
        await state.update_data(
            message_to_send=message,
            recipients=recipients,
            recipient_names=recipient_names,
            selected_levels=selected_levels,
            broadcast_to_all=broadcast_to_all,
            is_assignment=True
        )
        
        await message.answer(preview_text, reply_markup=kb)
        return
    
    # Обычная рассылка с улучшенным предпросмотром
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Отправить", callback_data="confirm_send_enhanced"),
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
        broadcast_to_all=broadcast_to_all,
        is_assignment=False
    )
    
    await message.answer(preview_text, reply_markup=kb)

# --- НОВЫЙ ОБРАБОТЧИК ПОДТВЕРЖДЕНИЯ РАССЫЛКИ С УЛУЧШЕННЫМИ УВЕДОМЛЕНИЯМИ ---
@dp.callback_query_handler(lambda c: c.data == "confirm_send_enhanced", state=Form.admin_message)
async def confirm_send_enhanced(callback, state):
    """Улучшенный обработчик подтверждения рассылки"""
    data = await state.get_data()
    message = data.get("message_to_send")
    recipients = data.get("recipients", [])
    selected_levels = data.get("selected_levels", [])
    broadcast_to_all = data.get("broadcast_to_all", False)
    is_assignment = data.get("is_assignment", False)
    
    await callback.message.edit_text(f"🔄 Начинаю рассылку...")
    
    # Формируем описание целевой аудитории
    if broadcast_to_all:
        target_description = "ВСЕМ пользователям"
    elif selected_levels:
        target_description = f"уровням: {', '.join(selected_levels)}"
    else:
        target_description = "выбранным пользователям"
    
    # Определяем тип рассылки
    broadcast_type = "assignment" if is_assignment else "regular"
    
    # Используем улучшенную функцию рассылки
    broadcast_id = await enhanced_broadcast(
        admin_id=callback.from_user.id,
        message=message,
        recipients=recipients,
        target_description=target_description,
        broadcast_type=broadcast_type
    )
    
    await state.finish()
    
    # Дополнительное меню для администратора
    if callback.from_user.id in [OLGA_ID, YOUR_ADMIN_ID]:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("📊 Статус рассылки", callback_data=f"broadcast_status:{broadcast_id}"),
            InlineKeyboardButton("📋 Список ошибок", callback_data=f"failed_list:{broadcast_id}:1")
        )
        kb.add(
            InlineKeyboardButton("📢 Новая рассылка", callback_data="admin_broadcast"),
            InlineKeyboardButton("⬅️ В меню", callback_data="back_main")
        )
        
        await callback.message.answer(
            f"✅ Рассылка запущена!\n\n"
            f"ID рассылки: <code>{broadcast_id}</code>\n"
            f"Вы получите уведомление о завершении.",
            reply_markup=kb,
            parse_mode="HTML"
        )

# --- НОВЫЙ ОБРАБОТЧИК ОТПРАВКИ КАК ЗАДАНИЕ С УЛУЧШЕННЫМИ УВЕДОМЛЕНИЯМИ ---
@dp.callback_query_handler(lambda c: c.data == "send_as_assignment", state=Form.admin_message)
async def send_as_assignment_enhanced(callback: types.CallbackQuery, state):
    """Администратор отправляет задание ученикам с улучшенными уведомлениями"""
    data = await state.get_data()
    message = data.get("message_to_send")
    selected_levels = data.get("selected_levels", [])
    broadcast_to_all = data.get("broadcast_to_all", False)
    
    await callback.message.edit_text(f"📚 Создаю задание...")
    
    # Загружаем данные
    users_data = load_users()["users"]
    assignments_data = load_assignments()
    
    # Создаем уникальный ID для задания
    assignment_id = f"assignment_{message.from_user.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    broadcast_id = f"broadcast_assignment_{assignment_id}"
    
    # Получаем имя администратора
    admin_name = "Ольга" if callback.from_user.id == OLGA_ID else "Суперадмин"
    
    # Собираем информацию о задании
    assignment_info = {
        "assignment_id": assignment_id,
        "broadcast_id": broadcast_id,
        "from_admin": True,
        "admin_id": str(callback.from_user.id),
        "admin_name": admin_name,
        "levels": selected_levels if not broadcast_to_all else ["ALL"],
        "timestamp": str(datetime.now()),
        "content_type": message.content_type,
        "sent_count": 0,
        "solutions_count": 0
    }
    
    if message.content_type == "text":
        assignment_info["text"] = message.text
    elif message.content_type == "photo":
        assignment_info["photo_id"] = message.photo[-1].file_id
        assignment_info["caption"] = message.caption
    elif message.content_type == "document":
        assignment_info["document_id"] = message.document.file_id
        assignment_info["caption"] = message.caption
    elif message.content_type == "voice":
        assignment_info["voice_id"] = message.voice.file_id
    elif message.content_type == "video":
        assignment_info["video_id"] = message.video.file_id
        assignment_info["caption"] = message.caption
    
    # Формируем описание целевой аудитории для уведомления
    if broadcast_to_all:
        target_description = "ВСЕМ ученикам"
    elif selected_levels:
        target_description = f"ученикам уровней: {', '.join(selected_levels)}"
    else:
        target_description = "выбранным ученикам"
    
    # Отправляем задание всем ученикам выбранных уровней
    recipients = []
    sent_to_students = []
    failed_deliveries = []
    
    await send_admin_notification(
        callback.from_user.id,
        "Начинаю отправку задания",
        f"🔹 Тип: Задание от администратора\n"
        f"🔹 Администратор: {admin_name}\n"
        f"🔹 Целевая аудитория: {target_description}",
        broadcast_id
    )
    
    total_students = 0
    for uid, u in users_data.items():
        if (broadcast_to_all or u.get("level") in selected_levels) and int(uid) not in [OLGA_ID, YOUR_ADMIN_ID]:
            total_students += 1
    
    sent_count = 0
    failed_count = 0
    
    for i, (uid, u) in enumerate(users_data.items(), 1):
        # Проверяем, что пользователь ученик (не админ) и его уровень в выбранных
        if (broadcast_to_all or u.get("level") in selected_levels) and int(uid) not in [OLGA_ID, YOUR_ADMIN_ID]:
            
            recipients.append(uid)
            user_name = f"{u['name']} {u.get('surname', '')}".strip()
            
            try:
                # Создаем клавиатуру для ученика
                kb_student = InlineKeyboardMarkup()
                kb_student.add(
                    InlineKeyboardButton("📤 Отправить решение наставнику", 
                                        callback_data=f"send_solution_to_mentor:{assignment_id}")
                )
                
                # Отправляем задание ученику
                if message.content_type == "text":
                    await bot.send_message(
                        uid,
                        f"📚 <b>НОВОЕ ЗАДАНИЕ ОТ {admin_name.upper()}</b>\n\n"
                        f"{message.text}\n\n"
                        f"<i>Нажмите кнопку ниже, чтобы отправить решение вашему наставнику</i>",
                        reply_markup=kb_student,
                        parse_mode="HTML"
                    )
                elif message.content_type == "photo":
                    await bot.send_photo(
                        uid,
                        message.photo[-1].file_id,
                        caption=f"📚 <b>НОВОЕ ЗАДАНИЕ ОТ {admin_name.upper()}</b>\n\n"
                               f"{message.caption or ''}\n\n"
                               f"<i>Нажмите кнопку ниже, чтобы отправить решение вашему наставнику</i>",
                        reply_markup=kb_student,
                        parse_mode="HTML"
                    )
                
                sent_to_students.append({
                    "student_id": uid,
                    "student_name": user_name,
                    "mentor_id": u.get("mentor"),
                    "level": u.get("level")
                })
                
                assignment_info["sent_count"] += 1
                sent_count += 1
                
                # Периодически отправляем обновление о прогрессе
                if i % 10 == 0:
                    await send_broadcast_progress_update(
                        callback.from_user.id, broadcast_id, i, total_students, sent_count, failed_count
                    )
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                failed_count += 1
                error_type = classify_error(str(e))
                error_message = str(e)
                
                failed_deliveries.append({
                    "user_id": uid,
                    "user_name": user_name,
                    "error_type": error_type,
                    "error_message": error_message,
                    "timestamp": str(datetime.now())
                })
                
                # Сохраняем в историю
                add_failed_delivery(
                    broadcast_id, uid, user_name, error_type, error_message, str(datetime.now())
                )
                
                log_error(f"Ошибка отправки задания ученику {uid}: {error_type} - {error_message}")
    
    # Сохраняем задание
    assignments_data.setdefault("assignments", {})[assignment_id] = assignment_info
    
    # Сохраняем информацию о том, кому отправлено
    assignments_data.setdefault("assignment_recipients", {})[assignment_id] = sent_to_students
    
    if save_assignments(assignments_data):
        # Сохраняем информацию о рассылке в историю
        add_broadcast_to_history(
            broadcast_id=broadcast_id,
            admin_id=str(callback.from_user.id),
            target=target_description,
            recipients_count=len(recipients),
            sent_count=sent_count,
            failed_count=failed_count,
            message_type=message.content_type,
            timestamp=str(datetime.now())
        )
        
        # Отправляем итоговую сводку
        await send_broadcast_summary(
            callback.from_user.id, broadcast_id, len(recipients), 
            sent_count, failed_count, target_description, failed_deliveries
        )
        
        # Дополнительное меню для задания
        kb_admin = InlineKeyboardMarkup(row_width=2)
        kb_admin.add(
            InlineKeyboardButton("📊 Статус выполнения", callback_data=f"check_assignment:{assignment_id}"),
            InlineKeyboardButton("📋 Ошибки доставки", callback_data=f"failed_list:{broadcast_id}:1")
        )
        kb_admin.add(
            InlineKeyboardButton("📝 Новое задание", callback_data="admin_broadcast"),
            InlineKeyboardButton("⬅️ В меню", callback_data="back_main")
        )
        
        await callback.message.answer(
            f"✅ Задание успешно создано!\n\n"
            f"ID задания: <code>{assignment_id}</code>\n"
            f"ID рассылки: <code>{broadcast_id}</code>",
            reply_markup=kb_admin,
            parse_mode="HTML"
        )
    else:
        await callback.message.edit_text("❌ Ошибка сохранения задания")
    
    await state.finish()

# --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ ПРОСМОТРА ОШИБОК И СТАТИСТИКИ ---

@dp.callback_query_handler(lambda c: c.data.startswith("broadcast_report:"))
async def show_broadcast_report(callback: types.CallbackQuery):
    """Показать детальный отчет по рассылке"""
    broadcast_id = callback.data.split(":")[1]
    
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ только для администраторов", show_alert=True)
        return
    
    broadcast_stats = get_broadcast_stats(broadcast_id)
    if not broadcast_stats:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    failed_deliveries = get_failed_deliveries_by_broadcast(broadcast_id)
    error_groups = group_errors_by_type(failed_deliveries)
    
    text = f"📊 <b>ДЕТАЛЬНЫЙ ОТЧЕТ ПО РАССЫЛКЕ</b>\n\n"
    text += f"🔹 ID: <code>{broadcast_id}</code>\n"
    text += f"🔹 Целевая аудитория: {broadcast_stats['target']}\n"
    text += f"🔹 Дата: {broadcast_stats['timestamp']}\n"
    text += f"🔹 Тип сообщения: {broadcast_stats['message_type']}\n\n"
    
    text += f"<b>СТАТИСТИКА:</b>\n"
    text += f"• Всего получателей: {broadcast_stats['recipients_count']}\n"
    text += f"• Успешно отправлено: {broadcast_stats['sent_count']}\n"
    text += f"• Не отправлено: {broadcast_stats['failed_count']}\n\n"
    
    if error_groups:
        text += f"<b>ГРУППИРОВКА ОШИБОК:</b>\n"
        for error_type, errors in error_groups.items():
            error_name = BROADCAST_ERROR_TYPES.get(error_type, "Неизвестная ошибка")
            text += f"\n📌 <b>{error_name}</b> ({len(errors)}):\n"
            for error in errors[:5]:  # Показываем только первые 5
                text += f"   • {error['user_name']} (ID: {error['user_id']})\n"
            if len(errors) > 5:
                text += f"   ... и еще {len(errors) - 5}\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📋 Полный список ошибок", 
                               callback_data=f"failed_list:{broadcast_id}:1"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast"))
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data.startswith("failed_list:"))
async def show_failed_list(callback: types.CallbackQuery):
    """Показать полный список неудачных отправок с постраничной навигацией"""
    parts = callback.data.split(":")
    broadcast_id = parts[1]
    page = int(parts[2]) if len(parts) > 2 else 1
    
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ только для администраторов", show_alert=True)
        return
    
    failed_deliveries = get_failed_deliveries_by_broadcast(broadcast_id)
    if not failed_deliveries:
        await callback.answer("Нет неудачных отправок", show_alert=True)
        return
    
    # Пагинация
    items_per_page = 10
    total_pages = (len(failed_deliveries) + items_per_page - 1) // items_per_page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_items = failed_deliveries[start_idx:end_idx]
    
    text = f"📋 <b>СПИСОК НЕОТПРАВЛЕННЫХ СООБЩЕНИЙ</b>\n\n"
    text += f"🔹 ID рассылки: <code>{broadcast_id}</code>\n"
    text += f"🔹 Всего ошибок: {len(failed_deliveries)}\n"
    text += f"🔹 Страница {page} из {total_pages}\n\n"
    
    for i, delivery in enumerate(page_items, start_idx + 1):
        error_name = BROADCAST_ERROR_TYPES.get(delivery['error_type'], "Неизвестная ошибка")
        text += f"{i}. <b>{delivery['user_name']}</b>\n"
        text += f"   ID: {delivery['user_id']}\n"
        text += f"   Ошибка: {error_name}\n"
        if len(delivery['error_message']) < 100:
            text += f"   Сообщение: {delivery['error_message']}\n"
        text += "\n"
    
    # Клавиатура для навигации
    kb = InlineKeyboardMarkup(row_width=5)
    
    # Кнопки навигации
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"failed_list:{broadcast_id}:{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"failed_list:{broadcast_id}:{page+1}"))
    
    if nav_buttons:
        kb.row(*nav_buttons)
    
    kb.add(
        InlineKeyboardButton("📊 Детальный отчет", callback_data=f"broadcast_report:{broadcast_id}"),
        InlineKeyboardButton("🔄 Повторить ошибки", callback_data=f"retry_failed:{broadcast_id}")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast"))
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data.startswith("broadcast_status:"))
async def show_broadcast_status(callback: types.CallbackQuery):
    """Показать статус рассылки"""
    broadcast_id = callback.data.split(":")[1]
    
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ только для администраторов", show_alert=True)
        return
    
    broadcast_stats = get_broadcast_stats(broadcast_id)
    if not broadcast_stats:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    text = f"📊 <b>СТАТУС РАССЫЛКИ</b>\n\n"
    text += f"🔹 ID: <code>{broadcast_id}</code>\n"
    text += f"🔹 Администратор: {broadcast_stats['admin_id']}\n"
    text += f"🔹 Дата: {broadcast_stats['timestamp']}\n"
    text += f"🔹 Целевая аудитория: {broadcast_stats['target']}\n\n"
    
    text += f"<b>СТАТИСТИКА ДОСТАВКИ:</b>\n"
    text += f"• Всего получателей: {broadcast_stats['recipients_count']}\n"
    text += f"• Успешно отправлено: {broadcast_stats['sent_count']}\n"
    text += f"• Не отправлено: {broadcast_stats['failed_count']}\n"
    
    success_rate = (broadcast_stats['sent_count'] / broadcast_stats['recipients_count'] * 100) \
        if broadcast_stats['recipients_count'] > 0 else 0
    text += f"• Успешность: {success_rate:.1f}%\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("📋 Детальный отчет", callback_data=f"broadcast_report:{broadcast_id}"),
        InlineKeyboardButton("📊 Общая статистика", callback_data="broadcast_stats_overview")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast"))
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data == "broadcast_stats_overview")
async def show_broadcast_stats_overview(callback: types.CallbackQuery):
    """Показать общую статистику по всем рассылкам"""
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ только для администраторов", show_alert=True)
        return
    
    history = load_broadcast_history()
    stats = history.get("stats", {})
    broadcasts = history.get("broadcasts", {})
    
    text = f"📈 <b>ОБЩАЯ СТАТИСТИКА РАССЫЛОК</b>\n\n"
    text += f"• Всего рассылок: {stats.get('total_broadcasts', 0)}\n"
    text += f"• Всего отправлено сообщений: {stats.get('total_sent', 0)}\n"
    text += f"• Всего ошибок доставки: {stats.get('total_failed', 0)}\n\n"
    
    if broadcasts:
        # Последние 5 рассылок
        text += f"<b>ПОСЛЕДНИЕ РАССЫЛКИ:</b>\n"
        recent_broadcasts = sorted(
            broadcasts.items(),
            key=lambda x: x[1].get('timestamp', ''),
            reverse=True
        )[:5]
        
        for i, (broadcast_id, broadcast_data) in enumerate(recent_broadcasts, 1):
            date_str = broadcast_data.get('timestamp', '')[:16]
            success_rate = (broadcast_data['sent_count'] / broadcast_data['recipients_count'] * 100) \
                if broadcast_data['recipients_count'] > 0 else 0
            
            text += f"\n{i}. {date_str}\n"
            text += f"   📊 {broadcast_data['sent_count']}/{broadcast_data['recipients_count']} "
            text += f"({success_rate:.0f}%)\n"
            text += f"   🎯 {broadcast_data['target'][:30]}...\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🧹 Очистить старые данные", callback_data="cleanup_old_data"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_broadcast"))
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data.startswith("retry_failed:"))
async def retry_failed_deliveries(callback: types.CallbackQuery):
    """Повторная отправка сообщений с ошибками"""
    broadcast_id = callback.data.split(":")[1]
    
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ только для администраторов", show_alert=True)
        return
    
    # Нужно найти оригинальное сообщение рассылки
    # В реальной реализации нужно хранить само сообщение или его параметры
    # Здесь упрощенный вариант - запрашиваем сообщение заново
    
    await callback.message.answer(
        "🔄 <b>Повторная отправка сообщений с ошибками</b>\n\n"
        "Отправьте сообщение, которое нужно повторно отправить пользователям, "
        "у которых была ошибка доставки.",
        parse_mode="HTML"
    )
    
    state = dp.current_state(user=callback.from_user.id, chat=callback.from_user.id)
    await state.update_data(
        retry_broadcast_id=broadcast_id,
        is_retry=True
    )
    
    await Form.admin_message.set()
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "cleanup_old_data")
async def cleanup_data_handler(callback: types.CallbackQuery):
    """Очистка старых данных"""
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Доступ только для администраторов", show_alert=True)
        return
    
    await callback.message.answer("🧹 Начинаю очистку старых данных...")
    
    cleaned_count = cleanup_old_data(days_to_keep=7)
    
    await callback.message.answer(
        f"✅ Очистка завершена!\n\n"
        f"• Удалено старых рассылок: {cleaned_count}\n"
        f"• Удалены старые backup файлы\n"
        f"• Данные актуальны (хранятся 7 дней)"
    )

@dp.callback_query_handler(lambda c: c.data == "noop")
async def noop_handler(callback: types.CallbackQuery):
    """Пустой обработчик для кнопок-заглушек"""
    await callback.answer()

# --- ПРОСМОТР РЕШЕНИЙ УЧЕНИКОВ ---
@dp.callback_query_handler(lambda c: c.data == "view_student_solutions")
async def view_student_solutions(callback: types.CallbackQuery):
    """Наставник просматривает решения от своих учеников"""
    mentor_id = str(callback.from_user.id)
    
    assignments_data = load_assignments()
    solutions = assignments_data.get("solutions", {})
    
    # Фильтруем решения, предназначенные этому наставнику
    mentor_solutions = []
    for solution_id, solution in solutions.items():
        if solution.get("mentor_id") == mentor_id:
            mentor_solutions.append(solution)
    
    if not mentor_solutions:
        await callback.message.answer(
            "📭 <b>У вас пока нет решений от учеников</b>\n\n"
            "Когда ваши ученики отправят решения заданий от администраторов, "
            "они появятся здесь.",
            parse_mode="HTML"
        )
        return
    
    # Сортируем по времени (новые сначала)
    mentor_solutions.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    
    text = f"📥 <b>Решения от ваших учеников</b>\n\n"
    text += f"Всего решений: {len(mentor_solutions)}\n\n"
    
    # Показываем последние 5 решений
    for i, solution in enumerate(mentor_solutions[:5], 1):
        timestamp = solution.get("timestamp", "")
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                time_str = dt.strftime("%d.%m %H:%M")
            except:
                time_str = timestamp
        else:
            time_str = "?"
        
        student_name = solution.get("student_name", "Ученик")
        preview = ""
        
        if solution.get("text"):
            preview = solution["text"][:50] + "..." if len(solution["text"]) > 50 else solution["text"]
        elif solution.get("caption"):
            preview = solution["caption"][:50] + "..." if len(solution["caption"]) > 50 else solution["caption"]
        
        text += f"{i}. <b>{student_name}</b> ({time_str})\n"
        if preview:
            text += f"   {preview}\n"
        text += "\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="view_student_solutions"))
    
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")

# --- УЧЕНИК ОТПРАВЛЯЕТ РЕШЕНИЕ НАСТАВНИКУ ---
@dp.callback_query_handler(lambda c: c.data.startswith("send_solution_to_mentor:"))
async def send_solution_to_mentor(callback: types.CallbackQuery):
    """Ученик хочет отправить решение своему наставнику"""
    assignment_id = callback.data.split(":")[1]
    student_id = str(callback.from_user.id)
    
    # Загружаем данные
    assignments_data = load_assignments()
    users_data = load_users()["users"]
    
    # Проверяем задание
    assignment = assignments_data.get("assignments", {}).get(assignment_id)
    if not assignment:
        await callback.answer("Задание не найдено", show_alert=True)
        return
    
    # Проверяем, что ученик есть в базе
    if student_id not in users_data:
        await callback.answer("Вы не зарегистрированы", show_alert=True)
        return
    
    student = users_data[student_id]
    
    # Проверяем, есть ли у ученика наставник
    mentor_id = student.get("mentor")
    if not mentor_id:
        await callback.answer("У вас нет наставника для отправки решения", show_alert=True)
        return
    
    # Проверяем, что наставник существует
    if mentor_id not in users_data:
        await callback.answer("Ваш наставник не найден в системе", show_alert=True)
        return
    
    # Сохраняем данные в состоянии
    state = dp.current_state(user=callback.from_user.id, chat=callback.from_user.id)
    await state.update_data(
        assignment_id=assignment_id,
        mentor_id=mentor_id,
        student_id=student_id
    )
    
    await callback.message.answer(
        "📤 <b>Отправка решения наставнику</b>\n\n"
        "Отправьте ваше решение задания.\n\n"
        "Вы можете отправить:\n"
        "• Текст с ответом\n"
        "• Фотографию/скриншот решения\n"
        "• Документ (PDF, Word)\n"
        "• Голосовое объяснение\n\n"
        "<i>Ваше решение будет отправлено вашему личному наставнику</i>"
    )
    
    await AssignmentStates.waiting_for_solution.set()

# --- ПОЛУЧЕНИЕ РЕШЕНИЯ ОТ УЧЕНИКА ---
@dp.message_handler(state=AssignmentStates.waiting_for_solution, content_types=types.ContentTypes.ANY)
async def receive_solution_from_student(message: types.Message, state):
    """Получение решения от ученика и отправка его наставнику"""
    student_id = str(message.from_user.id)
    data = await state.get_data()
    
    assignment_id = data.get("assignment_id")
    mentor_id = data.get("mentor_id")
    
    if not assignment_id or not mentor_id:
        await message.answer("❌ Ошибка: данные не найдены")
        await state.finish()
        return
    
    # Загружаем данные
    users_data = load_users()["users"]
    assignments_data = load_assignments()
    
    # Получаем информацию о пользователях
    student = users_data.get(student_id)
    mentor = users_data.get(mentor_id)
    assignment = assignments_data.get("assignments", {}).get(assignment_id)
    
    if not student or not mentor or not assignment:
        await message.answer("❌ Ошибка: данные не найдены")
        await state.finish()
        return
    
    student_name = f"{student['name']} {student.get('surname','')}".strip()
    mentor_name = f"{mentor['name']} {mentor.get('surname','')}".strip()
    admin_name = assignment.get("admin_name", "Администратора")
    
    # Создаем ID для решения
    solution_id = f"solution_{student_id}_{assignment_id}_{datetime.now().strftime('%H%M%S')}"
    
    # Сохраняем решение
    solution_info = {
        "solution_id": solution_id,
        "assignment_id": assignment_id,
        "student_id": student_id,
        "student_name": student_name,
        "mentor_id": mentor_id,
        "mentor_name": mentor_name,
        "timestamp": str(datetime.now()),
        "content_type": message.content_type,
        "from_admin_assignment": True,
        "admin_name": admin_name
    }
    
    # Сохраняем в зависимости от типа контента
    if message.content_type == "text":
        solution_info["text"] = message.text
    elif message.content_type == "photo":
        solution_info["photo_id"] = message.photo[-1].file_id
        solution_info["caption"] = message.caption
    elif message.content_type == "document":
        solution_info["document_id"] = message.document.file_id
        solution_info["caption"] = message.caption
    elif message.content_type == "voice":
        solution_info["voice_id"] = message.voice.file_id
    
    # Получаем текст задания для наставника
    assignment_text = assignment.get("text") or assignment.get("caption") or f"Задание от {admin_name}"
    if len(assignment_text) > 200:
        assignment_text = assignment_text[:200] + "..."
    
    # Сохраняем решение
    assignments_data.setdefault("solutions", {})[solution_id] = solution_info
    
    if save_assignments(assignments_data):
        try:
            # Отправляем решение наставнику с кнопкой для ответа
            kb_mentor = InlineKeyboardMarkup(row_width=2)
            kb_mentor.add(
                InlineKeyboardButton("💬 Ответить ученику", 
                                   callback_data=f"start_dialogue:{student_id}:{assignment_id}"),
                InlineKeyboardButton("👀 Просмотреть задание", 
                                   callback_data=f"view_assignment:{assignment_id}")
            )
            
            if message.content_type == "text":
                await bot.send_message(
                    mentor_id,
                    f"📤 <b>РЕШЕНИЕ ОТ ВАШЕГО УЧЕНИКА</b>\n\n"
                    f"👤 <b>Ученик:</b> {student_name}\n"
                    f"📚 <b>Задание от {admin_name}:</b>\n{assignment_text}\n\n"
                    f"<b>Решение ученика:</b>\n{message.text}\n\n"
                    f"<i>Нажмите кнопку ниже, чтобы начать диалог с учеником</i>",
                    reply_markup=kb_mentor,
                    parse_mode="HTML"
                )
            elif message.content_type == "photo":
                await bot.send_photo(
                    mentor_id,
                    message.photo[-1].file_id,
                    caption=f"📤 <b>РЕШЕНИЕ ОТ ВАШЕГО УЧЕНИКА</b>\n\n"
                           f"👤 <b>Ученик:</b> {student_name}\n"
                           f"📚 <b>Задание от {admin_name}:</b>\n{assignment_text}\n\n"
                           f"<b>Решение ученика:</b>\n{message.caption or 'Фото решения'}\n\n"
                           f"<i>Нажмите кнопку ниже, чтобы начать диалог с учеником</i>",
                    reply_markup=kb_mentor,
                    parse_mode="HTML"
                )
            
            # Уведомляем ученика с кнопкой для диалога
            kb_student = InlineKeyboardMarkup()
            kb_student.add(
                InlineKeyboardButton("💬 Начать диалог с наставником", 
                                   callback_data=f"start_dialogue:{mentor_id}:{assignment_id}")
            )
            
            await message.answer(
                f"✅ Ваше решение отправлено наставнику <b>{mentor_name}</b>!\n\n"
                f"Ожидайте обратной связи. Вы можете начать диалог с наставником, нажав кнопку ниже.",
                reply_markup=kb_student
            )
            
            # Обновляем статистику задания
            if assignment_id in assignments_data.get("assignments", {}):
                if "solutions_count" not in assignments_data["assignments"][assignment_id]:
                    assignments_data["assignments"][assignment_id]["solutions_count"] = 0
                assignments_data["assignments"][assignment_id]["solutions_count"] += 1
                
                # Сохраняем, кто отправил решение
                if "solutions_sent" not in assignments_data["assignments"][assignment_id]:
                    assignments_data["assignments"][assignment_id]["solutions_sent"] = []
                assignments_data["assignments"][assignment_id]["solutions_sent"].append({
                    "student_id": student_id,
                    "student_name": student_name,
                    "mentor_id": mentor_id,
                    "timestamp": str(datetime.now())
                })
                
                save_assignments(assignments_data)
                
        except Exception as e:
            log_error(f"Ошибка отправки решения наставнику: {e}")
            await message.answer(f"❌ Ошибка отправки решения: {e}")
    else:
        await message.answer("❌ Ошибка сохранения решения")
    
    await state.finish()

# --- НАСТАВНИК НАЧИНАЕТ ДИАЛОГ С УЧЕНИКОМ ---
@dp.callback_query_handler(lambda c: c.data.startswith("start_dialogue:"))
async def start_dialogue_handler(callback: types.CallbackQuery):
    """Начало диалога между наставником и учеником"""
    parts = callback.data.split(":")
    partner_id = parts[1]
    assignment_id = parts[2] if len(parts) > 2 else None
    
    user_id = str(callback.from_user.id)
    users_data = load_users()["users"]
    
    # Проверяем, существуют ли оба пользователя
    if user_id not in users_data or partner_id not in users_data:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return
    
    user = users_data[user_id]
    partner = users_data[partner_id]
    
    # Проверяем отношения наставник-ученик
    is_mentor_to_student = (user.get("mentor") == partner_id) or any(
        u.get("mentor") == user_id for uid, u in users_data.items() if uid == partner_id
    )
    
    if not is_mentor_to_student:
        await callback.answer("Диалог возможен только между наставником и его учеником", show_alert=True)
        return
    
    # Определяем, кто наставник, а кто ученик
    if user.get("mentor") == partner_id:
        # Пользователь - ученик, партнер - наставник
        user_role = "ученик"
        partner_role = "наставник"
        user_state = DialogueStates.in_dialogue_with_mentor
        partner_state = DialogueStates.in_dialogue_with_student
    else:
        # Пользователь - наставник, партнер - ученик
        user_role = "наставник"
        partner_role = "ученик"
        user_state = DialogueStates.in_dialogue_with_student
        partner_state = DialogueStates.in_dialogue_with_mentor
    
    # Сохраняем состояние диалога
    save_dialogue_state(user_id, partner_id, assignment_id)
    
    # Уведомляем обоих пользователей
    user_name = f"{user['name']} {user.get('surname','')}".strip()
    partner_name = f"{partner['name']} {partner.get('surname','')}".strip()
    
    # Клавиатура для управления диалогом
    kb_dialogue = InlineKeyboardMarkup(row_width=1)
    kb_dialogue.add(
        InlineKeyboardButton("🚫 Завершить диалог", callback_data="end_dialogue")
    )
    
    # Сообщение для инициатора диалога
    await callback.message.edit_text(
        f"💬 <b>Диалог начат</b>\n\n"
        f"Вы начали диалог с {partner_role} <b>{partner_name}</b>.\n"
        f"Теперь все ваши сообщения будут пересылаться собеседнику.\n\n"
        f"<i>Чтобы завершить диалог, нажмите кнопку ниже</i>",
        reply_markup=kb_dialogue
    )
    
    # Сообщение для собеседника
    await bot.send_message(
        partner_id,
        f"💬 <b>Начат диалог</b>\n\n"
        f"Ваш {user_role} <b>{user_name}</b> начал диалог с вами.\n"
        f"Теперь все ваши сообщения будут пересылаться собеседнику.\n\n"
        f"<i>Чтобы завершить диалог, нажмите кнопку ниже</i>",
        reply_markup=kb_dialogue
    )
    
    # Устанавливаем состояния для обоих пользователей
    state_user = dp.current_state(user=int(user_id), chat=int(user_id))
    await state_user.set(user_state)
    await state_user.update_data(dialogue_with=partner_id, assignment_id=assignment_id)
    
    state_partner = dp.current_state(user=int(partner_id), chat=int(partner_id))
    await state_partner.set(partner_state)
    await state_partner.update_data(dialogue_with=user_id, assignment_id=assignment_id)
    
    await callback.answer("Диалог начат!")

# --- ОБРАБОТКА СООБЩЕНИЙ В ДИАЛОГЕ ---
@dp.message_handler(state=DialogueStates.in_dialogue_with_mentor, content_types=types.ContentTypes.ANY)
async def handle_student_dialogue_message(message: types.Message, state):
    """Обработка сообщений от ученика в диалоге с наставником"""
    user_id = str(message.from_user.id)
    data = await state.get_data()
    mentor_id = data.get("dialogue_with")
    
    if not mentor_id:
        await message.answer("❌ Ошибка: собеседник не найден")
        await state.finish()
        return
    
    # Получаем информацию о пользователях
    users_data = load_users()["users"]
    if user_id not in users_data or mentor_id not in users_data:
        await message.answer("❌ Ошибка: пользователь не найден")
        await state.finish()
        return
    
    student = users_data[user_id]
    mentor = users_data[mentor_id]
    
    student_name = f"{student['name']} {student.get('surname','')}".strip()
    mentor_name = f"{mentor['name']} {mentor.get('surname','')}".strip()
    
    # Клавиатура для получателя
    kb_receiver = InlineKeyboardMarkup()
    kb_receiver.add(InlineKeyboardButton("🚫 Завершить диалог", callback_data="end_dialogue"))
    
    # Клавиатура для отправителя
    kb_sender = InlineKeyboardMarkup()
    kb_sender.add(InlineKeyboardButton("🚫 Завершить диалог", callback_data="end_dialogue"))
    
    try:
        # Сохраняем сообщение в истории
        message_data = {
            "content_type": message.content_type,
            "text": message.text if message.content_type == "text" else None,
            "photo_id": message.photo[-1].file_id if message.content_type == "photo" else None,
            "document_id": message.document.file_id if message.content_type == "document" else None,
            "voice_id": message.voice.file_id if message.content_type == "voice" else None,
            "caption": message.caption
        }
        
        save_dialogue_message(user_id, mentor_id, message_data)
        
        # Отправляем сообщение наставнику
        if message.content_type == "text":
            await bot.send_message(
                mentor_id,
                f"💬 <b>Сообщение от ученика {student_name}</b>\n\n{message.text}",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        elif message.content_type == "photo":
            await bot.send_photo(
                mentor_id,
                message.photo[-1].file_id,
                caption=f"💬 <b>Сообщение от ученика {student_name}</b>\n\n{message.caption or ''}",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        elif message.content_type == "document":
            await bot.send_document(
                mentor_id,
                message.document.file_id,
                caption=f"💬 <b>Сообщение от ученика {student_name}</b>\n\n{message.caption or ''}",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        elif message.content_type == "voice":
            await bot.send_voice(
                mentor_id,
                message.voice.file_id,
                caption=f"💬 <b>Голосовое сообщение от ученика {student_name}</b>",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        
        # Подтверждение для ученика
        await message.answer(
            f"✅ Сообщение отправлено наставнику <b>{mentor_name}</b>",
            reply_markup=kb_sender
        )
        
    except Exception as e:
        log_error(f"Ошибка отправки сообщения в диалоге: {e}")
        await message.answer(f"❌ Ошибка отправки сообщения: {e}")

@dp.message_handler(state=DialogueStates.in_dialogue_with_student, content_types=types.ContentTypes.ANY)
async def handle_mentor_dialogue_message(message: types.Message, state):
    """Обработка сообщений от наставника в диалоге с учеником"""
    user_id = str(message.from_user.id)
    data = await state.get_data()
    student_id = data.get("dialogue_with")
    
    if not student_id:
        await message.answer("❌ Ошибка: собеседник не найден")
        await state.finish()
        return
    
    # Получаем информацию о пользователях
    users_data = load_users()["users"]
    if user_id not in users_data or student_id not in users_data:
        await message.answer("❌ Ошибка: пользователь не найден")
        await state.finish()
        return
    
    mentor = users_data[user_id]
    student = users_data[student_id]
    
    mentor_name = f"{mentor['name']} {mentor.get('surname','')}".strip()
    student_name = f"{student['name']} {student.get('surname','')}".strip()
    
    # Клавиатура для получателя
    kb_receiver = InlineKeyboardMarkup()
    kb_receiver.add(InlineKeyboardButton("🚫 Завершить диалог", callback_data="end_dialogue"))
    
    # Клавиатура для отправителя
    kb_sender = InlineKeyboardMarkup()
    kb_sender.add(InlineKeyboardButton("🚫 Завершить диалог", callback_data="end_dialogue"))
    
    try:
        # Сохраняем сообщение в истории
        message_data = {
            "content_type": message.content_type,
            "text": message.text if message.content_type == "text" else None,
            "photo_id": message.photo[-1].file_id if message.content_type == "photo" else None,
            "document_id": message.document.file_id if message.content_type == "document" else None,
            "voice_id": message.voice.file_id if message.content_type == "voice" else None,
            "caption": message.caption
        }
        
        save_dialogue_message(user_id, student_id, message_data)
        
        # Отправляем сообщение ученику
        if message.content_type == "text":
            await bot.send_message(
                student_id,
                f"💬 <b>Сообщение от наставника {mentor_name}</b>\n\n{message.text}",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        elif message.content_type == "photo":
            await bot.send_photo(
                student_id,
                message.photo[-1].file_id,
                caption=f"💬 <b>Сообщение от наставника {mentor_name}</b>\n\n{message.caption or ''}",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        elif message.content_type == "document":
            await bot.send_document(
                student_id,
                message.document.file_id,
                caption=f"💬 <b>Сообщение от наставника {mentor_name}</b>\n\n{message.caption or ''}",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        elif message.content_type == "voice":
            await bot.send_voice(
                student_id,
                message.voice.file_id,
                caption=f"💬 <b>Голосовое сообщение от наставника {mentor_name}</b>",
                reply_markup=kb_receiver,
                parse_mode="HTML"
            )
        
        # Подтверждение для наставника
        await message.answer(
            f"✅ Сообщение отправлено ученику <b>{student_name}</b>",
            reply_markup=kb_sender
        )
        
    except Exception as e:
        log_error(f"Ошибка отправки сообщения в диалоге: {e}")
        await message.answer(f"❌ Ошибка отправки сообщения: {e}")

# --- ЗАВЕРШЕНИЕ ДИАЛОГА ---
@dp.callback_query_handler(lambda c: c.data == "end_dialogue", state=[DialogueStates.in_dialogue_with_mentor, DialogueStates.in_dialogue_with_student])
async def end_dialogue_handler(callback: types.CallbackQuery, state):
    """Завершение диалога"""
    user_id = str(callback.from_user.id)
    
    # Завершаем диалог в базе данных
    partner_id = end_dialogue(user_id)
    
    if partner_id:
        # Уведомляем собеседника
        users_data = load_users()["users"]
        if user_id in users_data:
            user_name = f"{users_data[user_id]['name']} {users_data[user_id].get('surname','')}".strip()
            await bot.send_message(
                partner_id,
                f"🚫 <b>Диалог завершен</b>\n\n"
                f"Собеседник <b>{user_name}</b> завершил диалог.\n"
                f"Теперь вы можете начать новый диалог или отправить сообщение через меню."
            )
    
    # Сбрасываем состояние
    await state.finish()
    
    # Уведомляем пользователя
    await callback.message.edit_text(
        f"🚫 <b>Диалог завершен</b>\n\n"
        f"Вы завершили диалог с собеседником.\n"
        f"Теперь вы можете начать новый диалог или отправить сообщение через меню."
    )
    
    await callback.answer("Диалог завершен")

# --- ПРОСМОТР ЗАДАНИЯ ---
@dp.callback_query_handler(lambda c: c.data.startswith("view_assignment:"))
async def view_assignment_handler(callback: types.CallbackQuery):
    """Наставник просматривает задание от администратора"""
    assignment_id = callback.data.split(":")[1]
    
    assignments_data = load_assignments()
    assignment = assignments_data.get("assignments", {}).get(assignment_id)
    
    if not assignment:
        await callback.answer("Задание не найдено", show_alert=True)
        return
    
    admin_name = assignment.get("admin_name", "Администратора")
    levels = assignment.get("levels", [])
    
    text = f"📚 <b>ЗАДАНИЕ ОТ {admin_name.upper()}</b>\n\n"
    text += f"• ID: <code>{assignment_id}</code>\n"
    text += f"• Уровни: {', '.join(levels) if levels else 'Все ученики'}\n"
    text += f"• Время: {assignment.get('timestamp', '?')}\n"
    text += f"• Отправлено ученикам: {assignment.get('sent_count', 0)}\n"
    text += f"• Решений получено: {assignment.get('solutions_count', 0)}\n\n"
    
    if assignment.get("text"):
        text += f"<b>Текст задания:</b>\n{assignment['text']}\n"
    elif assignment.get("caption"):
        text += f"<b>Задание:</b>\n{assignment['caption']}\n"
    
    # Показываем, какие ученики отправили решения
    solutions_sent = assignment.get("solutions_sent", [])
    if solutions_sent:
        text += f"\n<b>Решения от ваших учеников:</b>\n"
        
        # Фильтруем только учеников этого наставника
        mentor_students = [s for s in solutions_sent if s.get("mentor_id") == str(callback.from_user.id)]
        
        if mentor_students:
            for i, solution in enumerate(mentor_students, 1):
                text += f"{i}. {solution.get('student_name', '?')} - {solution.get('timestamp', '?')}\n"
        else:
            text += "Ваши ученики еще не отправляли решения\n"
    
    await callback.message.answer(text, parse_mode="HTML")

# --- ПРОВЕРКА СТАТУСА ЗАДАНИЯ ---
@dp.callback_query_handler(lambda c: c.data.startswith("check_assignment:"))
async def check_assignment_status(callback: types.CallbackQuery):
    """Администратор проверяет статус выполнения задания"""
    assignment_id = callback.data.split(":")[1]
    
    if callback.from_user.id not in [OLGA_ID, YOUR_ADMIN_ID]:
        await callback.answer("Только для администраторов", show_alert=True)
        return
    
    assignments_data = load_assignments()
    users_data = load_users()["users"]
    
    assignment = assignments_data.get("assignments", {}).get(assignment_id)
    if not assignment:
        await callback.answer("Задание не найдено", show_alert=True)
        return
    
    # Получаем всех получателей
    recipients = assignments_data.get("assignment_recipients", {}).get(assignment_id, [])
    
    admin_name = assignment.get("admin_name", "Администратора")
    
    text = f"📊 <b>СТАТУС ВЫПОЛНЕНИЯ ЗАДАНИЯ ОТ {admin_name.upper()}</b>\n\n"
    text += f"• ID: <code>{assignment_id}</code>\n"
    
    levels = assignment.get("levels", [])
    if levels == ["ALL"]:
        text += f"• Все ученики\n"
    else:
        text += f"• Уровни: {', '.join(levels)}\n"
        
    text += f"• Всего учеников: {len(recipients)}\n"
    text += f"• Отправили решения: {assignment.get('solutions_count', 0)}\n\n"
    
    # Группируем по наставникам
    mentors_summary = {}
    
    for recipient in recipients:
        mentor_id = recipient.get("mentor_id")
        if mentor_id:
            if mentor_id not in mentors_summary:
                mentor_name = ""
                if mentor_id in users_data:
                    mentor = users_data[mentor_id]
                    mentor_name = f"{mentor['name']} {mentor.get('surname','')}".strip()
                mentors_summary[mentor_id] = {
                    "name": mentor_name,
                    "students": [],
                    "solutions": 0
                }
            mentors_summary[mentor_id]["students"].append(recipient["student_name"])
    
    # Считаем решения по наставникам
    solutions_sent = assignment.get("solutions_sent", [])
    for solution in solutions_sent:
        mentor_id = solution.get("mentor_id")
        if mentor_id in mentors_summary:
            mentors_summary[mentor_id]["solutions"] += 1
    
    text += "<b>По наставникам:</b>\n"
    for mentor_id, info in list(mentors_summary.items())[:15]:  # Ограничиваем вывод
        text += f"\n👤 <b>{info['name'] or 'Без наставника'}</b>\n"
        text += f"   Учеников: {len(info['students'])}\n"
        text += f"   Решений: {info['solutions']}\n"
        if info['solutions'] < len(info['students']):
            missing = len(info['students']) - info['solutions']
            text += f"   ❌ Ждут: {missing} учеников\n"
    
    if len(mentors_summary) > 15:
        text += f"\n... и еще {len(mentors_summary) - 15} наставников"
    
    await callback.message.answer(text, parse_mode="HTML")

# --- СТАРЫЙ ОБРАБОТЧИК РАССЫЛКИ (ОСТАВЛЕН ДЛЯ СОВМЕСТИМОСТИ) ---
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

# --- ФУНКЦИЯ ДЛЯ АВТОМАТИЧЕСКОЙ ОЧИСТКИ ---
async def scheduled_cleanup():
    """Периодическая очистка старых данных"""
    await asyncio.sleep(60)  # Ждем 1 минуту после запуска
    while True:
        try:
            # Запускаем очистку каждый день в 3:00
            now = datetime.now()
            target_time = now.replace(hour=3, minute=0, second=0, microsecond=0)
            if now > target_time:
                target_time = target_time.replace(day=now.day + 1)
            wait_seconds = (target_time - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            
            cleaned_count = cleanup_old_data(days_to_keep=7)
            log_info(f"🚮 Выполнена плановая очистка: удалено {cleaned_count} старых рассылок")
            
        except Exception as e:
            log_error(f"Ошибка в scheduled_cleanup: {e}")
            await asyncio.sleep(3600)  # Ждем час при ошибке

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
    
    # Проверяем файл заданий
    if os.path.exists(ASSIGNMENTS_FILE):
        assignments_data = load_assignments()
        assignments_count = len(assignments_data.get('assignments', {}))
        solutions_count = len(assignments_data.get('solutions', {}))
        conversations_count = len(assignments_data.get('conversations', {}))
        active_dialogues_count = len(assignments_data.get('active_dialogues', {}))
        print(f"📚 Загружено заданий: {assignments_count}")
        print(f"📝 Загружено решений: {solutions_count}")
        print(f"💬 Загружено диалогов: {conversations_count}")
        print(f"🔗 Активных диалогов: {active_dialogues_count}")
    else:
        print(f"📚 Файл заданий создан")
    
    # Загружаем историю рассылок
    if os.path.exists(BROADCAST_HISTORY_FILE):
        history = load_broadcast_history()
        total_broadcasts = history.get("stats", {}).get("total_broadcasts", 0)
        print(f"📨 Загружено рассылок в истории: {total_broadcasts}")
    
    # Выполняем начальную очистку
    cleaned = cleanup_old_data(days_to_keep=7)
    if cleaned > 0:
        print(f"🧹 Очищено старых данных: {cleaned}")
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(set_bot_commands())
    print("✅ Меню команд настроено")
    
    # Запускаем фоновые задачи
    loop.create_task(daily_report())
    print("✅ Задача ежедневного отчета запущена")
    
    loop.create_task(scheduled_cleanup())
    print("✅ Задача периодической очистки запущена")
    
    print("="*50)
    print("🚀 Бот запущен и готов к работе!")
    print("🛡️  Данные защищены от потери")
    print("🔧 Новые команды для админа: /check_data, /fix_data")
    print("📊 Добавлены функции смены наставника и уровня")
    print("📚 Добавлена система заданий: Ольга/Суперадмин → ученики → наставники")
    print("🔄 Защита от длинных сообщений добавлена")
    print("💬 ДОБАВЛЕНА СИСТЕМА НЕПРЕРЫВНЫХ ДИАЛОГОВ:")
    print("   • Автоматическая пересылка сообщений между учеником и наставником")
    print("   • Кнопка 'Завершить диалог' для обеих сторон")
    print("   • Сохранение истории всех сообщений")
    print("   • Поддержка всех типов контента (текст, фото, документы, голос)")
    print("="*50)
    print("🆕 ДОБАВЛЕНЫ НОВЫЕ ФУНКЦИИ:")
    print("   📊 Детальный отчет по рассылкам с группировкой ошибок")
    print("   📋 Просмотр полного списка неудачных отправок")
    print("   🧹 Автоматическая очистка старых данных")
    print("   📢 Улучшенные уведомления для администратора")
    print("   🔍 Статистика доставки с типами ошибок")
    print("="*50)
    
    executor.start_polling(dp, skip_updates=True)
