import os
import asyncio
import pytz
import datetime
import aioschedule
import nest_asyncio
import openai
import pickle
import tarfile
import tiktoken
import shutil
from background import keep_alive
from aiogram import Bot, Dispatcher, types
from parser import url_article_parser, get_parser_params
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types.message import ContentType 

bot = Bot(os.environ['bot_token'])
openai.api_key = os.environ['openai_token']
payments_token = os.environ['payments_token']
allowed_group_chats = [int(os.environ['allowed_group_1']), int(os.environ['allowed_group_2']), int(os.environ['allowed_group_3'])]
admin_chat_id = int(os.environ['admin_chat_id'])
is_test = int(os.environ['is_test'])
allowed_test_chats = [int(os.environ['allowed_test_1']), int(os.environ['allowed_test_2']), int(os.environ['allowed_test_3'])]

bot.set_current(bot)
nest_asyncio.apply()
storage = MemoryStorage()      
dp = Dispatcher(bot, storage=storage)
temperature = 0.1
users_file = 'users.pkl'
users_data = None
users = {}
payments_file = 'payments.pkl'
payments_data = None
payments = {}
bot_details = None

user_not_found = '❗️Пользователь не найден. Пожалуйста, запустите команду /start'
group_not_allowed = '❗️Запуск этого бота в групповом чате не разрешен'
bots_not_allowed = '❗️Данный бот не работает в личном чате с другими ботами'
user_banned = '❗️Данный пользователь заблокирован'
test_not_allowed = '❗️Это тестовый бот. Работа с ним не разрешена. Пожалуйста, используйте @Notifikat_assist_bot'
value_conversion = 'конвертация значения параметра'
attribute_type = 'тип параметра'
attribute_value = 'значение параметра'
empty_message = 'пустое сообщение'


class TelegramUser:
  def __init__(self, username, first_name, last_name, user_id, chat_id):
    self.username = username
    self.first_name = first_name
    self.last_name = last_name
    self.user_id = user_id
    self.chat_id = chat_id
    self.is_banned = False
    self.is_excluded = False
    if self.user_id in allowed_group_chats:
      self.is_paid = True
      today = datetime.datetime.now(pytz.timezone('Europe/Moscow')).date()
      self.paid_status_expiry = today + datetime.timedelta(days=999)
    else:
      self.is_paid = False
      self.paid_status_expiry = None
    self.daily_limit_max = 5
    self.daily_limit_used = 0
    self.conversation = []
    self.max_tokens = 4000 #1000
    self.truncate_limit = 3500 #700
    self.reg_date = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
    self.last_prompt = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
    self.total_prompts = 0
    self.total_tokens = 0
    self.total_revenue = 0
    self.is_moderated = False
      
  async def set_me_paid(self, is_paid, subscription_days=None):
    self.is_paid = is_paid
    if is_paid and subscription_days is not None:
      if self.paid_status_expiry:
        self.paid_status_expiry += datetime.timedelta(days=subscription_days)
      else:
        today = datetime.datetime.now(pytz.timezone('Europe/Moscow')).date()
        self.paid_status_expiry = today + datetime.timedelta(days=subscription_days)
      self.max_tokens = 4000
      self.truncate_limit = 3500
      self.daily_limit_max = -1
    else:
      self.is_paid = False
      self.paid_status_expiry = None
      self.max_tokens = 4000 #1000
      self.truncate_limit = 3500 #700
      self.daily_limit_max = 5
      self.daily_limit_used = 0

  async def reset_check(self):
    today = datetime.datetime.now(pytz.timezone('Europe/Moscow')).date()
        
    self.daily_limit_used = 0
    
    if self.is_paid and today > self.paid_status_expiry:
      self.is_paid = False
      self.paid_status_expiry = None
      self.max_tokens = 4000 #1000
      self.truncate_limit = 3500 #700
      self.daily_limit_max = 5
            
    await self.reset_conversation()

  async def reset_conversation(self):
    if self.is_moderated:
      self.conversation = []
      content = "Ты действуешь как эксперт и советник по сертификации и нотификации. Не оправдывай свои ответы. Не приводи информацию и не отвечай на вопросы, не связанные с сертификацией или нотификацией."
      self.conversation.append({"role": "system", "content": content})
      content = "Моя задача - помочь вам получить необходимую информацию, связанную с сертификацией и нотификацией для продуктов и услуг. Я готов предоставить информацию и консультации по процессу сертификации и нотификации. Я не буду приводить информацию, не связанную с сертификацией или нотификацией."
      self.conversation.append({"role": "assistant", "content": content})     
    else:
      self.conversation = []

  async def reset_stats(self):
    self.last_prompt = self.reg_date
    self.total_prompts = 0
    self.total_tokens = 0

  async def reset_revenue(self):
    self.total_revenue = 0
    
  async def ban_me(self, status):
    if status == 0:
      self.is_banned = False
    else:
      self.is_banned = True

  async def exclude_me(self, status):
    if status == 0:
      self.is_excluded = False
    else:
      self.is_excluded = True

  async def moderate_me(self, status):
    if status == 0:
      self.is_moderated = False
    else:
      self.is_moderated = True

  async def truncate_conversation(self):
    while True:
      conversation_len = await self.get_conversation_len()
      if conversation_len > self.truncate_limit:
        now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
        print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | UserID {self.user_id} | Convestation size is {conversation_len} tokens, thus it will be truncated\033[0m")
        self.conversation.pop(2) 
      else:
        break

  async def get_conversation_len(self) -> int:
    tiktoken.model.MODEL_TO_ENCODING["gpt-4"] = "cl100k_base"
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    num_tokens = 0
    for msg in self.conversation:
      # every message follows <im_start>{role/name}\n{content}<im_end>\n
      num_tokens += 5
      for key, value in msg.items():
        num_tokens += len(encoding.encode(value))
        if key == "name":  # if there's a name, the role is omitted
            num_tokens += 5  # role is always required and always 1 token
    num_tokens += 5  # every reply is primed with <im_start>assistant
    return num_tokens
     
async def check_authority(message, command):
  error_code = 0
  if message.chat.type != types.ChatType.PRIVATE:
    message.from_user.id = message.chat.id
  if message.from_user.id != admin_chat_id:
    text = f"❗️Ошибка пользователя {message.from_user.id} ({message.from_user.username}): нет доступа к команде {command}"
    error_code = 4
    await msg2admin(text)
  return error_code

async def error_handling(message, command, error_msg):
  text = f"❗️Ошибка пользователя {message.from_user.id} ({message.from_user.username}): команда {command}, {error_msg}"
  await msg2admin(text)
  
async def msg2admin(text):
  await bot.send_message(admin_chat_id, text, parse_mode="HTML")

async def get_prompt_len(prompt: dict) -> int:
  tiktoken.model.MODEL_TO_ENCODING["gpt-4"] = "cl100k_base"
  encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
  num_tokens = 0
  # every message follows <im_start>{role/name}\n{content}<im_end>\n
  num_tokens += 5
  for msg in prompt:
    for key, value in msg.items():
      num_tokens += len(encoding.encode(value))
      if key == "name":  # if there's a name, the role is omitted
        num_tokens += 5  # role is always required and always 1 token
  return num_tokens

async def update_users(user):
  global users
  users[user.user_id] = user

async def find_user(message, skip_check=False, is_start=False):
  if not skip_check:
    if message.chat.type != types.ChatType.PRIVATE:
      if message.chat.id not in allowed_group_chats:
        text = f'❗️Попытка запустить бота в групповом чате {message.chat.id} ({message.chat.title})'
        await msg2admin(text)
        await message.answer(group_not_allowed, parse_mode="HTML")
        return None, group_not_allowed
      else:
        message.from_user.id = message.chat.id
    elif message.from_user.is_bot and message.chat.bot.id != bot_details.id:
      await message.answer(bots_not_allowed, parse_mode="HTML")
      return None, bots_not_allowed

  if is_test == 1 and (message.from_user.id not in allowed_test_chats or message.from_user.id != admin_chat_id):
    await message.answer(test_not_allowed, parse_mode="HTML")
    return
    
  user = users.get(message.from_user.id)
  if not user:
    if not is_start:
      await message.answer(user_not_found, parse_mode="HTML")
    return None, user_not_found
  else:
    if user.is_banned and user.user_id != admin_chat_id:
      await message.answer(user_banned, parse_mode="HTML")
      return None, user_banned
    else:  
      return user, None
  
async def typing(chat_id):
  typing = types.ChatActions.TYPING
  await bot.send_chat_action(chat_id=chat_id, action=typing)

def insert_html_tag(text, open_tag, close_tag, offset, length):
  return text[:offset] + open_tag + text[offset:offset + length] + close_tag + text[offset + length:]
  
async def get_menu(level=1, current_user=None):
  if not current_user:
    return
    
  if level == 1:
    text = '👋 Привет! Я <b>Notifikat Bot</b>. Буду рад возможности проконсультировать по вопросам Внешнеэкономической деятельности и не только. А еще я могу стать Вашим другом или энциклопедией, которая всегда под рукой! 😉'
  if not current_user.is_paid:
    button2 = InlineKeyboardButton(text='Оформить подписку 💎 >>', callback_data='subscribe')
    text += f'\n📌 В бесплатной версии доступно <b>{current_user.daily_limit_max}</b> запросов в день. При оформлении платной подписки количество запросов в день <b>неограничено</b>.'
    #    text += f'\n📌 В бесплатной версии доступно <b>{current_user.daily_limit_max}</b> запросов в день с максимальной длиной запроса не более <b>{current_user.max_tokens}</b> токенов. При оформлении платной подписки количество запросов в день <b>неограничено</b>, а максимальная длина запроса составляет <b>4000</b> токенов.'
  else:
    button2 = InlineKeyboardButton(text='Продлить подписку 💎 >>', callback_data='subscribe')
    text += '\nУ вас оформлена платная подписка, поэтому количество запросов в день <b>неограничено</b>.'
#    text += f'\nУ вас оформлена платная подписка, поэтому количество запросов в день <b>неограничено</b>, а максимальная длина запроса составляет <b>{current_user.max_tokens}</b> токенов.'
  
  button1 = InlineKeyboardButton(text='Информация о подписке 🔎', callback_data='info')

  button3 = InlineKeyboardButton(text='Как со мной работать... 📚', callback_data='help')
  button4 = InlineKeyboardButton(text='Очистить историю переписки 🧹', callback_data='reset_me')
  keyboard = InlineKeyboardMarkup(row_width=1)
  keyboard.add(button1)
  keyboard.add(button2)
  keyboard.add(button3)
  keyboard.add(button4)
  return text, keyboard

async def file_read():
  global users_file
  global users_data
  global payments_file
  global payments_data
  global users
  global payments

  if os.path.exists(users_file) and os.path.getsize(users_file) > 0:
    with open(users_file, 'rb') as f:
      users_data = pickle.load(f)
    users = users_data["users"]

  # Initialize new fields in existing TelegramUser objects
  for user in users.values():
    if not hasattr(user, 'is_moderated'):
     user.is_moderated = True
     await update_users(user)
  await file_write(users)

  if os.path.exists(payments_file) and os.path.getsize(payments_file) > 0:
    with open(payments_file, 'rb') as f:
      payments_data = pickle.load(f)
    payments = payments_data["payments"]

async def create_tar_gz_archive(output_file_path, files):
  with tarfile.open(output_file_path, 'w:gz') as tar:
    for file in files:
      tar.add(file, arcname=file)

async def unarchive_gzip_tar(gzip_file, extract_path):
  with tarfile.open(gzip_file, 'r:gz') as tar:
    tar.extractall(extract_path)
          
async def file_write(users=None, payments=None):
  global users_file
  global users_data
  global payments_file
  global payments_data
  
  if users and os.path.exists(users_file):
    users_data = {"users": users}
    with open(users_file, 'wb') as f:
      pickle.dump(users_data, f)

  if payments and os.path.exists(payments_file):
    payments_data = {"payments": payments}
    with open(payments_file, 'wb') as f:
      pickle.dump(payments_data, f)

async def file_init():
  global users_file
  global users_data
  global payments_file
  global payments_data
    
  if os.path.exists(users_file) and os.path.getsize(users_file) == 0:
    users_data = {"users": {}}
    with open(users_file, 'wb') as f:
      pickle.dump(users_data, f)

  if os.path.exists(payments_file) and os.path.getsize(payments_file) == 0:
    payments_data = {"payments": {}}
    with open(payments_file, 'wb') as f:
      pickle.dump(payments_data, f)

async def file_delete(files_to_delete):
  for file in files_to_delete:
    try:
      os.remove(file)
    except OSError as e:
      now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
      print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | Error occurred while deleting the file '{file}': {e}\033[0m")
  
@dp.message_handler(commands=['backup_123'])
async def file_backup(message: types.Message=None, job=False):
  global users_file
  global payments_file
  files_to_archive = []

  if not job:
    command = 'backup_123'
    error_code = await check_authority(message, command)
    if error_code != 0:
      return
  
    current_user, error_msg = await find_user(message, skip_check=True)
    if not current_user:
      return
    
  now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))

  backup_file = os.path.join("backup", f"{users_file}")
  files_to_archive.append(backup_file)
  try:
    shutil.copyfile(users_file, backup_file)
  except:
    print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | An error occurred while creating the backup file {backup_file}\033[0m")
    pass

  backup_file = os.path.join("backup", f"{payments_file}")
  files_to_archive.append(backup_file)
  try:
    shutil.copyfile(payments_file, backup_file)
  except:
    print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | An error occurred while creating the backup file {backup_file}\033[0m")
    pass
  timestr = now.strftime('%Y%m%d')
  output_archive = os.path.join("backup", f"backup_{timestr}.tar.gz")
  try:
    await create_tar_gz_archive(output_archive, files_to_archive)
    print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | Backup file {output_archive.split('/')[1]} was created successfully\033[0m")
    await file_delete(files_to_archive)
  except:
    print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | An error occurred while creating the backup file {output_archive}\033[0m")
    pass

  if not job:
    text = f"❗️Админ {current_user.user_id} ({current_user.username}) запустил резервное копирование в ручном режиме"    
    await msg2admin(text)

@dp.message_handler(commands=['unpack_123'])
async def file_unpack(message: types.Message=None):

  command = 'unpack_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return
  
  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return
    
  archive_file = os.path.join("backup", "backup.tar.gz")
  extracted_files = ""
  await unarchive_gzip_tar(archive_file, extracted_files)
  
async def maintenance_job():
  aioschedule.every().day.at('20:59').do(file_backup, job=True)
  aioschedule.every().day.at('21:00').do(daily_reset)
   
async def schedule_jobs():
  aioschedule.clear()
  asyncio.create_task(maintenance_job())
        
async def run_scheduled_jobs():
  while True:
    await aioschedule.run_pending()
    await asyncio.sleep(1)

@dp.message_handler(commands=['start'])
async def start_command_handler(message: types.Message):

  current_user, error_msg = await find_user(message, skip_check=False, is_start=True)
  if current_user:
    if message.chat.type == types.ChatType.PRIVATE:
      current_user.first_name = message.from_user.first_name
      current_user.last_name = message.from_user.last_name
      current_user.username = message.from_user.username
      current_user.chat_id = message.chat.id
    else:
      current_user.first_name = None
      current_user.last_name = None
      current_user.username = message.chat.title
      current_user.chat_id = message.chat.id
  elif error_msg == user_not_found:
    if message.chat.type == types.ChatType.PRIVATE:
      current_user = TelegramUser(message.from_user.username, message.from_user.first_name, message.from_user.last_name, message.from_user.id, message.chat.id)
    else:
      current_user = TelegramUser(message.chat.title, None, None, message.chat.id, message.chat.id)
    text = f"🔔 Новый пользователь: {current_user.user_id} ({current_user.username})"
    text = await msg2admin(text)
  else:
    return
  await update_users(current_user)
  await file_write(users)

  text, keyboard = await get_menu(1, current_user)
  await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.message_handler(commands=['reset_123'])
async def reset_user(message: types.Message=None):

  command = 'reset_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return
    
  content = message.text.replace('/'+command, '').strip()
  try:
    message.from_user.id = int(content.split(':')[0])
    what = content.split(':')[1]
  except:
    await error_handling(message, command, value_conversion)
    return
    
  target_user, error_msg = await find_user(message, skip_check=True)
  if not target_user:
    return

  if what == 'chat':
    await target_user.reset_conversation()
  elif what == 'stats':
    await target_user.reset_stats()
  elif what == 'paid':
    await target_user.set_me_paid(False)
  elif what == 'revenue':
    await target_user.reset_revenue()
  elif  what == 'full':
    await target_user.reset_conversation()
    await target_user.reset_stats()
    await target_user.set_me_paid(False)
    await target_user.reset_revenue()
  else:
    await error_handling(message, command, attribute_type)
    return   

  await update_users(target_user)
  await file_write(users)
  text = f"❗️Админ {current_user.user_id} ({current_user.username}) сбросил настройку <b>{what}</b> пользователя {target_user.user_id} ({target_user.username})"
  await msg2admin(text)

@dp.message_handler(commands=['set_paid_123'])
async def set_paid(message: types.Message=None):

  command = 'set_paid_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return
    
  content = message.text.replace('/'+command, '').strip()
  try:
    message.from_user.id = int(content.split(':')[0])
    num_days = int(content.split(':')[1])
  except:
    await error_handling(message, command, value_conversion)
    return

  if num_days <= 0:
    await error_handling(message, command, attribute_value)
    return
    
  target_user, error_msg = await find_user(message, skip_check=True)
  if not target_user:
    return
    
  await target_user.set_me_paid(True, num_days)
  await update_users(target_user)
  await file_write(users)
  text = f"❗️Админ {current_user.user_id} ({current_user.username}) установил для пользователя {target_user.user_id} ({target_user.username}) подписку на {num_days} дней"
  await msg2admin(text)

  text = f"❗️Вам активирована подписка на {num_days} дней (до {target_user.paid_status_expiry.strftime('%d.%m.%Y')})"
  await bot.send_message(target_user.chat_id, text, parse_mode="HTML")

@dp.message_handler(commands=['delete_123'])
async def delete_user(message: types.Message=None):

  command = 'delete_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return

  content = message.text.replace('/'+command, '').strip()
  try:
    user_id = int(content)
  except:
    await error_handling(message, command, value_conversion)
    return

  message.from_user.id = user_id
  target_user, error_msg = await find_user(message, skip_check=True)
  if target_user: 
    del users[user_id]
    await file_write(users)
    text = f"❗️Админ {current_user.user_id} ({current_user.username}) удалил пользователя {target_user.user_id} ({target_user.username})"
    await msg2admin(text)

@dp.message_handler(commands=['reset_all_123'])
async def reset_all(message: types.Message=None):

  command = 'reset_all_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return
    
  for user_id in users.keys():
    message.from_user.id = user_id
    target_user = None
    target_user, error_msg = await find_user(message, skip_check=True)
    if target_user:
      await target_user.reset_conversation()
      await update_users(target_user)
  await file_write(users)
  text = f"❗️Админ {current_user.user_id} ({current_user.username}) выполнил ручной сброс истории переписки с ботом для всех пользователей"
  await msg2admin(text)

@dp.message_handler(commands=['list_123'])
async def list_users(message: types.Message=None):

  command = 'list_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return

  n = 0
  text = ''
  for user_id in users.keys():
    message.from_user.id = user_id
    target_user = None
    target_user, error_msg = await find_user(message, skip_check=True)
    if target_user:
      n += 1
      fullname = ''
      if target_user.first_name:
        fullname += target_user.first_name
      if target_user.last_name:
        fullname += target_user.last_name
      if fullname:
        text += f'{n}. {target_user.user_id} ({target_user.username}) - {fullname}\n'
      else:
        text += f'{n}. {target_user.user_id} ({target_user.username})\n'
  if text:
    await msg2admin(text)
    
@dp.message_handler(commands=['moderate_all_123'])
async def moderate_all(message: types.Message=None):

  command = 'moderate_all_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return

  content = message.text.replace('/'+command, '').strip()
  try:
    status = int(content)
  except:
    await error_handling(message, command, value_conversion)
    return
    
  for user_id in users.keys():
    message.from_user.id = user_id
    target_user = None
    target_user, error_msg = await find_user(message, skip_check=True)
    if target_user:
      await target_user.moderate_me(status)
      await update_users(target_user)
  await file_write(users)
  if status == 0:
    text = f"❗️Админ {current_user.user_id} ({current_user.username}) убрал ограничения разговора для всех пользователей"
  else:
    text = f"❗️Админ {current_user.user_id} ({current_user.username}) установил ограничения разговора для всех пользователей"    
  await msg2admin(text)
  
@dp.message_handler(commands=['status_123'])
async def change_status(message: types.Message=None):

  command = 'status_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  current_user, error_msg = await find_user(message, skip_check=True)
  if not current_user:
    return
    
  content = message.text.replace('/'+command, '').strip()
  try:
    message.from_user.id = int(content.split(':')[0])
    attribute = content.split(':')[1]
    status = int(content.split(':')[2])
  except:
    await error_handling(message, command, value_conversion)
    return

  if status not in [0, 1]:
    await error_handling(message, command, attribute_value)
    return
    
  target_user, error_msg = await find_user(message, skip_check=True)
  if not target_user:
    return

  if attribute == 'ban':
    await target_user.ban_me(status)
    if status == 0:
      text = f"❗️Админ {current_user.user_id} ({current_user.username}) снял бан с пользователя {target_user.user_id} ({target_user.username})"
    else:
     text = f"❗️Админ {current_user.user_id} ({current_user.username}) установил бан на пользователя {target_user.user_id} ({target_user.username})"      
  elif attribute == 'exclude':
    await target_user.exclude_me(status)
    if status == 0:
      text = f"❗️Админ {current_user.user_id} ({current_user.username}) добавил в рассылку пользователя {target_user.user_id} ({target_user.username})"
    else:
     text = f"❗️Админ {current_user.user_id} ({current_user.username}) удалил из рассылки пользователя {target_user.user_id} ({target_user.username})"
  elif attribute == 'moderate':
    await target_user.moderate_me(status)
    if status == 0:
      text = f"❗️Админ {current_user.user_id} ({current_user.username}) убрал ограничения разговора для пользователя {target_user.user_id} ({target_user.username})"
    else:
     text = f"❗️Админ {current_user.user_id} ({current_user.username}) установил ограничения разговора  для пользователя {target_user.user_id} ({target_user.username})"
  else:
    await error_handling(message, command, attribute_type)
    return    
  
  await update_users(target_user)
  await file_write(users)

  await msg2admin(text)

@dp.message_handler(commands=['info_123'])
async def get_info(message: types.Message=None):

  command = 'info_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  content = message.text.replace('/'+command, '').strip()
  try:
    message.from_user.id = int(content)
  except:
    await error_handling(message, command, value_conversion)
    return

  await check_my_info(message, True)

@dp.message_handler(commands=['send_message_123'])
async def send_message(message: types.Message=None):

  command = 'send_message_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  content = message.text
  accumulated_offset = 0
  for entity in message.entities:
    offset = entity.offset + accumulated_offset - 1
    length = entity.length

    if entity.type == "bold":
      content = insert_html_tag(content, "<b>", "</b>", offset, length)
      accumulated_offset += len("<b></b>")
    elif entity.type == "italic":
      content = insert_html_tag(content, "<i>", "</i>", offset, length)
      accumulated_offset += len("<i></i>")
  content = content.replace('/'+command, '').strip()

  if not content:
    await error_handling(message, command, empty_message)
  
  for user_id in users.keys():
    message.from_user.id = user_id
    target_user = None
    target_user, error_msg = await find_user(message, skip_check=True)
    if target_user and not target_user.is_excluded:
      await bot.send_message(target_user.chat_id, content, parse_mode="HTML")
    
@dp.message_handler(commands=['stats_123'])
async def get_stats(message: types.Message=None):

  command = 'stats_123'
  error_code = await check_authority(message, command)
  if error_code != 0:
    return

  count_total = 0
  count_paid = 0
  count_active = 0
  count_new = 0
  count_banned = 0
  count_excluded = 0
  total_revenue = 0
  mp_userid = 0
  mp_username = None
  mp_num = 0
  mp_days = 0
  mt_userid = 0
  mt_username = None
  mt_num = 0
  mt_days = 0
  
  lookback_days = 7
  lookback_date  =  datetime.datetime.now(pytz.timezone('Europe/Moscow')).date() - datetime.timedelta(days=lookback_days)

  for user in users.values():
    
    count_total += 1

    total_revenue += user.total_revenue
    
    if user.is_paid:
      count_paid += 1
      
    if user.is_banned:
      count_banned += 1

    if user.is_excluded:
      count_excluded += 1

    if user.reg_date.date() >= lookback_date:
      count_new += 1

    if user.last_prompt.date() >= lookback_date:
      count_active += 1

    if user.total_tokens > mt_num:
      mt_userid = user.user_id
      mt_username = user.username
      mt_num = user.total_tokens
      mt_reg_date =  user.reg_date
      mt_last_prompt = user.last_prompt

    if user.total_prompts > mp_num:
      mp_userid = user.user_id
      mp_username = user.username
      mp_num = user.total_prompts
      mp_reg_date =  user.reg_date
      mp_last_prompt = user.last_prompt

  text = '📊Статистика пользователей:'
  text = f'\n👉 Всего пользователей: <b>{count_total}</b>'
  text += f'\n👉 Новых пользователей за последние {lookback_days} дней: <b>{count_new}</b>'
  text += f'\n👉 Активных пользователей за последние {lookback_days} дней: <b>{count_active}</b>'
  text += f'\n👉 Пользователей с платной подпиской: <b>{count_paid}</b>'
  text += f'\n👉 Забаненных пользователей: <b>{count_banned}</b>'
  text += f'\n👉 Пользователей, исключенных из рассылки: <b>{count_excluded}</b>'

  formatted_num = "{:.2f}".format(total_revenue)
  text += f'\n\n👉 Общая выручка с продаж: <b>{formatted_num} руб.</b>'
  
  if mp_userid != 0:
    text += f'\n\n👉 Максимальное количество запросов у {mp_userid} ({mp_username}): <b>{mp_num}</b>'
    mp_delta = mp_last_prompt.date() - mp_reg_date.date()
    mp_days = mp_delta.days + 1
    if mp_days != 0:
      mp_avg = mp_num / mp_days
      text += f'\n👉 В среднем <b>{mp_avg}</b> запросов в день за <b>{mp_days}</b> дней активности'

  if mt_userid !=0:
    text += f'\n\n👉 Максимальное количество токенов у {mt_userid} ({mt_username}): <b>{mt_num}</b>'
    mt_delta = mt_last_prompt.date() - mt_reg_date.date()
    mt_days = mt_delta.days + 1
    if mt_days != 0:
      mt_avg = mt_num / mt_days
      text += f'\n👉 В среднем <b>{mt_avg}</b> токенов за <b>{mt_days}</b> дней активности'
      
  await msg2admin(text)

@dp.message_handler(commands=['daily_reset_123'])
async def daily_reset(message: types.Message=None):
  if not message:
    message = types.Message()
    message.from_user = types.User()
  for user_id in users.keys():
    message.from_user.id = user_id
    target_user = None
    target_user, error_msg = await find_user(message, skip_check=True)
    if target_user:
      await target_user.reset_check()
      await update_users(target_user)
  await file_write(users)
  now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
  print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | Job 'Daily Reset' is completed\033[0m")
  
@dp.callback_query_handler(lambda query: query.data == 'subscribe')
async def handle_subscribe_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await get_subscription(message, True)
  await bot.answer_callback_query(query.id)
  
@dp.message_handler(commands=['subscribe'])
async def get_subscription(message: types.Message, from_menu=False):
  current_user, error_msg = await find_user(message)
  if not current_user:
    return
    
  button1 = InlineKeyboardButton(text='Подписка на 30 дней - 100 руб.', callback_data='sub30')
  button2 = InlineKeyboardButton(text='Подписка на 90 дней - 250 руб.', callback_data='sub90')
  button3 = InlineKeyboardButton(text='Подписка на 180 дней - 450 руб.', callback_data='sub180')
  keyboard = InlineKeyboardMarkup().add(button1).add(button2).add(button3)
  
  if from_menu:
    button4 = InlineKeyboardButton(text='<< Назад', callback_data='back1')
    keyboard.add(button4)
    result = await get_menu(1, current_user)
    await bot.edit_message_text(result[0], message.chat.id, message.message_id, parse_mode="HTML", reply_markup=keyboard)
  else:
    text = 'Выберите подходящую продолжительность подписки:'
    await bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query_handler(lambda query: query.data == 'sub30')
async def handle_sub30_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await send_invoice(message, 30)
  await bot.answer_callback_query(query.id)

@dp.callback_query_handler(lambda query: query.data == 'sub90')
async def handle_sub190_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await send_invoice(message, 90)
  await bot.answer_callback_query(query.id)

@dp.callback_query_handler(lambda query: query.data == 'sub180')
async def handle_sub180_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await send_invoice(message, 180)
  await bot.answer_callback_query(query.id)
  
@dp.callback_query_handler(lambda query: query.data == 'back1')
async def handle_back1_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  current_user, error_msg = await find_user(message)
  if not current_user:
    return
    
  text, keyboard = await get_menu(1, current_user)
  await bot.edit_message_text(text, message.chat.id, message.message_id, parse_mode="HTML", reply_markup=keyboard)
  await bot.answer_callback_query(query.id)
  
async def send_invoice(message: types.Message, num_days: int):
  current_user, error_msg = await find_user(message)
  if not current_user:
    return
    
  if payments_token.split(':')[1] == 'TEST':
      await bot.send_message(current_user.chat_id, "❗️Платежная система работает в тестовом режиме!!! Использовать только данные тестовой карты!!!")

  if num_days == 30:
    price = types.LabeledPrice(label="Подписка на 30 дней", amount=100*100)
  elif num_days == 90:
    price = types.LabeledPrice(label="Подписка на 90 дней", amount=250*100)
  elif num_days == 180:
    price = types.LabeledPrice(label="Подписка на 180 дней", amount=450*100)

  await bot.send_invoice(current_user.chat_id,
                         title=f"Подписка на {num_days} дней",
                         description=f"Активация платной подписки Notifikat Bot на {num_days} дней",
                         provider_token=payments_token,
                         currency="rub",
                         photo_url="https://i.postimg.cc/NFR16mGX/2023-05-25-18-46-15.jpg",
                         photo_width=365,
                         photo_height=228,
                         photo_size=365,
                         is_flexible=False,
                         prices=[price],
                         start_parameter=f"{num_days}-days-subscription",
                         payload=f"subscription_{num_days}")

# pre-checkout  (must be answered in 10 seconds)
@dp.pre_checkout_query_handler(lambda query: True)
async def pre_checkout_query(pre_checkout_q: types.PreCheckoutQuery):
  await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

# successful payment
@dp.message_handler(content_types=ContentType.SUCCESSFUL_PAYMENT)
async def successful_payment(message: types.Message):
  num_days: int
  current_user, error_msg = await find_user(message)
  if not current_user:
    return  
    
  text = "💰Новый платеж:"
  payment_info = message.successful_payment.to_python()
  now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
  key = f"{current_user.user_id}_{now}"
  payments[key] = payment_info
  text += f"\n- from user: {current_user.user_id} ({current_user.username})\n- time:{now}"
  for k, v in payment_info.items():
    text += f"\n- {k}: {v}"
    if k == "invoice_payload":
      try:
        num_days = int(v.split('_')[1])
      except:
        pass
      await current_user.set_me_paid(True, num_days)
    elif k == "total_amount":
      try:
        revenue = v / 100
      except:
        pass
      current_user.total_revenue += revenue
  await update_users(current_user)
  await file_write(users=users, payments=payments)
  await msg2admin(text)
  text = f"❗️Платеж завершен. Подписка активирована на {num_days} дней (до {current_user.paid_status_expiry.strftime('%d.%m.%Y')})"
  await bot.send_message(current_user.chat_id, text, parse_mode="HTML")

@dp.callback_query_handler(lambda query: query.data == 'help')
async def handle_help_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await print_help(message)
  await bot.answer_callback_query(query.id)
  
@dp.message_handler(commands=['help'])
async def print_help(message: types.Message):
  current_user, error_msg = await find_user(message)
  if not current_user:
    return
  text = 'Примеры кейсов для использования бота можно посмотреть тут: https://www.youtube.com/watch?v=42KVu8pmZHo\nРаздел наполняется по мере выхода практикумов.'
  await message.answer(text, parse_mode="HTML")
  
@dp.callback_query_handler(lambda query: query.data == 'info')
async def handle_info_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await check_my_info(message)
  await bot.answer_callback_query(query.id)
  
@dp.message_handler(commands=['info'])
async def check_my_info(message: types.Message, admin=False):
  current_user, error_msg = await find_user(message)
  if not current_user:
    return

  text = f'👉 ID: <b>{current_user.user_id}</b>'
  if not current_user.is_paid:
    text += '\n👉 Подписка: <b>не активна</b>'
    text += f'\n👉 Дневной лимит количества запросов: <b>{current_user.daily_limit_max}</b>'
    utc_time = aioschedule.jobs[1].next_run
    moscow_time = utc_time.astimezone(pytz.timezone('Europe/Moscow'))
    time_str = moscow_time.strftime('%d.%m.%Y %H:%M:%S')
    reqs_available = current_user.daily_limit_max - current_user.daily_limit_used
    text += f'\n👉 Доступно запросов до {time_str} MSK: <b>{reqs_available}</b>'
#    text += f'\n👉 Максимальная длина одного запроса: <b>{current_user.max_tokens}</b> токенов'
  else:
    text += '\n👉 Подписка: <b>активна</b>'
    time_str = current_user.paid_status_expiry.strftime('%d.%m.%Y')
    text += f'\n👉 Дата окончания подписки: <b>{time_str}</b>'
    text += '\n👉 Дневной лимит количества запросов: <b>неограничен</b>'
#    text += f'\n👉 Максимальная длина одного запроса: <b>{current_user.max_tokens}</b> токенов'
  if not admin:
    await message.answer(text, parse_mode="HTML")
  else:
    if current_user.is_banned:
      text += '\n👉 Бан: <b>да</b>'
    else:
      text += '\n👉 Бан: <b>нет</b>'
    if current_user.is_excluded:
      text += '\n👉 Рассылка: <b>нет</b>'
    else:
      text += '\n👉 Рассылка: <b>да</b>'
    if current_user.is_moderated:
      text += '\n👉 Модерация: <b>да</b>'
    else:
      text += '\n👉 Модерация: <b>нет</b>'
      
    text += f"\n👉 Дата регистрации: <b>{current_user.reg_date.strftime('%d.%m.%Y')}</b>"
    text += f"\n👉 Последний запрос: <b>{current_user.last_prompt.strftime('%d.%m.%Y')}</b>"
    formatted_num = "{:.2f}".format(current_user.total_revenue)
    text += f'\n👉 Выручка с продаж: <b>{formatted_num}</b> руб.'
    await msg2admin(text)

@dp.message_handler(lambda message: not message.text.startswith('/'))
async def default_message_handler(message: types.Message):
  article_text = []
  parser_option = 1
  orig_url = False
  post_prompt = ' Не оправдывай свои ответы. Если запрос не связан с системным контекстом, то отвечай "Запрос не относится к моей области знаний"'
  
  current_user, error_msg = await find_user(message)
  if not current_user:
    return
  elif bot_details.username in message.text:
    content = message.text.replace(f'@{bot_details.username}', '').strip()
  elif message.chat.type == types.ChatType.PRIVATE:
    content = message.text
  else:
    return
    
  if not current_user.is_paid and current_user.daily_limit_used >= current_user.daily_limit_max:
    text = f'❗️Достигнут дневной лимит бесплатных запросов ({current_user.daily_limit_used}).'
    utc_time = aioschedule.jobs[1].next_run
    moscow_time = utc_time.astimezone(pytz.timezone('Europe/Moscow'))
    time_str = moscow_time.strftime('%d.%m.%Y %H:%M:%S') 
    text += f'\nСчетчик запросов будет сброшен {time_str} MSK. Также вы можете оформить платную подписку (команда /subscribe), чтобы получить <b>неограниченное</b> количество запросов в день.'
#    text += f'\nСчетчик запросов будет сброшен {time_str} MSK. Также вы можете оформить платную подписку (команда /subscribe), чтобы получить <b>неограниченное</b> количество запросов в день и максимальную длину запроса <b>4000</b> токенов.'    
    await message.answer(text, parse_mode="HTML")
    return

  await typing(message.chat.id)

  if current_user.is_paid:
    if message.entities is not None:
      for entity in message.entities:
        if entity.type == "url":
          url = message.text[entity.offset: entity.offset + entity.length]
          if url.startswith('http'):
            params = await get_parser_params(message.text)
            parser_option = params['parser_option']
            orig_url = params['orig_url']
            article_text = await url_article_parser(url=url, parser_option=parser_option, orig_url=orig_url)
            content = content.replace(f'parser_option{parser_option}', '').strip()
            content = content.replace('orig_url', '').strip()
            if article_text != '':
              content = content.replace(url, '')
              content += "\n" + article_text

  if current_user.is_moderated:
    content += post_prompt
  
  prompt_len = await get_prompt_len(prompt=[{"role": "user", "content": content}])
  if prompt_len > current_user.max_tokens:
    text = f'❗️Длина запроса {prompt_len} токенов > максимальной длины запроса {current_user.max_tokens}'
#    if not current_user.is_paid:
#      text += '\n Максимальная длина запроса для платных подписчиков 4000 токенов'
    await message.answer(text, parse_mode="HTML")
    return
         
  current_user.conversation.append({"role": "user", "content": content})
  await current_user.truncate_conversation()

  text = 'Ожидайте, формирую ответ...\nПросьба пока не отправлять новые запросы.'
  LastMessage = await message.reply(text)

  max_tokens_chat = current_user.max_tokens - await current_user.get_conversation_len()
  try:
    completion = openai.ChatCompletion.create(
      model="gpt-3.5-turbo",
      messages=current_user.conversation,
      max_tokens=max_tokens_chat,
      temperature=temperature,
      )
  except (
      openai.error.APIError,
      openai.error.APIConnectionError,
      openai.error.AuthenticationError,
      openai.error.InvalidAPIType,
      openai.error.InvalidRequestError,
      openai.error.OpenAIError,
      openai.error.PermissionError,
      openai.error.PermissionError,
      openai.error.RateLimitError,
      openai.error.ServiceUnavailableError,
      openai.error.SignatureVerificationError,
      openai.error.Timeout,
      openai.error.TryAgain,
  ) as e:
    print(f"\033[38;2;255;0;0mUserID {current_user.user_id} | OpenAI API error: {e}\033[0m")
    pass
    
  gpt_finish_reason = completion.choices[0].finish_reason
  if gpt_finish_reason.lower() == 'stop':
    gpt_response = completion.choices[0].message.content
    current_user.conversation.append({"role": "assistant", "content": gpt_response})
    if not current_user.is_paid:
      current_user.daily_limit_used += 1
      gpt_response += f'\n({current_user.daily_limit_used}/{current_user.daily_limit_max})'
    await bot.edit_message_text(chat_id=current_user.chat_id, message_id=LastMessage.message_id, text=gpt_response)
    current_user.total_prompts += 1
    response_len = await get_prompt_len(prompt=[{"role": "assistant", "content": gpt_response}])
    current_user.total_tokens += prompt_len + response_len
    current_user.last_prompt = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
    await update_users(current_user) 
    await file_write(users)
  else: 
    text = f'❗️Ошибка OpenAI API: {gpt_finish_reason}'
    await message.answer(text, parse_mode="HTML")

@dp.callback_query_handler(lambda query: query.data == 'reset_me')
async def handle_reset_callback(query: types.CallbackQuery):
  message = query.message
  message.from_user.id = query.from_user.id
  await reset_me(message)
  await bot.answer_callback_query(query.id)
  
@dp.message_handler(commands=['reset_me'])
async def reset_me(message: types.Message):
  current_user, error_msg = await find_user(message)
  if not current_user:
    return
  await current_user.reset_conversation()
  await update_users(current_user)
  await file_write(users)
  text = '❗️История переписки с ботом очищена'
  await message.answer(text, parse_mode="HTML")
  
async def main():
  global bot_details
  bot_details = await bot.get_me()
  await file_init()
  await file_read()
  await schedule_jobs()
  job_loop = asyncio.get_event_loop()
  job_loop.create_task(run_scheduled_jobs())
  await dp.start_polling(timeout=30)
  
if __name__ == '__main__':
  keep_alive()
  main_loop = asyncio.get_event_loop() 
  main_loop.run_until_complete(main())