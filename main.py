import os
import asyncio
import pytz
import datetime
import aioschedule
import nest_asyncio
import openai
import pickle
import tiktoken
from background import keep_alive
from aiogram import Bot, Dispatcher, types
from parser import url_article_parser, get_parser_params
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

bot = Bot(os.environ['bot_token'])
bot.set_current(bot)
openai.api_key = os.environ['openai_token']

nest_asyncio.apply()
storage = MemoryStorage()

class GPTSystem(StatesGroup):
  question1 = State()
  
class AgendaAdd(StatesGroup):
  question1 = State()

class AgendaDelete(StatesGroup):
  question1 = State()
  
dp = Dispatcher(bot, storage=storage)
chat_id = 0
poll_message = 0
pinned_message_id = 0
total_answers = 0
PollingJob = False
JobActive = False
bot_details = None
conversations = {}
max_tokens = 4000
truncate_limit = 3500
temperature = 1
agenda = []
filename = 'saved_data.pkl'
filedata = None
    
@dp.message_handler(lambda message: not message.text.startswith('/'))
async def default_message_handler(message: types.Message, role: str="user"):
  global conversations
  global max_tokens
  global temperature
  global bot_details
  article_text = []
  url_yes = False
  parser_option = 1
  orig_url = False

  if message.chat.type == types.ChatType.GROUP and role != "system":
    if bot_details.username in message.text:
      content = message.text.replace(f'@{bot_details.username}', '').strip()
    else:
      return
  else:
    content = message.text

  await typing(message.chat.id)
  
  if message.entities is not None:
    for entity in message.entities:
      if entity.type == "url":
        url = message.text[entity.offset: entity.offset + entity.length]
        if url.startswith('http'):
          params = await get_parser_params(message.text)
          parser_option = params['parser_option']
          orig_url = params['orig_url']
          article_text = await url_article_parser(url=url, parser_option=parser_option, orig_url=orig_url)
          content = content.replace(f'parser_option{parser_option}', '')
          content = content.replace('orig_url', '').strip()
          if article_text != '':
            content = content.replace(url, '')
            content += "\n" + article_text
  
  if message.reply_to_message:
    if message.reply_to_message.entities is not None:
      for entity in message.reply_to_message.entities:
        if entity.type == "url":
          url = message.reply_to_message.text[entity.offset: entity.offset + entity.length]
          if url.startswith('http'):
            params = await get_parser_params(message.text)
            parser_option = params['parser_option']
            orig_url = params['orig_url']
            article_text = await url_article_parser(url=url, parser_option=parser_option, orig_url=orig_url)
            content = content.replace(f'parser_option{parser_option}', '')
            content = content.replace('orig_url', '').strip()
            if article_text != '':
              url_yes = True
              content += "\n" + article_text
              break
    
    if not url_yes:
      if message.reply_to_message.text:
        reply_to_text = message.reply_to_message.text
        if bot_details.username in reply_to_text:
          reply_to_text = reply_to_text.replace(f'@{bot_details.username}', '').strip()
        if reply_to_text:
          content += "\n" + reply_to_text
      elif message.reply_to_message.caption:
        content += "\n" + message.reply_to_message.caption

  prompt_len = await get_prompt_len(prompt=[{"role": role, "content": content}])
  if prompt_len > max_tokens:
    text = f'❗️Длина запроса {prompt_len} токенов > максимальной длины разговора {max_tokens}'
    await message.answer(text, parse_mode="HTML")
    return
         
  if message.chat.id not in conversations:
    conversations[message.chat.id] = []
  conversations[message.chat.id].append({"role": role, "content": content})
  await truncate_conversation(message.chat.id)
  
  max_tokens_chat = max_tokens - await get_conversation_len(message.chat.id)
  try:
    completion = openai.ChatCompletion.create(
      model="gpt-3.5-turbo",
      messages=conversations[message.chat.id],
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
    print(f"\033[38;2;255;0;0mOpenAI API error: {e}\033[0m")
    pass  
    
  gpt_finish_reason = completion.choices[0].finish_reason
  if gpt_finish_reason.lower() == 'stop':
    gpt_response = completion.choices[0].message.content
    await message.reply(gpt_response)
    conversations[message.chat.id].append({"role": "assistant", "content": gpt_response})
    await file_write()
  else: 
    text = f'❗️Ошибка OpenAI API: {gpt_finish_reason}'
    await message.answer(text, parse_mode="HTML")

async def truncate_conversation(chat_id: int):
  global conversations
  global truncate_limit
  while True:
    conversation_len = await get_conversation_len(chat_id)
    if conversation_len > truncate_limit and chat_id in conversations:
      now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
      print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | Convestation size is {conversation_len} tokens, thus it will be truncated\033[0m")
      conversations[chat_id].pop(0) 
    else:
      break

async def get_conversation_len(chat_id: int) -> int:
  global conversations
  tiktoken.model.MODEL_TO_ENCODING["gpt-4"] = "cl100k_base"
  encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
  num_tokens = 0
  for msg in conversations[chat_id]:
    # every message follows <im_start>{role/name}\n{content}<im_end>\n
    num_tokens += 5
    for key, value in msg.items():
      num_tokens += len(encoding.encode(value))
      if key == "name":  # if there's a name, the role is omitted
          num_tokens += 5  # role is always required and always 1 token
  num_tokens += 5  # every reply is primed with <im_start>assistant
  return num_tokens

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

@dp.message_handler(commands=['gpt_system'])
async def gpt_system_message(message: types.Message):
  await message.answer("Введите системное сообщение...")
  await GPTSystem.question1.set()

@dp.message_handler(state=GPTSystem.question1)
async def gpt_system_question1_handler(message: types.Message, state: FSMContext):
  await state.finish()
  await default_message_handler(message, "system")

async def typing(chat_id):
  typing = types.ChatActions.TYPING
  await bot.send_chat_action(chat_id=chat_id, action=typing)
  
@dp.message_handler(commands=['gpt_clear'])
async def gpt_clear(message: types.Message, silent_mode=False):
  global conversations
  if message.chat.id in conversations:
    del conversations[message.chat.id]
    await file_write()

  if not silent_mode:
    text = '❗️История переписки с ChatGPT очищена 💪'
    await message.answer(text, parse_mode="HTML")
    
@dp.message_handler(commands=['gpt_clear_all'])
async def gpt_clear_all():
  global conversations
  conversations = {}
  await file_write()
  now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
  print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | Job 'gpt_clear_all' is completed\033[0m")

@dp.message_handler(commands=['gpt_show'])
async def gpt_show(message: types.Message):
  global conversations
  if message.chat.id in conversations:
    message = await bot.send_message(message.chat.id, f"Chat_id: {message.chat.id}")
    LastMessage_id = message.message_id
    LastMessage_text = message.text + "\n"
    for msg in conversations[message.chat.id]:
      LastMessage_text += f"- {msg['content']}\n"
      await bot.edit_message_text(chat_id=message.chat.id, message_id=LastMessage_id, text=LastMessage_text)
  else:
    text = '❗️История переписки с ChatGPT пустая'
    await message.answer(text, parse_mode="HTML")

@dp.message_handler(commands=['gpt_show_all'])
async def gpt_show_all(message: types.Message):
  global conversations
  if conversations:
    for chat_id in conversations:
      message = await bot.send_message(message.chat.id, f"Chat_id: {chat_id}")
      LastMessage_id = message.message_id
      LastMessage_text = message.text + "\n"
      for msg in conversations[chat_id]:
        LastMessage_text += f"- {msg['content']}\n"
        await bot.edit_message_text(chat_id=message.chat.id, message_id=LastMessage_id, text=LastMessage_text)
  else:
    text = '❗️История переписки с ChatGPT пустая'
    await message.answer(text, parse_mode="HTML")

@dp.message_handler(commands=['agenda_add'])
async def agenda_add(message: types.Message):
  await message.answer("С новой строки введите один или несколько пунктов для добавления в повестку заседания Клуба 101 или введите 'Нет' для прерывания команды...")
  await AgendaAdd.question1.set()

@dp.message_handler(state=AgendaAdd.question1)
async def agenda_add_question1_handler(message: types.Message, state: FSMContext):
  await state.finish()
  global agenda
  if message.text.lower() != "нет":
    lines = message.text.splitlines()
    for line in lines:
      agenda.append(line)
    await file_write()
  else:
    text = '❗️Команда сброшена 🤬'
    await message.answer(text, parse_mode="HTML")
  await agenda_show(message)

@dp.message_handler(commands=['agenda_delete'])
async def agenda_delete(message: types.Message):
  global agenda
  if agenda != []:
    await agenda_show(message)
    await message.answer("Введите через запятую номера позиций, которые нужно удалить из повестки заседания Клуба 101 или введите 'Нет' для прерывания команды...")
    await AgendaDelete.question1.set()
  else:
    text = '❗️Повестка заседания Клуба 101 пустая - удалять нечего 😢'
    await message.answer(text, parse_mode="HTML")

@dp.message_handler(state=AgendaDelete.question1)
async def agenda_delete_question1_handler(message: types.Message, state: FSMContext):
  global agenda
  temp_agenda = []
  num_array = []
  await state.finish()
  if message.text.lower() != "нет":
    lines_to_delete = message.text.split(',')
    for line_num in lines_to_delete:
      try:
        num = int(line_num.strip())
        if num > 0 and num <= len(agenda):
          num_array.append(num)
      except ValueError:
        #ignore non-integer line numbers
        pass
    if num_array != []:
      for i, line in enumerate(agenda):
        if i+1 not in num_array:
          temp_agenda.append(line)
      agenda = temp_agenda
      await file_write()
  else:
    text = '❗️Команда сброшена 🤬'
    await message.answer(text, parse_mode="HTML")
  await agenda_show(message)

@dp.message_handler(commands=['agenda_show'])
async def agenda_show(message: types.Message):
  global agenda
  mes = []
  if agenda != []:
    text = '❗️Повестка заседания Клуба 101:'
    mes.append(text)
    for i, item in enumerate(agenda):
      item_id = i + 1
      text = f'👉 {item_id}. {item}'
      mes.append(text)
    await bot.send_message(message.chat.id,'\n'.join(mes), parse_mode="HTML")
  else:
    text = '❗️Повестка заседания Клуба 101 пока пустая 😢'
    await bot.send_message(message.chat.id, text, parse_mode="HTML")
  
@dp.message_handler(commands=['agenda_clear'])
async def agenda_clear(message: types.Message):
  global agenda
  agenda = []
  await file_write()
  text = '❗️Повестка заседания Клуба 101 очищена 💪'
  await message.answer(text, parse_mode="HTML")
  
@dp.message_handler(commands=['send_poll_now'])
async def send_poll(message: types.Message):
  global chat_id
  global poll_message
  global total_answers
  total_answers = 0
  if chat_id == 0 and message.chat.id != 0:
    chat_id = message.chat.id
  await unpin_poll_results()
  moscow_tz = pytz.timezone('Europe/Moscow')
  now = datetime.datetime.now(moscow_tz)
  days_until_friday = (4 - now.weekday()) % 7
  fri_date = now.date() + datetime.timedelta(days=days_until_friday)
  option1 = 'Пятница (' + fri_date.strftime('%d.%m.%Y') + ')'
  days_until_saturday = (5 - now.weekday()) % 7
  sat_date = now.date() + datetime.timedelta(days=days_until_saturday)
  option2 = 'Суббота (' + sat_date.strftime('%d.%m.%Y') + ')'
  option3 = 'Пропущу в этот раз 😢'
  option4 = 'Зайду на внеклассное 😎'
  options = [option1, option2, option3, option4]
  poll_question = 'Когда состоится следующее заседание Клуба 101? 😤'
  waiting_time = 3600 #Время голосования в секундах
  poll_message = await bot.send_poll(chat_id, poll_question, options=options, is_anonymous=False, allows_multiple_answers=True)
  text = '❗️Голосование длится максимум 1 час или до получения 3 голосов.\nМожно выбрать несколько вариантов ответа.\nДля подтверждения ввода обязательно нажать <b>VOTE</b>.'
  await message.answer(text, parse_mode="HTML")
  await asyncio.sleep(waiting_time)
  if not bot.get_poll(chat_id=chat_id, message_id=poll_message.message_id).is_closed:
    await bot.stop_poll(chat_id, poll_message.message_id)

@dp.poll_handler(lambda closed_poll: closed_poll.is_closed is True)
async def poll_results(closed_poll: types.Poll):
  global chat_id
  global pinned_message_id
  message = types.Message(chat=types.Chat(id=chat_id))
  max_option = closed_poll.options[0].text
  max_votes = closed_poll.options[0].voter_count
  max_id = 0
  option4_votes = 0
  for i, option in enumerate(closed_poll.options):
    if option.voter_count > max_votes:
      max_option = option.text
      max_votes = option.voter_count
      max_id = i+1
    if i==3:
      option4_votes = option.voter_count
  if max_votes == 1 or max_votes == 0:
    text = '❗️Голосование завершено. Решение не принято - слишком мало голосов 🤬'
    message = await bot.send_message(chat_id, text, parse_mode="HTML")
  else:
    if max_id == 3:
      text = '❗️Голосование завершено. Заседание на этой неделе не состоится - большинство не может участвовать 👎'
      if(option4_votes!=0):
        text1 = f'🤘 Однако {option4_votes} человека хотели бы зайти на внеклассное чтение.'
        text = text + '\n' + text
      await bot.send_message(chat_id, text, parse_mode="HTML")
    else:
      text = f'❗️Голосование завершено. Заседание на этой неделе состоится в <b>{max_option}</b> 👍'
      if(option4_votes!=0):
        text1 = f'🤘 Также {option4_votes} человека хотели бы зайти на внеклассное чтение.'
        text = text + '\n' + text1
      message = await bot.send_message(chat_id, text, parse_mode="HTML")
      pinned_message_id = message.message_id
      await bot.pin_chat_message(chat_id=chat_id, message_id=pinned_message_id)
      await file_write()
      await agenda_show(message)
 
@dp.poll_answer_handler(lambda poll_answer: True)
async def poll_answer(poll_answer: types.PollAnswer):
  global chat_id
  global poll_message
  global total_answers
  total_answers += 1
  if total_answers == 3:
    await bot.stop_poll(chat_id, poll_message.message_id)
    total_answers = 0

async def unpin_poll_results():
  global chat_id
  global pinned_message_id
  if pinned_message_id != 0:
    await bot.unpin_chat_message(chat_id=chat_id, message_id=pinned_message_id)
  now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
  print(f"\033[38;2;128;0;128m{now.strftime('%d.%m.%Y %H:%M:%S')} | Job 'unpin_poll_results' is completed\033[0m")
  
async def polling_job(message: types.Message, silent_mode=False):
  aioschedule.every().thursday.at('09:00').do(send_poll, message=message)
  global chat_id
  
  if not silent_mode:
    text = '❗️Опрос по расписанию запущен 💪'
    moscow_tz = pytz.timezone('Europe/Moscow')
    utc_time = aioschedule.jobs[0].next_run
    moscow_time = utc_time.astimezone(moscow_tz)
    time_str = moscow_time.strftime('%Y-%m-%d %H:%M:%S')
    text += f'\nСледующий опрос состоится <b>{time_str} MSK</b>'
    await bot.send_message(chat_id, text, parse_mode="HTML")

async def maintenance_job():
  aioschedule.every().day.at('01:00').do(gpt_clear_all)
  aioschedule.every().monday.at('01:01').do(unpin_poll_results)

@dp.message_handler(commands=['schedule_start'])
async def schedule_start(message: types.Message):
  global PollingJob
  global JobActive
  global chat_id
  
  if message.chat.type == types.ChatType.GROUP:
    chat_id = message.chat.id
    PollingJob = True
    JobActive = True
    await file_write()
    await schedule_jobs(message)
  else:
    text = '❗️Запуск плановых задач возможен только из группового чата'
    await bot.send_message(message.chat.id, text, parse_mode="HTML")

@dp.message_handler(commands=['schedule_check'])
async def schedule_check(message: types.Message):
  global PollingJob
  if PollingJob == True:
    text = '❗️Опрос по расписанию активен'
    moscow_tz = pytz.timezone('Europe/Moscow')
    utc_time = aioschedule.jobs[0].next_run  #Опрос всегда первый в списке для упрощения
    moscow_time = utc_time.astimezone(moscow_tz)
    time_str = moscow_time.strftime('%Y-%m-%d %H:%M:%S')
    text += f'\nСледующий опрос состоится <b>{time_str} MSK</b>'
    await message.answer(text, parse_mode="HTML")
  else:
    text = '❗️Опрос по расписанию неактивен'
    await message.answer(text, parse_mode="HTML")
    
@dp.message_handler(commands=['schedule_stop'])
async def schedule_stop(message: types.Message):
  global PollingJob
  PollingJob = False
  await file_write()
  await schedule_jobs(message)    
  text = '❗️Опрос по расписанию остановлен 💪'
  await message.answer(text, parse_mode="HTML")

async def schedule_jobs(message: types.Message, silent_mode=False):
  global PollingJob
  global JobActive
  aioschedule.clear()
  if PollingJob:
    asyncio.create_task(polling_job(message, silent_mode))
  if JobActive:
    asyncio.create_task(maintenance_job())
    
async def file_read():
  global filedata
  global filename
  global JobActive
  global PollingJob
  global pinned_message_id
  global chat_id
  global agenda
  global conversations

  if os.path.exists(filename) and os.path.getsize(filename) > 0:
    with open(filename, 'rb') as f:
      filedata = pickle.load(f)
    JobActive = filedata["JobActive"]
    PollingJob = filedata["PollingJob"]
    pinned_message_id = filedata["pinned_message_id"]
    chat_id = filedata["chat_id"]
    agenda = filedata["agenda"]
    conversations = filedata["conversations"]

async def file_write():
  global filedata
  global filename
  global JobActive
  global PollingJob
  global pinned_message_id
  global chat_id
  global agenda
  global conversations
  
  if os.path.exists(filename):
    filedata = {"JobActive": JobActive,
                "PollingJob": PollingJob,
                "pinned_message_id": pinned_message_id,
                "chat_id": chat_id,
                "agenda": agenda,
                "conversations": conversations}
    with open(filename, 'wb') as f:
      pickle.dump(filedata, f)

async def file_init():
  global filedata
  global filename
  
  if os.path.exists(filename) and os.path.getsize(filename) == 0:
    filedata = {"JobActive": False,
                "PollingJob": False,
                "pinned_message_id": 0,
                "chat_id": 0,
                "agenda": [],
                "conversations": {}}
    with open(filename, 'wb') as f:
      pickle.dump(filedata, f)

async def run_scheduled_jobs():
  while True:
    await aioschedule.run_pending()
    await asyncio.sleep(1)
  
async def main():
  global bot_details
  bot_details = await bot.get_me()
  await file_init()
  await file_read()
  if JobActive:
    message = types.Message(chat=types.Chat(id=chat_id))
    await schedule_jobs(message, silent_mode=True)
  job_loop = asyncio.get_event_loop()
  job_loop.create_task(run_scheduled_jobs())
  await dp.start_polling(allowed_updates=False, timeout=30)

if __name__ == '__main__':
  keep_alive()
  main_loop = asyncio.get_event_loop() 
  main_loop.run_until_complete(main())