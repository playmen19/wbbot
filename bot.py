import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from bs4 import BeautifulSoup as bs
import requests
import json
from datetime import datetime, date, time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import Column, ForeignKey, Integer, ARRAY, String, Text, Date, DateTime, Table, MetaData, create_engine, delete, select, update, and_, or_, not_

bot = Bot(token="6768477969:AAGpm1NUHsgJYcOqARiOlwu5aMkb5Dnq2Ow")
dp = Dispatcher(storage=MemoryStorage())

class Data(CallbackData, prefix="my"):
    callArt: int
    callBool: bool

class Art(StatesGroup):
    artW = State()
    sub = State()

@dp.message(StateFilter(None), Command("start"))
async def start(message: types.Message):
    keyboard = [
        [types.InlineKeyboardButton(text = "Информация о товаре", callback_data = 'art')],
        [types.InlineKeyboardButton(text = "Получить информацию из БД", callback_data = 'db')],
        [types.InlineKeyboardButton(text = "Остановить уведомления", callback_data = 'stop')]
    ]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    await message.answer("Hello!", reply_markup = keyboard)
    
@dp.message(Art.artW)
async def wbRequest(message: types.Message, state: FSMContext):
    await state.clear()
    res = requests.get('https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm=' + message.text)
    json_data = json.loads(res.text)
    for key, value in json_data.items():
        if key == 'data':
            for secondKey, secondValue in value.items():
                if secondKey == 'products':
                    if len(secondValue) == 0:
                        empty = 1
                        break
                    for thirdKey, thirdValue in secondValue[0].items():
                        if thirdKey == 'name':
                            name = str(thirdValue)
                        elif thirdKey == 'reviewRating':
                            rating = str(thirdValue)
                        elif thirdKey == 'sizes':
                            for fourthKey, fourthValue in thirdValue[0].items():
                                if fourthKey == 'price':
                                    for fifthKey, fifthValue in fourthValue.items():
                                        if fifthKey == 'basic':
                                            basePrice = str(fifthValue)[:-2]
                                        elif fifthKey == 'product':
                                            disPrice = str(fifthValue)[:-2]
                                elif fourthKey == 'stocks':
                                    for i in fourthValue:
                                        for fifthKey, fifthValue in i.items():
                                            if fifthKey == 'qty':
                                                if 'count' in locals():
                                                    count += int(fifthValue)
                                                else:
                                                    count = int(fifthValue)
    if 'empty' in locals():
        await message.answer('Артикул неверный!')
    else:
        try:
            with engine.connect() as connection:
                s = request.insert().values(
                    userId = message.chat.id,
                    time = datetime.today().replace(microsecond=0),
                    vendorCode = int(message.text)
                )
                connection.execute(s)
        except:
            await message.answer('db connect fail!')
            
        keyboard = [
            [types.InlineKeyboardButton(text = "Подписаться", callback_data = Data(callArt = message.text, callBool = 'True').pack())]
        ]
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=keyboard)
        
        await message.answer(
            name + '\nКолличество ' + 
            str(count) + '\nЦена ' + 
            basePrice + '\nЦена со скидкой ' + 
            disPrice + '\nРейтинг ' + 
            rating + '\nАртикул ' + 
            message.text, reply_markup = keyboard
        )

@dp.callback_query(F.data == "art")
async def callback_message(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите артикул")
    await state.set_state(Art.artW)
        
@dp.callback_query(F.data == "db")
async def callback_message(callback: types.CallbackQuery):     
    try:
        with engine.connect() as connection:
            r = connection.execute(request.select().order_by(request.c.id.desc()).limit(5)).fetchall()
            r.reverse()
            for row in r:
                await callback.message.answer(
                    'Id пользователя ' + str(row[1]) + 
                    '\nВремя ' + str(row[2]) +
                    '\nАртикул ' + str(row[3])
                )
                await asyncio.sleep(0.25)
    except Exception as exc:
        print(exc)
        await callback.message.answer('db connect fail!')
        
@dp.callback_query(F.data == "stop")
async def callback_message(callback: types.CallbackQuery):
    try:
        with engine.connect() as connection:
            connection.execute(users.delete().where(users.c.userId == int(callback.message.chat.id)))
            await callback.message.answer("Все уведомления аннулированы")
    except:
        await callback.message.answer('db connect fail!')
            
@dp.callback_query(Data.filter(F.callBool == True))
async def callback_message(callback: types.CallbackQuery, callback_data: Data):
#     await callback.message.answer(str(callback_data.callArt))
#     await callback.message.answer(str(callback.message.chat.id))
    try:
        with engine.connect() as connection:
            if connection.execute(users.select().where(and_(users.c.vendorCode.op('&&')([int(callback_data.callArt)]), users.c.userId == int(callback.message.chat.id)))).first() is not None:  
                await callback.message.answer('Вы уже подписаны на этот товар')
            elif connection.execute(users.select().where(users.c.userId == int(callback.message.chat.id))).first() is None:
                s = users.insert().values(
                    userId = int(callback.message.chat.id),
                    vendorCode = [int(callback_data.callArt)]
                )
                connection.execute(s)
                await callback.message.answer('Вы подписаны!')
            else:
                vendorCode = connection.execute(users.select().where(users.c.userId == int(callback.message.chat.id))).first()[1]
                vendorCode.append(int(callback_data.callArt))
                s = users.update().values(
                    userId = int(callback.message.chat.id),
                    vendorCode = vendorCode
                )
                connection.execute(s)
                await callback.message.answer('Вы подписаны!')
    except:
        await callback.message.answer('db connect fail!')

async def inform():
    try:
        with engine.connect() as connection:
            r = connection.execute(users.select()).fetchall()
            for row in r:
                for vendorCode in row[1]:
                    res = requests.get('https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&spp=30&nm=' + str(vendorCode))
                    json_data = json.loads(res.text)
                    for key, value in json_data.items():
                        if key == 'data':
                            for secondKey, secondValue in value.items():
                                if secondKey == 'products':
                                    if len(secondValue) == 0:
                                        empty = 1
                                        break
                                    for thirdKey, thirdValue in secondValue[0].items():
                                        if thirdKey == 'name':
                                            name = str(thirdValue)
                                        elif thirdKey == 'reviewRating':
                                            rating = str(thirdValue)
                                        elif thirdKey == 'sizes':
                                            for fourthKey, fourthValue in thirdValue[0].items():
                                                if fourthKey == 'price':
                                                    for fifthKey, fifthValue in fourthValue.items():
                                                        if fifthKey == 'basic':
                                                            basePrice = str(fifthValue)[:-2]
                                                        elif fifthKey == 'product':
                                                            disPrice = str(fifthValue)[:-2]
                                                elif fourthKey == 'stocks':
                                                    for i in fourthValue:
                                                        for fifthKey, fifthValue in i.items():
                                                            if fifthKey == 'qty':
                                                                if 'count' in locals():
                                                                    count += int(fifthValue)
                                                                else:
                                                                    count = int(fifthValue)
                    if 'empty' in locals():
                        await bot.send_message(row[0], 'Артикул неверный!')
                    else:
                        await bot.send_message(
                            row[0],
                            name + '\nКолличество ' + 
                            str(count) + '\nЦена ' + 
                            basePrice + '\nЦена со скидкой ' + 
                            disPrice + '\nРейтинг ' + 
                            rating + '\nАртикул ' + 
                            str(vendorCode)
                        )
    except Exception as exc:
        print(exc)
        
def set_scheduled_jobs(scheduler):
    scheduler.add_job(inform, "interval", minutes=5)
        
async def main():
#     bot = Bot(token="6768477969:AAGpm1NUHsgJYcOqARiOlwu5aMkb5Dnq2Ow")
#     dp = Dispatcher(storage=MemoryStorage())
    scheduler = AsyncIOScheduler()
    set_scheduled_jobs(scheduler)
    scheduler.start()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    metadata = MetaData()

    request = Table('request', metadata,
        Column('id', Integer, nullable=False, unique=True, primary_key=True, autoincrement=True),
        Column('userId', Integer, nullable=False, comment='Id пользователя'),
        Column('time', DateTime, nullable=False, comment='Время'),
        Column('vendorCode', Integer, nullable=False, comment='Артикул')
    )

    users = Table('users', metadata,
        Column('userId', Integer, nullable=False, unique=True, primary_key=True, autoincrement=False, comment='id пользователя'),
        Column('vendorCode', ARRAY(Integer), comment='Артикул')
    )

#     engine = create_engine('postgresql://postgres:123@localhost:5433/postgres')
    engine = create_engine('postgresql://postgres:32313@172.17.0.1:5432/postgres')
    metadata.create_all(engine)
    # scheduler = AsyncIOScheduler()
    # set_scheduled_jobs(scheduler)
    # scheduler.start()
    # await bot.delete_webhook(drop_pending_updates=True)
    # await dp.start_polling(bot)
    asyncio.run(main())