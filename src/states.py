from aiogram.dispatcher.filters.state import State, StatesGroup


class AddBirthday(StatesGroup):
    month = State()
    day = State()
    name = State()
    complete = State()
