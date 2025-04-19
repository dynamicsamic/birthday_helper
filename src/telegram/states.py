from aiogram.fsm.state import State, StatesGroup


class NewBirthday(StatesGroup):
    month = State()
    day = State()
    name = State()
    complete = State()
