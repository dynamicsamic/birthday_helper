"""
Add birthday process.

The process of adding a new birthday starts with a user command to the Bot.
It takes 4 messages to accomplish receiving bithday data from the user.
1. You're adding a new birthday. Enter month.
2. Now enter day in month.
3. Now enter the name.
4. Great! Everything's fine.
After we received all the information from user, we should update the file on
the Yandex Disk cloud storage.

Before adding new birthday, we need to download the lates file, because users
may add new birthday directly via Yandex Disk, bypassing the Bot.

Download the most recent file from Yandex Disk.
Append new birthday to the downloaded file.
Update local CSV birthday file, for it to contain the most recent version.
Backup the original file on Yandex Disk under new file name.
Upload the updated local excel file to Yandex Disk under the original name.

Things to think about:
- consider making appends via Pandas.
"""

"""
Fetch birthday process.

User-Bot interaction.
1. User: sends get_birthday command to Bot.
2. Bot:
    - replies "type in a name or a date".
    - checks when the cloud file was last modified.
      Compares last modified date with BirthdayMessageCache.
      If cloud file was updated later than BirthdayMessageCache
      - downloads the source file from Yandex Disk 
      and stores it in a temporary dataframe.
3. User: sends message either with name or a date.
4. Bot:
    - parses the user message;
    - conducts a search through the temp dataframe;
    - returns the search result in a message;
      the result may be none, one or many birthdays;
      Bot should display the the `repeat search` button;
    - updates the local BirthdayMessageCache.

Things to think about:
- when performing actual search consider replying to user something
  like "Performing search, please wait..." if the search takes much time
  in order to provide better UX.
"""


# cloud_storage_manager = YandexDiskStorageManager("token")
# file_processor = ExcelProcessor("configs")
# query_engine = BirthdayQueryEngine("configs")
# sync_manager = BirthdayDataSynchronizationManager(
#     cloud_storage_manager, file_processor, query_engine
# )
# sync_manager.sync_state()
# sync_manager.get_birthday_data_cache()
# sync_manager.get_telegram_birthday_message_cache()
"""
BirthdayDataStateManager:
  cloud_storage_manager = YandexDiskStorageManager
  file_parser = ExcelProcessor
  query_engine = BirthdayQueryEngine

  def refresh_state():
    self.cloud_storate_manager.download_file()
    birthday_data = self.file_parser.parse(self.file_path)
    
    self.update_birthday_data_cache(birthday_data)
    today_birthdays = self.query_engine(birthday_data).find_for_today()
    upcoming_birthdays = self.query_engine(birthday_data).find_upcoming()
    
    self.update_telegram_birthday_message_cache(today_birthdays, upcoming_birthdays)
    
  def is_state_synced():
    souce_file_last_modified = self.cloud_storage_manager.check_file_modified(self.source_file)
    return source_file_last_modified > self.birthday_data_cache.last_modified
      
  def get_birthda_data_cache(self):
    return self.birthday_data_cache

  def get_telegram_birthday_message_cache(self):
    return self.telegram_birthday_message_cache

    

FileProcessor:
  parser
  query_engine

data = ExcelFileProcessor(filename).check_unique("day", "month", "name").check_non_empty_columns("").
bithday_data_cache = BirthdayDataCache(data)
query = QueryEngine(data).find()
today_birthdays = QueryEngine(data).find_for_today()
upcoming_birthdays = QueryEngine(data).find_upcoming()

message_cache = BirthdayMessageCache(today=today_birthdays, upcoming=upcoming_birthdays)

...
message_cache = get_message_cache()
cloud_file_last_modified = cloud_storage_manager.get_file_modified('file_name')
if cloud_file_last_modified > message_cache.last_modified:
  - download cloud excel file -> cloud_storage_manager
  - parse excel file -> excel_parser
  - create new BirthdayDataCache -?
  - query updated birthday info -> query_engine
  - create new BirthdayTelegramMessageCache

  <need to update cache>
"""


"""
middleware:
  cloud_manager.get_file_info()
  if need_to_update:
    cache_manager.update()
    data['birthday_data'] = cache_manager.get_cache()

    
handler:
  query_manager.get_recent_birthdays(birthday_data) 

"""


"""
Project structure

src/
├── cloud/
│   ├── __init__.py
│   ├── storage_manager.py   # Cloud storage upload/download functions
│   └── exceptions.py        # Custom exceptions related to cloud operations
├── excel/
│   ├── __init__.py
│   ├── file_processor.py    # Excel file processing logic
│   └── file_query.py        # Querying Excel files
├── data_processing/
│   ├── __init__.py
│   └── pandas_query.py 
├── telegram/
│   ├── __init__.py
│   ├── bot.py               # Main bot initialization and configuration
│   ├── handlers/            # Bot request/command handlers
│   │   ├── __init__.py
│   │   └── general_handlers.py  # e.g., start, help, etc.
|   |── keyboards/
│   │   ├── __init__.py
│   │   └── ...
|   |── replies/
│   │   ├── __init__.py
│   │   └── ...
│   ├── middlewares/         # Custom middlewares for the bot
│   │   ├── __init__.py
│   │   └── repository_middleware.py  # Middleware to inject repositories into handlers
│   └── utils/               # Utility functions for bot-specific operations
│       ├── __init__.py
│       └── helper_functions.py
└── main.py 


src/
    bot/
        configs/
        handlers/
        keyboards/
        replies/
        bot.py
        filters.py
        middlewares.py
        states.py

    cloud/
        storage_
    
    service_providers/
        yandex_disk_provider.py
        excel_parser_provider.py
        csv_parser_provider.py  
"""


class YandexDiskProvider:
    pass


"""
Find birthday process
QueryIn
  Middleware performs cloud sync check
    it takes sync manager from context
    it take data cache from context
    it feeds data cache to sync_managers.check_cache_fresh()
    result: we esnured the cache is up to date
    middleware awaits the handler
  Handler:
    takes global cache
    creates AsyncQueryEngine instance
    runs query
    formats the result
    sends message back
"""

"""
Add birthday process
QueryIn
  Middleware performs cloud sync check
    it takes sync manager from context
    it take data cache from context
    it feeds data cache to sync_managers.check_cache_fresh()
    result: we esnured the cache is up to date
    middleware awaits the handler
  Handler:

  

@roter.message(command, flags={'sync_check': True})
async def handler  

"""

"""
data_manager.get_birthday(name: str)
  check cache is fresh
  return query_engine(self.cache).find(lambda row: name in row['name'])

data_manager.get_upcoming_birthdays()
  check cache is fresh
  get_today_date()
  get_today_plus_three_days()
  return query_engine(self.cache).find(lambda row: (row['month'] == today.month and row['day'] == today.day) or (row['month']==today_plus_three.month and row['day'] > today.day and row['day'] <= today_plus_three.day )

data_manager.add_birthday(birthday_data)
  check cache is fresh
  file_processor(self.local_file_path).add_row(birthday_data)
  self.sync_manager.update_file(local_file_path)

data_manager.delete_birthday(name: str)
  check cache is fresh
  file_processor(self.local_file_path).delete_row(lambda row: name in row['name'])
  self.sync_manager.update_file(local_file_path)

data_manager.update_birthday(name: str, update_data: dict)
  check cache is fresh
  self.file_processor(self.local_file_path).update_row(lambda row: name in row['name'], update_data)
  self.sync_manager.update_file(local_file_path)

check_cache()
  if not self._is_cache_fresh():
    self.sync_manager.download_file(file_path)
    df = await self.file_processor(file_path).read()
    self.cache.update_data(df)
"""
