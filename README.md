# Инструкция по работе со ETL

## Разворачиваем локально

Убедиться, что на установлен python3.8

Рекомендую использовать [pyenv](https://github.com/pyenv/pyenv), т.к. на нем построено все в облаке. Да и вообще, полезная штука

Для того, чтобы установить python3.8.10 с помощью pyenv, 

```shell
pyenv install 3.8.10
```

После этого в директории с проектом автоматически будет использоваться нужная версия питона. Она указана в файле .python-version

```shell
cd travel_etl
python --version
```

Должно выдать 3.8.10

Для сборки проекта используется poetry. Рекомендую установить его в какой-нибудь внешний питон

```shell
pip install poetry
```

Возможно придется добавить команду poetry в PATH - через .bash_profile, .zprofile, .zsh_profile (чтобы команда была доступна при перезагрузке терминала)

Дальше все просто. Переходим в проект, и устанавливаем окружение

```shell
cd travel_etl
poetry install
```

Вуаля, окружение готово

Запуск любых команд в окружении можно делать с помощью poetry run

Например, запуск тестового файла (main.py) в этом окружении

Например, запуск тестового файла (main.py) в этом окружении

```shell
poetry run python main.py
```

## Локальные тесты

К сожалению, версии airflow и ydb немного конфликтуют, поэтому приходится использовать старые версии этих пакетов. Это приводит к тому, что открыть UI Airflow на MacOS без плясок с бубном не получится. Поэтому если дело не касается переиначивания структуры проекта, тесты скриптов удобнее всего производить через YQL-интерфейс YDB. Либо настроить утилиту YC и тогда можно запускать YQL-скрипты из консоли. Остальная вся часть по сути автоматизирована.

То есть для создания новых сущностей в DWH YDB/S3 достаточно создать YQL-скрипт

Окончательное тестированеи можно произвести на проде, подменив директорию DWH (далее)

Документация [YQL](https://ydb.tech/en/docs/yql/reference/)


## Структура DWH

Код организован так, что пути в репозиториях соответствуют путям в YDB/S3

Все пути к таблицам в DWH (YDB/S3) организованы следующим образом:

```
/{PARENT_DIR}/{LAYER}/{TABLE}
```

**PARENT_DIR** - корневая директория проекта (проектов может быть много и у них будут разные корневые директории)

На проде в данном проекте - parser

Но если в конфигурациях подменить ее на test, например, то новая версия проекта раскатится и не затронет основные таблицы, что обеспечивает какую-никакую изоляцию

**LAYER** - слой данных, определяется из репозитория

Сейчас есть слои

raw - сырые данные
det - пропаршенные данные по разным агрегаторам
prod - унифицированные данные, которые уже используются на клиенте


**TABLE** - таблица, определяется из репозитория

Например, название таблицы с унифицированным офферами /parser/prod/offers

## Структура репозитория

Теперь то, как эти пути конструиются на основе путей в репозитории

Код, собирающий таблицу

```
/{PARENT_DIR}/{LAYER}/{TABLE}
```

Лежит по пути в репозитории

```
travel_etl/{LAYER}/{TABLE}
```

То есть код, сробирающий таблицу с унифицированным офферами /parser/prod/offers лежит в репозитории по пути travel_etl/prod/offers


## Добавление новой таблицы YDB

Допустим, мы хотим создать таблицу /parser/prod/offers_new

Тогда в репозитории мы должны создать директориюю travel_etl/prod/offers_new

Структура этой директории

```
travel_etl/prod/offers_new
                        |---__init__.py
                        |---loader.py
                        |---query1.sql
                        |---query2.sql
```

Верхнеуровнео - loader.py - код определения таблицы и ее загрузчк. query*.sql - SQL - скрипты формирования таблицы

query.sql - это параметризованный SQL-скрипт. Параметры нужны для того, чтобы их было легко менять (названия таблиц, константы и тд). Они подставляются в скрипт позднее, путем питоновского форматирования (через оператор %)

Нотация параметризации (опять-таки питоновская)

```
%(parameter)s
```

**Обязательный параметр - target, в него подставится название таблицы (из структуры репозитория)**

Пример скрипта

```sql
REPLACE INTO `%(target)s`
SELECT some_columne
FROM  `%(source)s`
```

В нем два параметра - target (обязательный), source - необязательный - имя таблицы-источника



