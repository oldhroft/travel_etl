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

Далее табличку нужно добавить в файлик database.py - который выполняется в процессе деплоя
и создать таск с заполнением таблицы в workflows/main.py

В данном случае допусти, что таблица источник у нас /parser/det/pivot

```python
@task.external_python(task_id="etl_prod_offers_new", python=PATH_TO_PYTHON)
def etl_det_pivot(days_offer, directory):

    # Импортируем участвующие в процессе таблицы
    import travel_etl.det.teztour as teztour
    import travel_etl.prod.offers as offers_new

    # Инициализируем эти таблицы в директории
    det_teztour = teztour.DetTeztour(directory)
    det_travelata = offers_new.ProdOffersNew(directory)

    # Параметры, которые подставляются в скрипт
    cfg = {
        "source_teztour": det_teztour,
        "days_offer": days_offer,
    }

    # Загружаем таблицу
    det_pivot.load_table(**cfg)
```

И наконец добавляем загрузку таблицы в граф

```python
with DAG(
    dag_id="etl_create_det_offers",
    catchup=False,
    schedule_interval=SCHEDULE,
    start_date=datetime.datetime(2023, 3, 1),
) as dag:
    task_start = BashOperator(task_id="start_task", bash_command="date", dag=dag)

    # load_travelata_task = etl_det_travelata(HOURS, DIRECTORY)
    load_teztour_task = etl_det_teztour(HOURS, DIRECTORY)
    load_pivot_task = etl_det_pivot(HOURS, DIRECTORY)
    load_offers_task = etl_prod_offers(HOURS, DIRECTORY, DAYS_OFFER)
    load_options_task = etl_prod_options(DIRECTORY, BUCKET)
    load_offers_new_task = etl_prod_offers_new(HOURS, DIRECTORY, DAYS_OFFER)

    comb = task_start >> [load_teztour_task] >> load_pivot_task
    # Добавляем наш таск последним в цепочке
    comb >> load_offers_task >> load_options_task >> load_offers_new_task
```

## TODO

- Сейчас процесс загрузки таблицы = сама таблица (то есть он определяется в том же классе, что и таблица). Кажется, нужно разделить эти две вещи. Вопрос - как это организовать

- К предыдущему пункту. Сейчас структура репозитория определяет путь до таблицы в YDB. Кажется это удобно только для YDB, и легко смигрировать всю структуру на другую базу будет тяжело. Подумать над тем, чтобы от этого отказаться


