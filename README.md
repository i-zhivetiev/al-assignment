# Вычислительная система

Проект организован в виде пакета Python. Каждый процесс находится в отдельном
модуле и запускается вызовом этого модуля.

## Процессы

- Диспетчер - `al_assignment.dispatcher`
- Клиент - `al_assignment.client`
- Вычислитель - `al_assignment.worker`

## Как запустить

Создать клон репозитория.
```
$ git clone ...
$ cd ...
```

Запустить удобным способом. Например, с помощью виртуального окружения или
прямо из директории пакета.

### Виртуальное окружение

Создать виртуальное окружение:
```
$ virtualenv -p <path-to-python> ./venv
$ source .venv/bin/acitvate
$ pip install ./al-assignment
```

Запустить нужный процесс. Например, процесс клиента:
```
$ python -m al_assignment.client -c <path-to-client.json>
```

### Директория пакета

```
$ python al_assignment/client.py [-c path-to-client.json]
```

## Конфигурация

Каждый процесс конфигурируется с помощью файла в формате `JSON`. По
умолчанию конфигурационный файл ищется в текущей директории, a его имя
совпадает с именем модуля. Например, `client.json` для модуля клиента.

Каждому процессу можно передать аргументы командной строки во время запуска.

### Клиент
#### client.json
```
{
  // параметры сервера процесса
  "server": {
    "port": <int, порт>,
    "min_send_task_period": <int, минимальный интервал, через который будет
                                  отправлено очередное задание, секунды>,
    "max_send_task_period": <int, максимальный интервал, через который будет
                                  отправлено очередное задание, секунды>,
    "request_timeout": <int, таймаут ожидания ответа с результатом вычисления,
                             секунды>
  },

  // параметры сервера, куда будут отправляться задачи; это может быть
  // процесс вычислителя или диспетчера
  "worker": {
    "host": <str, хост, на котором запущен вычислитель или диспетчер>,
    "port": <int, порт>,
  },

  // параметры логгирования, будут переданы в качестве именованных аргументов
  // в logging.basicConfig
  "logging": {
    "level": "DEBUG",
    ...
  }
}
```
#### Командная строка
```
Использование: client.py [-h] [-c CONFIG] [-p PORT]

Запускает процесс клиента.

необязательные аргументы:
  -h, --help
  -c CONFIG, --config CONFIG  конфигурационный файл
  -p PORT, --port PORT        порт, на котором будет запущен сервер;
                              переопределяет значение из конфигурационного
                              файла
```

### Вычислитель
#### worker.json
```
{
  // параметры сервера процесса
  "server": {
    "port": <int, порт>,
    "ping_interval": <int, интервал отправки сообщения ping, секунды>,
    "min_calculation_period": <int, минимальный интервал выполнения
                                    задания, секунды>,
    "max_calculation_period": <int, максимальный интервал выполнения
                                    задания, секунды>,
    "breaking_probability": <float, вероятность поломки вычислителя,
                                    0.0-1.0>,
    "gray_out_period": <int, интервал на который вычислитель выходит из строя
                             в случае поломки, секунды>
  },

  // параметры сервера, куда будут отправляться сообщения ping и выполненные
  // задачи, может быть процессом клиента или диспетчера
  "client": {
    "host": "localhost",
    "port": 9000
  },

  // параметры логгирования, будут переданы в качестве именованных аргументов
  // в logging.basicConfig
  "logging": {
    "level": "DEBUG",
    ...
  }
}
```
#### Командная строка
```
Использование: worker.py [-h] [-c CONFIG] [-p PORT]

Запускает процесс вычислителя.

необязательные аргументы:
  -h, --help
  -c CONFIG, --config CONFIG  конфигурационный файл
  -p PORT, --port PORT        порт, на котором будет запущен сервер;
                              переопределяет значение из конфигурационного
                              файла
```

### Диспетчер
#### dispatcher.json
```
{
  // параметры сервера процесса
  "server": {
    "port": <int, порт>,
    "worker_timeout": <int, интервал, по истечении которого вычислитель
                            считается недоступным, секунды>,
  },

  // параметры логгирования, будут переданы в качестве именованных аргументов
  // в logging.basicConfig
  "logging": {
    "level": "DEBUG",
    ...
  }
}
```
#### Командная строка
```
Использование: dispatcher.py [-h] [-c CONFIG] [-p PORT]

Запускает процесс диспетчера.

необязательные аргументы:
  -h, --help
  -c CONFIG, --config CONFIG  конфигурационный файл
```