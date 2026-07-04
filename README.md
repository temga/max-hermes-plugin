# Max Messenger плагин для Hermes Agent

Платформенный адаптер для [Max Messenger](https://max.ru) для [Hermes Agent](https://hermes-agent.nousresearch.com).

Построен на [maxapi](https://github.com/love-apples/maxapi).

## Требования

Перед установкой убедитесь, что у вас есть:

1. **Зарегистрированный бот в Max** — токен из [Max Business Platform](https://business.max.ru/self) → Чат-боты → Создать бота → Расширенные настройки → Настроить
2. **Публичный домен с HTTPS** — Max API отправляет webhook'и только по HTTPS с сертификатом от доверенного CA (с 25 мая 2026 HTTP и самоподписные не принимаются). Домен любой, подойдёт бесплатный + Let's Encrypt
3. **Reverse proxy** (Caddy, nginx) — проксирует запросы с публичного домена на локальный порт адаптера (8088)
4. **Установленный Hermes Agent** v0.18+ с работающим gateway

## Установка

```bash
pip install git+https://github.com/temga/max-hermes-plugin.git
```

После обновления Hermes переустановите плагин:

```bash
pip install --force-reinstall git+https://github.com/temga/max-hermes-plugin.git
```

## Конфигурация

### config.yaml

```yaml
plugins:
  enabled:
    - max

gateway:
  platforms:
    max:
      enabled: true
      extra:
        burst_merge_seconds: 2.0
        busy_text_mode: queue
        busy_text_debounce_seconds: 1.5
        webhook_host: "0.0.0.0"
        webhook_port: 8088
        webhook_path: "/max/webhook"
```

### .env

```bash
MAX_BOT_TOKEN=токен_бота
MAX_WEBHOOK_URL=https://ваш-домен/max/webhook
MAX_WEBHOOK_SECRET=секрет_5_256_символов
# Опционально:
MAX_BOT_USER_ID=user_id_бота
MAX_ALLOWED_USERS=123456,789012
MAX_ALLOW_ALL_USERS=1
```

### Reverse proxy (Caddy)

```
ваш-домен.com {
    handle /max/webhook {
        reverse_proxy 127.0.0.1:8088
    }
}
```

После настройки — `hermes gateway restart`.

## Как это работает

Адаптер работает в webhook-режиме: поднимает локальный HTTP-сервер, подписывается через Max API, и входящие сообщения обрабатываются через POST-запросы от Max.

Long polling не подходит — Max API отдаёт некоторые типы сообщений (голосовые, аудио) через `GET /updates` без тела сообщения. Webhook доставляет полный payload.

SSL для `platform-api2.max.ru` (сертификат Минцифры) обрабатывается библиотекой maxapi — в пакете встроен `russiantrustedca.pem`. SSL для вашего домена — ваша ответственность (Let's Encrypt и т.п.).

## Возможности

- Пересланные сообщения (link.type=forward)
- Burst-merge для серии сообщений от одного пользователя
- Inline-клавиатуры для подтверждения команд и уточняющих вопросов
- Редактирование сообщений (уборка кнопок после callback)
- Загрузка файлов (изображения, видео, голосовые) через `Bot.upload_media()`
- Скачивание медиа для STT и vision-пайплайна
- Per-platform busy_text_mode и debounce

## Зависимости

[maxapi](https://github.com/love-apples/maxapi) >= 1.2.1 — подтягивает aiohttp, backoff, pydantic, aiofiles, puremagic, magic_filter.

## Ссылки

- [Max Bot API](https://dev.max.ru/docs-api/)
- [maxapi](https://love-apples.github.io/maxapi/)

## Лицензия

MIT
