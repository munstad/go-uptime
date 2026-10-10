# go-uptime

Монитор доступности сайтов на Go. Демонстрирует горутины, каналы, пул воркеров,
контексты и graceful shutdown. Только стандартная библиотека.

## Как это работает

```
scheduler --jobs--> [worker 1..N] --results--> aggregator --> state
                                                    |
                                                    +--> hub --> SSE-клиенты
```

- **scheduler** - горутина с `time.Ticker`, раз в интервал кладет адреса в канал `jobs`
- **workers** - N горутин читают `jobs`, делают HTTP-запрос и пишут `Result` в канал `results`
- **aggregator** - читает `results`, обновляет состояние под `sync.RWMutex`
- **hub** - fan-out: рассылает обновления всем подписчикам через буферизованные каналы,
  медленных подписчиков не ждет
- **shutdown** - отмена `context` закрывает `jobs`, воркеры выходят, закрывается `results`,
  агрегатор завершается; SSE-соединения закрываются через `done`-канал

## Запуск

```bash
go run . -interval 5s -workers 4 https://go.dev https://github.com https://example.com
```

Дашборд: http://localhost:8080

Флаги: `-addr`, `-interval`, `-workers`, `-timeout`. Адреса передаются аргументами.

Docker:

```bash
docker build -t go-uptime .
docker run -p 8080:8080 go-uptime https://go.dev https://github.com
```

## API

- `GET /` - живая страница (обновляется через SSE)
- `GET /api/status` - текущее состояние в JSON
- `GET /api/events` - поток Server-Sent Events
- `GET /healthz` - healthcheck

## Тесты

```bash
go test -race ./...
```

## Идеи для развития

- История проверок и аптайм в процентах (SQLite)
- Уведомления в Telegram при падении сайта
- Конфиг из YAML, экспорт метрик Prometheus
- Retry с экспоненциальной паузой
