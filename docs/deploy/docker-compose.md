# Deploy com Docker Compose

## Pre-requisitos
- Docker + Docker Compose

## Passos

1. Copiar `.env.example` para `.env` e ajustar variaveis.
2. Subir stack:

```bash
docker compose up --build -d
```

3. Ver logs:

```bash
docker compose logs -f app
```

4. Health check:

```
http://localhost:5000/health
```

## Encerrar

```bash
docker compose down
```
