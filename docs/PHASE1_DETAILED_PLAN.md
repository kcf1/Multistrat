# Phase 1: Detailed Plan — Docker, Postgres (versioned), Redis

**Goal:** Run infrastructure locally with Docker Compose; Postgres with version-controlled schema; Redis ready for streams. No application services yet.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **Orchestration** | Docker Compose |
| **Postgres** | 16-alpine, with version-controlled migrations only (no schema in Phase 2 yet) |
| **Redis** | 7-alpine |
| **pgAdmin** | dpage/pgadmin4 — web UI for Postgres (optional; useful for inspecting DB and running SQL) |
| **Redis GUI** | redis/redisinsight — web UI for Redis (optional; browse keys, streams, CLI) |
| **Postgres versioning** | [Alembic](https://alembic.sqlalchemy.org/) — migrations in repo; single source of truth for schema |

---

## 2. Running the stack (Compose)

**Preferred:** Run the stack directly with Compose. Images are pulled automatically when missing.

From the project root (where `docker-compose.yml` lives):

```bash
docker-compose up -d
```

On first run, Compose will pull `postgres:16-alpine` and `redis:7-alpine` as needed, then start the services. No separate pull step is required.

### 2.1 Optional: pre-pull images

If you want to pull images in advance (e.g. CI, slow network, or to refresh after changing image tags):

```bash
docker-compose pull
```

Or by image name: `docker pull postgres:16-alpine` and `docker pull redis:7-alpine`. Then run `docker-compose up -d` as usual.

### 2.2 Verify images (optional)

```bash
docker images postgres:16-alpine redis:7-alpine
```

Both should appear with an image ID and size after the first successful `docker-compose up -d`.

---

## 3. Postgres version control (Alembic)

### 3.1 Principles

- **All schema changes are migrations:** No ad-hoc SQL; every change is a new Alembic revision.
- **Migrations are ordered and immutable:** Once applied, do not edit; add a new revision to fix or extend.
- **Applied revisions are recorded by Alembic** in the `alembic_version` table so the same revision is never applied twice.

### 3.2 Directory layout

After `alembic init alembic` (or `alembic init migrations` if you prefer that name):

```
alembic/
├── env.py              # Use DATABASE_URL from environment; target_metadata if using SQLAlchemy models
├── script.py.mako      # Template for new revisions
├── README
└── versions/
    ├── (empty in Phase 1 or one initial revision)
    └── xxxxx_initial.py   # Optional: first revision (no-op or bootstrap)
alembic.ini              # Config; set sqlalchemy.url from env or leave for env.py
```

- **Version table:** Alembic creates and uses `alembic_version` (single row with current revision id). No custom `schema_migrations` table needed.

### 3.3 Configure Alembic to use DATABASE_URL

- In `alembic/env.py`: read `DATABASE_URL` from the environment (e.g. `os.environ.get("DATABASE_URL")`) and set the engine URL for migrations (e.g. `config.set_main_option("sqlalchemy.url", url)` or pass the URL to `create_engine` and use `connection` in `run_migrations_online`). Do not hard-code credentials in `alembic.ini`; load from `.env` or env.
- Ensure `.env` is loaded when you run Alembic (e.g. `python-dotenv` in `env.py` or export variables before `alembic upgrade head`).

### 3.4 Creating and running migrations

- **Create a new revision:**  
  `alembic revision -m "description_in_snake_case"`  
  This adds a new file under `alembic/versions/`. Edit `upgrade()` and `downgrade()` as needed.

- **Apply all pending migrations:**  
  `alembic upgrade head`

- **Current revision:**  
  `alembic current`

- **History:**  
  `alembic history`

### 3.5 Phase 1 migration content

- **Option A:** No revisions in Phase 1. Run `alembic upgrade head` once Postgres is up; nothing applies; `alembic_version` may not exist until the first revision is added. Then add the first "initial" revision in Phase 2 when you add real tables.
- **Option B (recommended):** Add one initial revision in Phase 1 that is a no-op (empty `upgrade()` / `downgrade()`) or that only creates a small bootstrap. Then run `alembic upgrade head` so the database is under Alembic from day one and `alembic_version` has one row.

No tables for booking, positions, or market data in Phase 1; those are added in Phase 2+ as new Alembic revisions.

### 3.6 Dependencies

- Python 3.x. Use a **requirements.txt** for reproducibility (see below). A **venv is recommended but not required**: you can create one with `python -m venv .venv` and activate it, then `pip install -r requirements.txt`; or install into system Python if you prefer.
- Suggested **requirements.txt** for Phase 1 (Alembic only):

  ```
  alembic>=1.13
  psycopg2-binary>=2.9
  python-dotenv>=1.0   # optional: load .env in alembic/env.py
  ```

---

## 4. Docker Compose

### 4.1 File: `docker-compose.yml`

- **Services:** `postgres`, `redis`, `pgadmin`, `redisinsight`.
- **Network:** One custom network (e.g. `multistrat`) so future app containers can attach and reach both.

Example structure:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-multistrat}"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped
    networks:
      - multistrat

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped
    networks:
      - multistrat

  pgadmin:
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: ${PGADMIN_DEFAULT_EMAIL}
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_DEFAULT_PASSWORD}
    ports:
      - "5050:80"
    restart: unless-stopped
    networks:
      - multistrat
    depends_on:
      postgres:
        condition: service_healthy

  redisinsight:
    image: redis/redisinsight:latest
    ports:
      - "5540:5540"
    volumes:
      - redisinsight_data:/data
    restart: unless-stopped
    networks:
      - multistrat
    depends_on:
      redis:
        condition: service_healthy

volumes:
  postgres_data:
  redisinsight_data:

networks:
  multistrat:
    driver: bridge
```

- Use `.env` for `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` so no secrets in the file.
- No Redis volume in Phase 1 (add later if you want AOF).

### 4.2 pgAdmin

- **Image:** `dpage/pgadmin4:latest`. Web UI for Postgres; useful for browsing tables, running SQL, and inspecting migrations.
- **Login:** Open http://localhost:5050 and sign in with `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`.
- **Add server:** In pgAdmin, register a new server; host is `postgres` (Docker service name), port `5432`, username/password/database from `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`.

### 4.3 RedisInsight (Redis GUI)

- **Image:** `redis/redisinsight:latest`. Web UI for Redis; browse keys, inspect streams, use CLI.
- **URL:** http://localhost:5540. First time: add a Redis database; host is `redis` (Docker service name), port `6379` (no password unless you add one later).
- **Volume:** `redisinsight_data` persists added connections and settings.

### 4.4 No init SQL for schema

- Do **not** put application schema in `/docker-entrypoint-initdb.d/`. Schema is managed only by Alembic and `alembic/versions/` so it stays version-controlled and repeatable.

---

## 5. Redis

- **Image:** `redis:7-alpine`.
- **Port:** 6379.
- **Persistence:** None in Phase 1; enable AOF later if needed.
- **Usage:** Streams will be created when Phase 2+ services start; no Redis setup in Phase 1 beyond ensuring the container is healthy.

---

## 6. Environment and config

### 6.1 `.env.example`

Provide a template so developers can copy to `.env`:

```env
# Postgres
POSTGRES_USER=multistrat
POSTGRES_PASSWORD=changeme
POSTGRES_DB=multistrat

# pgAdmin (web UI: http://localhost:5050)
PGADMIN_DEFAULT_EMAIL=admin@local.dev
PGADMIN_DEFAULT_PASSWORD=admin

# Connection strings (for apps and Alembic)
DATABASE_URL=postgresql://multistrat:changeme@localhost:5432/multistrat
REDIS_URL=redis://localhost:6379
```

- `DATABASE_URL` is used by Alembic and later by Booking/Position Keeper/Market Data.
- `REDIS_URL` is used by future services.

### 6.2 `.gitignore`

- Ignore `.env` (so real passwords are not committed).
- Do **not** ignore `alembic/` or `.env.example`.

---

## 7. Task checklist (implementation order)

Execute in this order: define and start the stack first so Postgres is running and reachable, then set up Alembic and run migrations.

- [ ] **7.1** Add `docker-compose.yml` with `postgres`, `redis`, `pgadmin`, and `redisinsight`; use `.env` for Postgres and pgAdmin vars.
- [ ] **7.2** Add `.env.example` and ensure `.env` is in `.gitignore` (copy to `.env` and set values).
- [ ] **7.3** Start stack: `docker-compose up -d` (Compose will pull images if needed); wait for Postgres and Redis to be healthy. Verify with `docker-compose ps` and e.g. `psql "$DATABASE_URL" -c 'SELECT 1'`.
- [ ] **7.4** Add `requirements.txt` (alembic, psycopg2-binary, optional python-dotenv). Then install: `pip install -r requirements.txt` — use a venv (e.g. `python -m venv .venv`) if you want isolation, or system Python is fine.
- [ ] **7.5** Initialize Alembic: `alembic init alembic`; configure `alembic/env.py` to use `DATABASE_URL` from environment (and optionally load `.env`).
- [ ] **7.6** Add one initial revision: `alembic revision -m "initial"` and leave `upgrade()`/`downgrade()` empty (or add a no-op) so Phase 1 has a single applied revision.
- [ ] **7.7** Run migrations: `alembic upgrade head`; `alembic current` should show the initial revision; `alembic_version` in Postgres has one row.
- [ ] **7.8** Update main README “Getting Started”: clone, copy `.env.example` to `.env`, `docker-compose up -d`, run `alembic upgrade head`, verify Postgres and Redis from host.

---

## 8. Verification (acceptance)

These can be run manually or turned into a small script/CI step (see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#testing-corresponding-to-each-phase)).

- [ ] `docker-compose up -d` succeeds; `docker-compose ps` shows postgres, redis, pgadmin, and redisinsight healthy (or running).
- [ ] pgAdmin: http://localhost:5050 — login with `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`; add server host `postgres`, port 5432, user/password/db from `.env`.
- [ ] RedisInsight: http://localhost:5540 — add Redis database host `redis`, port 6379; browse keys and streams.
- [ ] From host: `psql "$DATABASE_URL" -c '\dt'` shows `alembic_version` (and default `pg_*` catalogs). No application tables yet if using Option B with empty initial revision.
- [ ] `psql "$DATABASE_URL" -c 'SELECT * FROM alembic_version;'` shows one row (revision id).
- [ ] `alembic current` matches that revision; running `alembic upgrade head` again does nothing (idempotent).
- [ ] From host: `redis-cli -u $REDIS_URL ping` returns `PONG`.

---

## 9. Out of scope for Phase 1

- Application containers (OMS, Booking, etc.).
- Creating Redis streams (done in Phase 2 when services are added).
- Any Postgres tables other than `alembic_version` (booking/positions/market data in Phase 2+).
- Redis persistence (AOF) — optional later.

---

## 10. Handoff to Phase 2

- Phase 2 will add new Alembic revisions (e.g. `alembic revision -m "add_accounts_positions"`) for `accounts`, `positions`, `balances`, etc., and run `alembic upgrade head` against the same `DATABASE_URL`.
- Same Compose file can be extended with app services that use `multistrat` network and `DATABASE_URL` / `REDIS_URL`.
