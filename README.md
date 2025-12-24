# OPC UA Testing Lab (asyncua)

Repositorio para pruebas y comparativas de versiones de `asyncua` con escenarios
de cliente/servidor, MQTT e InfluxDB. Incluye utilidades y scripts de apoyo,
datasets y herramientas de infraestructura para entornos locales.

## Objetivo

- Probar compatibilidad y cambios de `asyncua` en multiples versiones.
- Ejecutar tests reproducibles con servidor OPC UA local.
- Mantener un entorno de pruebas organizado y escalable.

## Estructura del proyecto

- `scripts/`: scripts de cliente y utilidades.
- `tests/`: pruebas automatizadas con `pytest`.
- `experiments/`: subproyectos y pruebas aisladas.
- `data/`: datasets y resultados.
- `infra/`: configuracion y datos locales de servicios (MQTT, InfluxDB, Grafana).
- `certs/`: certificados y llaves (no se versionan por defecto).
- `third_party/`: dependencias externas de terceros.
- `notebooks/`: notebooks de analisis.

## Requisitos

- Python 3.11+
- `asyncua`, `pytest`, `pytest-asyncio`
- Opcional: Docker (para infraestructura local)

## Instalacion

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e ".[dev]"
```

## Tests

```bash
pytest -q
```

Para probar multiples versiones de `asyncua`:

```bash
tox
```

## Infraestructura local (opcional)

`infra/docker-compose.yml` contiene servicios para InfluxDB, Grafana,
Mosquitto y Telegraf.

```bash
docker compose -f infra/docker-compose.yml up -d
```

## Variables de entorno

Configurar `.env` con credenciales y endpoints necesarios:

- `INFLUXDB_TOKEN`
- `INFLUXDB_ORG`
- `INFLUXDB_URL`
- `INFLUXDB_BUCKET`
- `MQTT_BROKER`
- `MQTT_PORT`
- `MQTT_CAFILE`
- `MQTT_CERT`
- `MQTT_KEY`
- `MQTT_USER`
- `MQTT_PASSWORD`

## Versionado

Se recomienda usar tags para versiones estables del set de pruebas y ramas
de trabajo para nuevos escenarios o versiones de `asyncua`.
