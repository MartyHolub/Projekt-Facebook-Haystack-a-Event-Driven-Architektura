# Projekt-Facebook-Haystack-a-Event-Driven-Architektura

Implementace obsahuje 4 FastAPI služby:

- `cloud/s3_gateway/app.py` — S3 Gateway (metadata, upload orchestrace, download proxy, soft delete)
- `cloud/message_broker/app.py` — jednoduchý HTTP Message Broker (Pub/Sub)
- `cloud/haystack_node/app.py` — append-only Haystack storage node s rotací volume souborů
- `cloud/image_worker/app.py` — image worker placeholder

Dále obsahuje:

- `compact.py` — administrační skript pro kompakci jednoho volume souboru
- `cloud/common/contracts.py` — MessagePack kontrakty (`storage.write`, `storage.ack`)
- `cloud/common/broker.py` — HTTP i in-memory broker klient

## Instalace

```bash
python -m pip install -e .[dev]
```

## Spuštění služeb

```bash
uvicorn cloud.message_broker.app:app --host 127.0.0.1 --port 8001
uvicorn cloud.haystack_node.app:app --host 127.0.0.1 --port 8002
uvicorn cloud.s3_gateway.app:app --host 127.0.0.1 --port 8000
uvicorn cloud.image_worker.app:app --host 127.0.0.1 --port 8003
```

## API přehled

### S3 Gateway
- `GET /` (demo frontend UI)
- `POST /upload` (vrací `202`, objekt ve stavu `uploading`)
- `GET /objects/{object_id}` (detail objektu: stav + technické detaily)
- `GET /download/{object_id}` (funguje jen pro `ready` a nesmazané objekty)
- `DELETE /download/{object_id}` (strict soft delete)
- `GET /admin/system-health` (health gateway/broker/haystack pro UI)
- `GET /admin/volumes/{volume_id}/live-objects` (pro compaction)
- `PATCH /admin/objects/{object_id}/location` (pro compaction)
- `POST /admin/volumes/{volume_id}/compact` (spustí `compact.py` z gateway)

### Haystack Node
- `GET /volume/{volume_id}/{offset}/{size}`

### Message Broker
- `POST /topics/{topic}/publish`
- `POST /topics/{topic}/subscribe`
- `GET /topics/{topic}/consume/{consumer_id}`

## Compaction

```bash
python compact.py 1 --gateway-base-url http://127.0.0.1:8000 --volumes-dir volumes
```

Skript načte seznam živých objektů z Gateway, vytvoří `volume_1_compacted.dat`, přepočítá offsety a po úspěchu atomicky nahradí původní `volume_1.dat`.

## Demo UI scénář

1. Spusť služby:
   ```bash
   uvicorn cloud.message_broker.app:app --host 127.0.0.1 --port 8001
   uvicorn cloud.haystack_node.app:app --host 127.0.0.1 --port 8002
   uvicorn cloud.s3_gateway.app:app --host 127.0.0.1 --port 8000
   ```
2. Otevři `http://127.0.0.1:8000/`.
3. V sekci **Upload** nahraj soubor; UI vypíše HTTP kód + `object_id`.
4. V sekci **Objekt** klikni na **Kontrola stavu** a ověř `status`, `volume_id`, `offset`, `size`.
5. Klikni na **Download** (u `ready` objektu uvidíš HTTP `200` a velikost dat).
6. Klikni na **Soft Delete** a znovu **Kontrola stavu** / **Download** (download už vrátí `404`).
7. V sekci **Systémový stav** ověř health pro gateway/broker/haystack.
8. V sekci **Compaction demo** zadej `volume_id` a spusť compaction; UI vypíše HTTP kód a výsledek.

## Testy

```bash
pytest
```
