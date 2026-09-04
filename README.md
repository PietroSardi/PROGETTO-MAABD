# FinDetect

Prototipo per il corso **MAADB 2025/26** (Univ. Torino).
Polyglot persistence su dataset **LDBC FinBench SF 1**: stessi dati in
**Neo4j** (grafo delle transazioni) e **MongoDB** (anagrafiche / aggregati).
UI Flask + Tailwind per eseguire 6 query dimostrative.

## Prerequisiti

- **Docker Desktop** (Mongo + Neo4j girano in container)
- **Python 3.11 o 3.12** e **Maven 3.9+** (via Homebrew)
- **OpenJDK 11** e **Maven** — servono *solo* se si vuole rigenerare il dataset
  con il datagen (i CSV di SF 1 sono già inclusi nel repo, quindi normalmente no)

Su Mac ARM (M1/M2/M3/M4/M5):
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# aggiungi brew al PATH come ti dice lo script, poi:
brew install python@3.12
brew install --cask docker   # oppure scarica Docker.app da docker.com
brew install maven openjdk@11   # opzionale, solo per il datagen
```
Aggiungi `JAVA_HOME` al tuo profilo:
```bash
echo '' >> ~/.zprofile
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"' >> ~/.zprofile
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zprofile
source ~/.zprofile
java -version   # deve dire openjdk 11.x
```

> Nota Mac ARM: `openjdk@8` e `temurin@8` su brew sono solo x86_64
> (serve Rosetta 2). OpenJDK 11 è la scelta più pulita.

### Clonare
```bash
git clone https://github.com/PietroSardi/PROGETTO-MAABD.git
cd PROGETTO-MAABD
```
Il repo include i **CSV del dataset SF 1** (`data/bulk-load/raw`, ~1 GB): si può
saltare il passo **2 (datagen)** e andare diretti a Docker + ETL + app.

## Struttura del progetto

```
.
├── docker-compose.yml         # Neo4j + MongoDB (replica set a 1 nodo)
├── mongo-init/init-replica.js # script di init del replica set
├── datagen/run_datagen.sh     # clona + builda + lancia il datagen v0.1.0 (opzionale)
├── etl/load_mongo.py          # CSV -> MongoDB
├── etl/load_neo4j.py          # CSV -> Neo4j
├── app/                       # Flask + 6 query
│   ├── app.py                 # rotte, config delle query, /api/random
│   ├── db/{mongo.py,neo4j.py} # connessioni (una funzione per DB)
│   ├── queries/q1..q6.py      # una query per file; _common.py = helper condivisi
│   └── templates/{index,results}.html
├── test_smoke.py              # esegue le 6 query coi default e verifica che tornino righe
├── docs/                      # relazione (tex + pdf), outline presentazione, screenshot
├── data/bulk-load/raw/        # CSV del dataset SF 1 (inclusi nel repo)
└── requirements.txt
```

## Avvio passo-passo

### 0. Setup iniziale (una tantum)
Prepara ambiente Python e credenziali **prima di tutto**:
```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt   # installa python-dotenv (serve a leggere .env)
cp .env.example .env               # poi metti la password reale in NEO4J_PASS
```
> Se sposti o rinomini la cartella del progetto il venv smette di funzionare
> (contiene path assoluti): cancellalo e ricrealo con i due comandi sopra.

### 1. Database in Docker
```bash
docker compose up -d
```
Al primo avvio il servizio `mongo-init` chiama `rs.initiate()` per creare
il replica set `rs0` (serve per le transazioni MongoDB 7) e termina.

- Neo4j Browser: http://localhost:7474 — user `neo4j`, pwd = `NEO4J_PASS` (vedi `.env`)
- MongoDB Compass: `mongodb://localhost:27017/?replicaSet=rs0`

### 2. Generazione dataset SF 1 (opzionale: i CSV sono già nel repo)
```bash
./datagen/run_datagen.sh
```
Lo script:
1. clona `ldbc_finbench_datagen` al tag `v0.1.0` in `datagen/_repo/`
2. compila con `mvn clean package -DskipTests`
3. scarica Spark in `~/spark` (se assente)
4. lancia il datagen con `--scale-factor 1 --memory 8g`
5. copia i CSV grezzi in `data/bulk-load/`

Tempo indicativo su laptop Apple Silicon: 20-30 min la prima volta
(dominano i download Maven + Spark).

### 3. ETL (CSV → MongoDB e Neo4j)
```bash
python -m etl.load_mongo
python -m etl.load_neo4j
```
Circa 10-15 minuti in tutto su un laptop recente (Neo4j è la parte lenta: 5,4 M
archi). `load_mongo` accetta anche nomi di collezione per ricaricarne solo alcune,
es. `python -m etl.load_mongo companyOwnAccount`.

### 4. Avvio app
```bash
python -m app.app
```
Apri http://localhost:5050 e scegli la query dal menu.

### 5. Verifica rapida (opzionale)
```bash
python test_smoke.py
```
Esegue le 6 query con i parametri di default e controlla che tornino righe;
utile prima della demo per accertarsi che i DB siano su e popolati.

## Le 6 query

| ID | DB           | Descrizione |
|----|--------------|-------------|
| Q1 | Neo4j        | Transfer uscenti da un account in una finestra temporale |
| Q2 | MongoDB      | Profilo completo persona: anagrafica + conti + prestiti |
| Q3 | Cross-DB     | Scheda conto + intestatario (persona o azienda) + top-10 vicini per numero di transfer |
| Q4 | Neo4j        | Cicli di transfer A→B→C→A oltre una soglia e finestra |
| Q5 | MongoDB      | Top-10 account per volume trasferito in un mese |
| Q6 | Cross-DB     | Hub detection (degree centrality, per soglia assoluta **o** top x%) + tipo conto, data apertura e profilo intestatario |

Ogni query restituisce `{rows, count, elapsed_ms}`.

**Hub detection (Q6)**: il criterio di default è la soglia assoluta sul degree
(`minDegree`), quello operativo per l'antifrode. Compilando il campo opzionale
`Top x%` si passa al criterio relativo (frazione più attiva della distribuzione);
in entrambi i casi la tabella riporta quanti hub soddisfano il criterio e il degree
minimo fra loro, così i due approcci si confrontano a colpo d'occhio.

## Reset / pulizia

```bash
docker compose down -v
rm -rf .neo4j_data .mongo_data datagen/_repo datagen/_out   # i CSV in data/bulk-load/raw restano
```

## Variabili d'ambiente

| Var         | Default                                                |
|-------------|--------------------------------------------------------|
| MONGO_URI   | `mongodb://localhost:27017/?replicaSet=rs0`            |
| MONGO_DB    | `findetect`                                            |
| NEO4J_URI   | `bolt://localhost:7687`                                |
| NEO4J_USER  | `neo4j`                                                |
| NEO4J_PASS  | da `.env` (template in `.env.example`)                 |
| DATA_DIR    | `./data/bulk-load`                                     |
