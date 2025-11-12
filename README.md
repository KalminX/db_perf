````markdown
## Prerequisites

- Python 3.10+  
- Docker  
- pip  

---

## Setup Virtual Environment

### Linux / macOS
```bash
python3 -m venv venv
source venv/bin/activate
````

### Windows (PowerShell)

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### Windows (cmd.exe)

```cmd
venv\Scripts\activate.bat
```

---

## Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Docker Setup

### PostgreSQL

```bash
docker run -d \
  --name pg-perf-test \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=kalmin123 \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  postgres:16
```

### MongoDB

```bash
docker run -d \
  --name mongo-perf-test \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=kalmin123 \
  -e MONGO_INITDB_DATABASE=perf_test_db \
  -p 27017:27017 \
  mongo:7.0
```

---

## Run Benchmark

```bash
python main.py
```

* Connects to PostgreSQL and MongoDB
* Performs single and bulk CRUD operations
* Measures latency (ms) and throughput (rows/documents/sec)
* Displays rich table and Matplotlib graphs
* Provides automatic conclusions

---

## Notes

* Bulk operations default to 10,000 rows/documents (configurable)
* Ensure ports 5432 and 27017 are free
* Graphs show latency on top and bulk throughput on bottom

```
```

