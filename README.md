# 🚀 AI-Powered Data Platform with Real-Time Streaming and RAG-based Query Engine

---

## 📌 Project Overview

This project implements a **modern end-to-end data platform** combining real-time streaming, batch processing, lakehouse architecture, and an AI-powered query system using **Retrieval-Augmented Generation (RAG)**.

It enables users to query structured data using **natural language**, bridging the gap between data engineering and AI-driven analytics.

---

## 🧠 Key Capabilities

* Real-time data ingestion and processing
* Scalable lakehouse architecture (Bronze, Silver, Gold layers)
* Batch and streaming data pipelines
* AI-powered natural language querying using RAG
* RESTful API for data access
* Workflow orchestration with Airflow

---

## 🏗️ Architecture

The platform follows a **Lakehouse Architecture**:

* **Bronze Layer** → Raw ingested data
* **Silver Layer** → Cleaned and transformed data
* **Gold Layer** → Aggregated, analytics-ready data

### 🔄 Data Flow

1. Data is generated and streamed via **Kafka**
2. Processed using **Spark Structured Streaming**
3. Stored in **Delta Lake tables (MinIO as S3 backend)**
4. Transformed into analytics-ready datasets
5. Embedded and indexed in **FAISS**
6. Queried via **LLM using RAG pipeline**

📊 Detailed Diagram:
👉 `architecture-diagram/architecture.md`

---

## 🛠️ Tech Stack

| Layer            | Technology                         |
| ---------------- | ---------------------------------- |
| Ingestion        | Kafka                              |
| Processing       | Apache Spark (PySpark), Delta Lake |
| Storage          | MinIO (S3-compatible), Delta Lake  |
| Orchestration    | Apache Airflow                     |
| AI / RAG         | LangChain, OpenAI API, FAISS       |
| API              | FastAPI                            |
| Containerization | Docker, Docker Compose             |

---

## ✨ Features

* 📡 Real-time streaming (Kafka + Spark)
* 🧱 Lakehouse architecture with Delta tables
* 🔄 Batch processing pipelines
* 🤖 RAG-based natural language querying
* 🌐 FastAPI-based REST endpoints
* ⏱️ Airflow orchestration for workflows
* 🧪 Modular and testable codebase

---

## ⚙️ Setup Instructions

### 🔧 Prerequisites

* Docker & Docker Compose
* Python 3.9+
* OpenAI API Key

---

### 1️⃣ Clone Repository

```bash
git clone <your-repo-url>
cd ai-powered-data-platform
```

---

### 2️⃣ Environment Configuration

```bash
cp .env .env.local
```

Update:

```
OPENAI_API_KEY=your_api_key_here
```

---

### 3️⃣ Start Infrastructure

```bash
docker-compose up -d
```

This will start:

* Kafka
* Spark
* MinIO

---

### 4️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 5️⃣ Initialize Lakehouse

```bash
python lakehouse/init_delta.py
```

---

## ▶️ Running the Platform

### 🔹 Start Data Ingestion

```bash
python ingestion/producer.py
```

---

### 🔹 Run Streaming Pipeline

```bash
python streaming/stream_processor.py
```

---

### 🔹 Run Batch Processing

```bash
python processing/batch_processor.py
```

---

### 🔹 Build RAG Embeddings

```bash
python rag/build_embeddings.py
```

---

### 🔹 Start API Server

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

---

### 🔹 Setup Airflow

```bash
airflow db init

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

Run services:

```bash
airflow webserver --port 8080
airflow scheduler
```

---

## 🔍 Example Query

```bash
curl -X POST "http://localhost:8000/query" \
-H "Content-Type: application/json" \
-d '{"question": "What is the total sales by category?"}'
```

---

## 🧪 Testing

```bash
pytest tests/
```

---

## 📁 Project Structure

```
ai-powered-data-platform/
│
├── ingestion/
├── streaming/
├── processing/
├── lakehouse/
├── rag/
├── api/
├── airflow/
├── config/
├── tests/
├── architecture-diagram/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 📸 Screenshots

* Architecture Diagram
* API Response Example
* Airflow DAG View

---

## 🚀 Future Improvements

* Add role-based access control
* Enhance observability and logging
* Deploy on AWS/Azure cloud
* Integrate vector databases like Pinecone

---

## 🤝 Contributing

Contributions are welcome. Please open issues or submit pull requests.

---

## 📄 License

This project is licensed under the MIT License.

---

