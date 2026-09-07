# Big Data Streaming & Batch Analytics Pipeline

An educational end-to-end Big Data project using **Apache Kafka, Apache Spark/PySpark, batch processing, Structured Streaming, transaction analytics, and crypto-trade streaming**.

## Architecture

```text
Transaction / Synthetic Data
          |
          v
    Kafka Producer
          |
          v
     Apache Kafka
          |
          v
PySpark Structured Streaming
          |
     +----+----+
     |         |
     v         v
Transform   Risk / AML Analytics
     |         |
     +----+----+
          v
   Processed Output

Historical CSV ---> PySpark Batch ---> Analytics
```

## Technologies
- Python
- Apache Kafka
- Apache Spark / PySpark
- Spark Structured Streaming
- Pandas / NumPy
- pytest
- Linux/WSL2-friendly workflow

## Project Contents
- `data/` - input/sample data
- `src/` - Kafka, Spark, batch, streaming and analytics code
- `tests/` - tests
- `docs/` - architecture and release documentation
- `output/` - runtime output (ignored by Git)

## Setup

### Prerequisites
Install Python 3.10+, Java compatible with your Spark version, Apache Kafka, and Apache Spark.

Check:
```bash
python --version
java -version
spark-submit --version
```

### Python environment
Windows PowerShell:
```powershell
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

Linux/WSL:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Kafka
For Kafka installations using ZooKeeper:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
For KRaft installations, use the startup procedure for your Kafka version.

Create a topic:
```bash
bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092
```

## Run
Inspect available applications first:
```bash
find src -type f
```
Windows PowerShell:
```powershell
Get-ChildItem -Recurse src -File
```

Python producer example:
```bash
python path/to/kafka_producer.py
```

Spark application example:
```bash
spark-submit path/to/spark_script.py
```

Run tests:
```bash
pytest -q
```

## Recommended Demo
1. Start Kafka.
2. Create the required topic.
3. Start the producer.
4. Start the Spark Structured Streaming application.
5. Publish transaction events.
6. Inspect streaming output.
7. Run the historical batch pipeline.

## Data & Licensing
Verify the license and redistribution terms of every dataset before publishing it publicly. If a dataset is not redistributable, replace it with a synthetic sample or instructions for obtaining it. Never commit private or personally identifiable data.

## Security
This is an educational project. Production systems should use Kafka authentication/authorization, TLS, secret management, strict data access controls, validation, monitoring, retention policies, and secure logging.

## Learning Goals
Practice distributed processing, event-driven pipelines, Kafka producers/consumers, Spark DataFrames, Structured Streaming, batch vs. streaming analytics, transaction features, and reproducible Big Data workflows.

## Disclaimer
Risk/AML calculations in this repository are experimental learning components, not production compliance, financial, or fraud-decision systems.
