
# project-1 Usage Guide

Guide for creating a consumer-producer pipeline, setting a Kafka consumer that saves the data into an HBase table to analyze it with a trained sentiment analysis model and visualize the results in a consolidated Metabase dashboard.

---

## Project Structure

```
/project-1
├── docker-compose.yml
├── venv
├── log4j.properties
├── README.md
├── requirements.txt
├── src/
│   ├── consumer.py
│   ├── ETLcomments.py
│   ├── producer.py
│   └── sql_load.py
```
---

## Steps

### 1. Setup Virtual Environment

In the root of the project, create the virtual environment:
```bash
python3 -m venv venv
```

Then, activate the environment:
```bash
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install the required dependencies:
```bash
pip install -r requirements.txt
```

---

### 2. Run Docker Compose to Initialize Services

Ensure Docker Desktop is running (OrbStack if you are on Mac).

In a new terminal window, navigate to the root of the project and execute:
```bash
docker-compose up --build
```

This will initialize:
- Kafka Server (Broker + Zookeeper)
- HBase Server
- MySQL Server
- Metabase Connection

You will see the server logs in that terminal.

---

### 3. Run the Kafka Consumer

Open a new terminal, activate your virtual environment, and run the consumer script:
```bash
python src/consumer.py
```

> **Important**: When you run the consumer for the first time, it will create a new HBase table named `comment` where all incoming messages will be stored.

---

### 4. Run the Kafka Producer

In another new terminal, activate your environment and run the producer script:
```bash
python src/producer.py
```

The producer will display options in the terminal to start sending messages.  
The consumer will listen for new messages and log them into the HBase table.

---

### 5. Run the ETL Pipeline (Sentiment Analysis)

In a new terminal, activate your environment and run the ETL script:
```bash
python src/ETLcomments.py
```

This will:
- Extract comments from the HBase table.
- Analyze sentiments using a Hugging Face model.
- Update the comments with the predicted sentiment.
- Load the enriched data into the MySQL database.

---

### 6. View Data in Metabase

Once the data is loaded into MySQL:

1. Open your browser and visit:
   ```
   http://localhost:3000
   ```
2. Set up your Metabase account and connect to your MySQL database.
3. Start creating dashboards or writing SQL queries to explore your data.

---

## Additional Notes

- Ensure that HBase, Kafka, MySQL, and Metabase containers are fully running before executing the scripts.
- If you need to restart services:
  ```bash
  docker-compose down -v
  docker-compose up --build
  ```
- The sentiment analysis model will be downloaded from Hugging Face the first time you run the ETL script, so ensure you have an internet connection.

- For checking directly the data on Hbase table you can run
  ```bash
  docker exec -t habse hbase shell
  scan 'comment'
  ```
- For loggin in Metabase you need to check the credentials settled in the docker-compose.yml for MySQL, for this particular case:
  Host: mysql-container
  Username: root
  Password: rootpassword
  Database: mydatabase
  Port: 3306

# Ready to Process Comments and Visualize Insights!
