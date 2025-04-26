# Kafka Server example

Tiny example of how to setup a Kafka Server locally, create some Producers and Consumers to show the message transmition between them.

## Steps

### 1. Setup Virtual environment

In the root of the proyect create the environment:
``` bash
python3 -m venv venv
```

Then, activate the environment in your Terminal:
``` bash
source venv/bin/activate
```

Install the `requirements.txt` for the Kafka example:
``` bash
pip install -r kafka_example/requirements.txt
```

### 2. Run the Docker compose to create the Kafka Server

You need to have Docker Desktop running (OrbStack in Mac).

On a new Terminal, go to the root folder of the Kafka Example, run the next command:

``` bash
docker-compose up
```

This will download the Kafka server and it will initiate, in that Terminal you will see the logs from the server.

### 3. Run the Producers

On a new terminal, activate the environment and run the Consumer python script:
``` bash
python kafka_example/src/producer.py
```

You can start sending messages to the server. You can have as many Consumers as you want.

### 3. Run the Consumer

On a new terminal, activate the environment and run the Consumer python script:
``` bash
python kafka_example/src/consumer.py
```

The Consumer will start listening to the server for new messages and log them.
