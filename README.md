# Kafka Job Connector
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](../../LICENSE.md)&nbsp;
[![en](https://img.shields.io/badge/lang-fr-red.svg)](README-fr.md)

This connector waits for an expected Kafka message and returns success/failure for Visual TOM job triggering.

# Disclaimer
No support or warranties will be provided by Absyss SAS for this project and associated files. Use is at your own risk.

Absyss SAS cannot be held responsible for damages caused by the use of any files made available in this Github repository.

Consulting days can be requested for implementation.

# Prerequisites

  * Visual TOM 7.3.2 or higher
  * Python 3.x or higher
  * Access to a Kafka cluster / topic
  * Install required python packages with pip: `pip install -r requirements.txt`

# Instructions
  * Windows :
    * Place the file `kafkaJob.py` in the `%ABM_BIN%` directory
    * Declare the queue / custom application on the agent (see administrator guide)

  * Linux/Unix :
    * Place the file `kafkaJob.py` in the `$TOM_ADMIN` directory
    * Declare the queue / custom application on the agent (see administrator guide)

  * Create a Kafka Context and define the following environment variables:
    * `KAFKA_EXPECTED_IDS`: Optional comma-separated IDs expected in the message
    * `KAFKA_SECURITY_PROTOCOL`: `PLAINTEXT` / `SASL_SSL` / `SSL` (default: `PLAINTEXT`)
    * `KAFKA_SASL_MECHANISM`: SASL mechanism (e.g. `PLAIN`)
    * `KAFKA_USERNAME`: SASL username
    * `KAFKA_PASSWORD`: SASL password
    * `KAFKA_SSL_CAFILE`: Optional CA certificate path

  * Create in Visual TOM a "Custom Application" connection with the following definition or import the MyApplication-Kafka.xml file:
   ```
   vtimport -x -f MyApplication-Kafka.xml
   ```
   ![Custom application screenshot](screenshots/Kafka_CustomApplication.png?raw=true)

The script returns specific exit codes:
- `0`: Matching message found within the timeout
- `1`: Timeout / no matching message / error

The connector provides logging with:
- Timestamps
- Log levels (INFO, WARNING, ERROR)
- Compact operation status
- Kafka library internal logs reduced to WARNING

Example log output:
```
2026-08-07 11:04:12,298 | INFO | Starting Kafka job | servers=localhost:9092 topic=demo-topic group=vtom-job text=VTOM_ID ids=none commit=0(none) wait=60.0s protocol=PLAINTEXT
2026-08-07 11:04:12,298 | INFO | Connected to Kafka
2026-08-07 11:04:15,509 | INFO | Assigned partitions: demo-topic:0
2026-08-07 11:04:15,849 | INFO | Listening (max wait=60.0s)...
2026-08-07 11:04:28,054 | INFO | Matching message found (offset=14, partition=0)
2026-08-07 11:04:28,054 | INFO | Result: OK (code 0)
```

# License
This project is licensed under Apache 2.0. See the [LICENSE](LICENSE) file for more details.

# Code of Conduct
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.1%20adopted-ff69b4.svg)](code-of-conduct.md)
Absyss SAS has adopted the [Contributor Covenant](CODE_OF_CONDUCT.md) as a Code of Conduct and expects project participants to adhere to it as well. Please read the [complete document](CODE_OF_CONDUCT.md) to understand which actions will or will not be tolerated.
