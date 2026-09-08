# Connecteur Kafka Job
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](../../LICENSE.md)&nbsp;
[![fr](https://img.shields.io/badge/lang-en-red.svg)](README.md)

Le connecteur attend un message Kafka attendu et retourne un succes/echec pour declencher un traitement Visual TOM.

# Disclaimer
Aucun support ni garanties ne seront fournis par Absyss SAS pour ce projet et fichiers associes. L'utilisation est a vos propres risques.

Absyss SAS ne peut etre tenu responsable des dommages causes par l'utilisation d'un des fichiers mis a disposition dans ce depot Github.

Il est possible de faire appel a des jours de consulting pour l'implementation.

# Prerequis

  * Visual TOM 7.3.2 ou plus
  * Python 3.x ou plus
  * Acces a un cluster / topic Kafka
  * Installer les packages python requis avec pip: `pip install -r requirements.txt`

# Consignes
  * Windows :
    * Placer le fichier `kafkaJob.py` dans le repertoire `%ABM_BIN%`
    * Declarer la queue / application custom sur l'agent (voir guide administrateur)

  * Linux/Unix :
    * Placer le fichier `kafkaJob.py` dans le repertoire `$TOM_ADMIN`
    * Declarer la queue / application custom sur l'agent (voir guide administrateur)

  * Creer un Contexte Kafka et definir les variables d'environnement suivantes :
    * `KAFKA_EXPECTED_IDS` : IDs optionnels attendus dans le message, separes par `,`
    * `KAFKA_SECURITY_PROTOCOL` : `PLAINTEXT` / `SASL_SSL` / `SSL` (defaut: `PLAINTEXT`)
    * `KAFKA_SASL_MECHANISM` : mecanisme SASL (ex: `PLAIN`)
    * `KAFKA_USERNAME` : utilisateur SASL
    * `KAFKA_PASSWORD` : mot de passe SASL
    * `KAFKA_SSL_CAFILE` : chemin optionnel du certificat CA

  * Créer dans Visual TOM une connexion "Custom Application" avec la définition suivante ou importer le fichier MyApplication-Kafka.xml:
   ```
   vtimport -x -f MyApplication-Kafka.xml
   ```
   ![Custom application screenshot](screenshots/Kafka_CustomApplication.png?raw=true)

Le script retourne des codes de sortie specifiques :
- `0` : Message correspondant trouve dans le delai
- `1` : Timeout / aucun message / erreur

Le connecteur fournit une journalisation avec :
- Horodatage
- Niveaux de log (INFO, WARNING, ERROR)
- Statut compact des operations
- Logs internes Kafka reduits au niveau WARNING

Exemple de sortie de log :
```
2026-08-07 11:04:12,298 | INFO | Starting Kafka job | servers=localhost:9092 topic=demo-topic group=vtom-job text=VTOM_ID ids=none commit=0(none) wait=60.0s protocol=PLAINTEXT
2026-08-07 11:04:12,298 | INFO | Connected to Kafka
2026-08-07 11:04:15,509 | INFO | Assigned partitions: demo-topic:0
2026-08-07 11:04:15,849 | INFO | Listening (max wait=60.0s)...
2026-08-07 11:04:28,054 | INFO | Matching message found (offset=14, partition=0)
2026-08-07 11:04:28,054 | INFO | Result: OK (code 0)
```

# Licence
Ce projet est sous licence Apache 2.0. Voir le fichier [LICENCE](LICENSE) pour plus de details.

# Code de conduite
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.1%20adopted-ff69b4.svg)](code-of-conduct.md)
Absyss SAS a adopte le [Contributor Covenant](CODE_OF_CONDUCT.md) en tant que Code de Conduite et s'attend a ce que les participants au projet y adherents egalement. Merci de lire le [document complet](CODE_OF_CONDUCT.md) pour comprendre les actions qui seront ou ne seront pas tolerees.
