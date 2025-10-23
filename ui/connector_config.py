"""Helper utilities for building the Debezium connector configuration."""

from __future__ import annotations

import os

CONNECT_SECRETS_PATH = "/app/data/secrets.properties"

DB_INCLUDE_LIST = os.getenv("TS_CONNECT_DB_INCLUDE_LIST", "SMDB,SSMDB2")
TABLE_INCLUDE_LIST = os.getenv(
    "TS_CONNECT_TABLE_INCLUDE_LIST",
    "SMDB.Schuetze,SSMDB2.Scheiben,SSMDB2.Serien,SSMDB2.Treffer",
)
COLUMN_INCLUDE_LIST = os.getenv(
    "TS_CONNECT_COLUMN_INCLUDE_LIST",
    (
        "SMDB.Schuetze.SportpassID,SMDB.Schuetze.VereinsID,SMDB.Schuetze.Nachname,"
        "SMDB.Schuetze.Vorname,SMDB.Schuetze.EMail,SSMDB2.Scheiben.ScheibenID,"
        "SSMDB2.Scheiben.Starterliste,SSMDB2.Scheiben.StarterlistenID,"
        "SSMDB2.Scheiben.Nachname,SSMDB2.Scheiben.Vorname,"
        "SSMDB2.Scheiben.SportpassID,SSMDB2.Scheiben.Disziplin,"
        "SSMDB2.Scheiben.DisziplinID,SSMDB2.Scheiben.KlassenID,"
        "SSMDB2.Scheiben.Klasse,SSMDB2.Scheiben.Verein,SSMDB2.Scheiben.VereinsID,"
        "SSMDB2.Scheiben.Trefferzahl,SSMDB2.Scheiben.TotalRing,"
        "SSMDB2.Scheiben.TotalRing01,SSMDB2.Scheiben.BesterTeiler01,"
        "SSMDB2.Scheiben.Zeitstempel,SSMDB2.Serien.ScheibenID,"
        "SSMDB2.Serien.Stellung,SSMDB2.Serien.Serie,SSMDB2.Serien.Ring,"
        "SSMDB2.Serien.Ring01,SSMDB2.Treffer.ScheibenID,SSMDB2.Treffer.Stellung,"
        "SSMDB2.Treffer.Treffer,SSMDB2.Treffer.x,SSMDB2.Treffer.y,"
        "SSMDB2.Treffer.Innenzehner,SSMDB2.Treffer.Ring,SSMDB2.Treffer.Ring01,"
        "SSMDB2.Treffer.Teiler01,SSMDB2.Treffer.Zeitstempel,"
        "SSMDB2.Treffer.Millisekunden"
    ),
)
DB_CONNECTION_TZ = os.getenv("TS_CONNECT_DB_CONNECTION_TZ", "Europe/Berlin")
SNAPSHOT_MODE = os.getenv("TS_CONNECT_SNAPSHOT_MODE", "initial")
HISTORY_BOOTSTRAP = os.getenv("TS_CONNECT_HISTORY_BOOTSTRAP", "redpanda:9092")
HISTORY_TOPIC = os.getenv("TS_CONNECT_HISTORY_TOPIC", "_ts_db_history")
SINGLE_TOPIC_MODE = os.getenv("TS_CONNECT_SINGLE_TOPIC", "false").strip().lower() in {"1", "true", "yes", "on"}
SINGLE_TOPIC_REGEX = os.getenv("TS_CONNECT_SINGLE_TOPIC_REGEX", ".*")
TOPIC_REPLICATION_FACTOR = os.getenv("TS_CONNECT_TOPIC_REPLICATION_FACTOR", "1")
TOPIC_PARTITIONS = os.getenv("TS_CONNECT_TOPIC_PARTITIONS", "3")


def build_connector_config(settings: dict, *, offline_mode: bool = False) -> dict:
    """Return the Kafka Connect configuration payload for Debezium.

    Parameters
    ----------
    settings: dict
        Dictionary containing the persisted UI settings. Expected keys:
        db_host, db_port, db_user, server_id, server_name, topic_prefix,
        confluent_bootstrap, confluent_sasl_username.
    """
    transforms = ["unwrap", "addsrc"]

    router_configs = [
        ("route_schuetze", r"^.+\.SMDB\.Schuetze$", "ts.raw.schuetze"),
        ("route_scheiben", r"^.+\.SSMDB2\.Scheiben$", "ts.raw.scheiben"),
        ("route_serien", r"^.+\.SSMDB2\.Serien$", "ts.raw.serien"),
        ("route_treffer", r"^.+\.SSMDB2\.Treffer$", "ts.raw.treffer"),
    ]

    if not SINGLE_TOPIC_MODE:
        transforms.extend(name for name, _, _ in router_configs)

    cfg: dict[str, str] = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": settings["db_host"],
        "database.port": str(settings["db_port"]),
        "database.user": settings["db_user"],
        "database.password": f"${{file:{CONNECT_SECRETS_PATH}:db_password}}",
        "database.server.id": str(settings["server_id"]),
        "database.server.name": settings["server_name"],
        "include.schema.changes": "false",
        "topic.prefix": settings["topic_prefix"],
        "database.allowPublicKeyRetrieval": "true",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "database.history.kafka.bootstrap.servers": HISTORY_BOOTSTRAP,
        "database.history.kafka.topic": HISTORY_TOPIC,
        "schema.history.internal.kafka.bootstrap.servers": HISTORY_BOOTSTRAP,
        "schema.history.internal.kafka.topic": HISTORY_TOPIC,
        "producer.override.bootstrap.servers": f"${{file:{CONNECT_SECRETS_PATH}:confluent_bootstrap}}",
        "producer.override.security.protocol": "SASL_SSL",
        "producer.override.sasl.mechanism": "PLAIN",
        "producer.override.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='${{file:{CONNECT_SECRETS_PATH}:confluent_sasl_username}}' "
            f"password='${{file:{CONNECT_SECRETS_PATH}:confluent_sasl_password}}';"
        ),
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
        "key.converter.schema.registry.url": f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_url}}",
        "value.converter.schema.registry.url": f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_url}}",
        "key.converter.basic.auth.credentials.source": "USER_INFO",
        "value.converter.basic.auth.credentials.source": "USER_INFO",
        "key.converter.basic.auth.user.info": (
            f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_key}}:"
            f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_secret}}"
        ),
        "value.converter.basic.auth.user.info": (
            f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_key}}:"
            f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_secret}}"
        ),
        "database.include.list": DB_INCLUDE_LIST,
        "table.include.list": TABLE_INCLUDE_LIST,
        "column.include.list": COLUMN_INCLUDE_LIST,
        "database.connectionTimeZone": DB_CONNECTION_TZ,
        "snapshot.mode": SNAPSHOT_MODE,
        "topic.creation.default.replication.factor": TOPIC_REPLICATION_FACTOR,
        "topic.creation.default.partitions": TOPIC_PARTITIONS,
        "topic.creation.default.cleanup.policy": "delete",
        "errors.retry.timeout": "600",
        "errors.retry.delay.max.ms": "10000",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.addsrc.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.addsrc.topic.field": "source_topic",
    }

    if offline_mode:
        cfg.update(
            {
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
            }
        )
    else:
        cfg.update(
            {
                "producer.override.bootstrap.servers": f"${{file:{CONNECT_SECRETS_PATH}:confluent_bootstrap}}",
                "producer.override.security.protocol": "SASL_SSL",
                "producer.override.sasl.mechanism": "PLAIN",
                "producer.override.sasl.jaas.config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f"username='${{file:{CONNECT_SECRETS_PATH}:confluent_sasl_username}}' "
                    f"password='${{file:{CONNECT_SECRETS_PATH}:confluent_sasl_password}}';"
                ),
                "key.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "key.converter.schemas.enable": "true",
                "value.converter.schemas.enable": "true",
                "key.converter.schema.registry.url": f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_url}}",
                "value.converter.schema.registry.url": f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_url}}",
                "key.converter.basic.auth.credentials.source": "USER_INFO",
                "value.converter.basic.auth.credentials.source": "USER_INFO",
                "key.converter.basic.auth.user.info": (
                    f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_key}}:"
                    f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_secret}}"
                ),
                "value.converter.basic.auth.user.info": (
                    f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_key}}:"
                    f"${{file:{CONNECT_SECRETS_PATH}:schema_registry_secret}}"
                ),
            }
        )

    if not SINGLE_TOPIC_MODE:
        for name, regex, replacement in router_configs:
            cfg.update(
                {
                    f"transforms.{name}.type": "org.apache.kafka.connect.transforms.RegexRouter",
                    f"transforms.{name}.regex": regex,
                    f"transforms.{name}.replacement": replacement,
                }
            )
    else:
        transforms.append("route")
        cfg.update(
            {
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": SINGLE_TOPIC_REGEX,
                "transforms.route.replacement": settings["topic_prefix"],
            }
        )

    if SINGLE_TOPIC_MODE:
        cfg["transforms"] = ",".join(transforms)
        return cfg

    cfg["transforms"] = ",".join(transforms)

    return cfg
