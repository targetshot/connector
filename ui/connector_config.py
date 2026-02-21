"""Helper utilities for building the Debezium connector configuration."""

from __future__ import annotations

import os

CONNECT_SECRETS_PATH = "/app/data/secrets.properties"

DB_INCLUDE_LIST = os.getenv("TS_CONNECT_DB_INCLUDE_LIST", "SMDB,SSMDB2")
TABLE_INCLUDE_LIST = os.getenv(
    "TS_CONNECT_TABLE_INCLUDE_LIST",
    "",
)
COLUMN_INCLUDE_LIST = os.getenv(
    "TS_CONNECT_COLUMN_INCLUDE_LIST",
    "",
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

    schema_group = "(?:SMDB|SSMDB2)"
    router_configs = [
        ("route_schuetze", rf"(?i)^.+\.{schema_group}\.Schuetze$", "ts.raw.schuetze"),
        ("route_scheiben", rf"(?i)^.+\.{schema_group}\.Scheiben$", "ts.raw.scheiben"),
        ("route_serien", rf"(?i)^.+\.{schema_group}\.Serien$", "ts.raw.serien"),
        ("route_treffer", rf"(?i)^.+\.{schema_group}\.Treffer$", "ts.raw.treffer"),
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
    if DB_INCLUDE_LIST.strip():
        cfg["database.include.list"] = DB_INCLUDE_LIST
    if TABLE_INCLUDE_LIST.strip():
        cfg["table.include.list"] = TABLE_INCLUDE_LIST
    if COLUMN_INCLUDE_LIST.strip():
        cfg["column.include.list"] = COLUMN_INCLUDE_LIST

    if offline_mode:
        cfg.update(
            {
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
            }
        )
        for key in (
            "key.converter.schema.registry.url",
            "value.converter.schema.registry.url",
            "key.converter.basic.auth.credentials.source",
            "value.converter.basic.auth.credentials.source",
            "key.converter.basic.auth.user.info",
            "value.converter.basic.auth.user.info",
            "producer.override.bootstrap.servers",
            "producer.override.security.protocol",
            "producer.override.sasl.mechanism",
            "producer.override.sasl.jaas.config",
        ):
            cfg.pop(key, None)
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
