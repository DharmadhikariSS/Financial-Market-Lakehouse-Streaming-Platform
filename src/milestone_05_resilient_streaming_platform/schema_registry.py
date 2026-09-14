"""Confluent-Compatible Schema Registry Client and Wire Format Codec.

Implements the Confluent / Redpanda Schema Registry wire format:
- Byte 0: Magic byte (0x00)
- Bytes 1-4: Big-endian 32-bit unsigned integer schema ID
- Bytes 5+: Binary Avro payload via fastavro

Provides schema evolution checking under BACKWARD compatibility rules.
"""

from __future__ import annotations

import io
import json
import struct
from pathlib import Path
from typing import Any, cast

import fastavro

from src.common.logger import get_logger

logger = get_logger("schema_registry")

MAGIC_BYTE = b"\x00"
MAGIC_BYTE_INT = 0


class SchemaRegistryError(Exception):
    """Base exception for schema registry operations."""


class WireFormatError(SchemaRegistryError):
    """Raised when incoming payload does not conform to Confluent wire framing."""


class IncompatibleSchemaError(SchemaRegistryError):
    """Raised when a schema evolution violates compatibility rules."""


class SchemaRegistryClient:
    """In-memory and HTTP-capable Schema Registry manager."""

    def __init__(self, schemas_dir: Path | None = None) -> None:
        self.schemas_dir = schemas_dir or (Path(__file__).parent / "schemas")
        self._id_to_schema: dict[int, dict[str, Any]] = {}
        self._subject_versions: dict[str, list[int]] = {}
        self._next_id = 1
        self._load_local_schemas()

    def _load_local_schemas(self) -> None:
        """Pre-load local .avsc schema definitions if available."""
        v1_path = self.schemas_dir / "trade_event_v1.avsc"
        v2_path = self.schemas_dir / "trade_event_v2.avsc"

        if v1_path.exists():
            schema_v1 = json.loads(v1_path.read_text(encoding="utf-8"))
            self.register("market.trades-value", schema_v1)

        if v2_path.exists():
            schema_v2 = json.loads(v2_path.read_text(encoding="utf-8"))
            self.register("market.trades-value", schema_v2)

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        """Register a schema under a subject after verifying BACKWARD compatibility."""
        parsed_schema = cast(dict[str, Any], fastavro.parse_schema(schema))

        # If subject already has versions, verify BACKWARD compatibility against latest
        if subject in self._subject_versions and self._subject_versions[subject]:
            latest_id = self._subject_versions[subject][-1]
            latest_schema = self._id_to_schema[latest_id]
            is_compat, reason = self.check_backward_compatibility(
                reader_schema=parsed_schema, writer_schema=latest_schema
            )
            if not is_compat:
                raise IncompatibleSchemaError(
                    f"Schema evolution for '{subject}' violates BACKWARD compatibility: {reason}"
                )

        schema_id = self._next_id
        self._next_id += 1
        self._id_to_schema[schema_id] = parsed_schema

        if subject not in self._subject_versions:
            self._subject_versions[subject] = []
        self._subject_versions[subject].append(schema_id)

        version_num = len(self._subject_versions[subject])
        logger.info(
            f"Registered schema for '{subject}' [version={version_num}, schema_id={schema_id}]"
        )
        return schema_id

    def get_schema(self, schema_id: int) -> dict[str, Any]:
        """Retrieve parsed schema by its numeric ID."""
        if schema_id not in self._id_to_schema:
            raise SchemaRegistryError(f"Schema ID {schema_id} not found in registry")
        return self._id_to_schema[schema_id]

    def get_latest_schema_id(self, subject: str) -> int:
        """Get the latest registered schema ID for a given subject."""
        if subject not in self._subject_versions or not self._subject_versions[subject]:
            raise SchemaRegistryError(f"No schemas registered for subject '{subject}'")
        return self._subject_versions[subject][-1]

    @staticmethod
    def check_backward_compatibility(
        reader_schema: dict[str, Any], writer_schema: dict[str, Any]
    ) -> tuple[bool, str]:
        """Check if reader_schema can read data serialized with writer_schema (BACKWARD).

        Rule: Every field present in the reader must either exist in the writer
        OR have a default value defined in the reader.
        """
        reader_fields = {f["name"]: f for f in reader_schema.get("fields", [])}
        writer_fields = {f["name"]: f for f in writer_schema.get("fields", [])}

        for field_name, r_field in reader_fields.items():
            if field_name not in writer_fields:
                if "default" not in r_field:
                    return (
                        False,
                        f"Field '{field_name}' in reader schema does not exist in writer schema and has no default",
                    )
        return True, "Compatible"

    def serialize(
        self, subject: str, record: dict[str, Any], schema_id: int | None = None
    ) -> bytes:
        """Serialize a dict record into Confluent Wire Format bytes."""
        if schema_id is None:
            schema_id = self.get_latest_schema_id(subject)

        schema = self.get_schema(schema_id)
        out_buf = io.BytesIO()

        # Write Confluent framing: 0x00 + 4-byte big-endian schema ID
        out_buf.write(MAGIC_BYTE)
        out_buf.write(struct.pack(">I", schema_id))

        # Write binary Avro record
        fastavro.schemaless_writer(out_buf, schema, record)
        return out_buf.getvalue()

    def deserialize(
        self, payload: bytes, reader_schema_id: int | None = None
    ) -> tuple[int, dict[str, Any]]:
        """Deserialize Confluent Wire Format bytes into a Python dict.

        Returns (schema_id, decoded_record).
        """
        if len(payload) < 5:
            raise WireFormatError(f"Payload too short ({len(payload)} bytes) for Confluent framing")

        magic_byte = payload[0:1]
        if magic_byte != MAGIC_BYTE:
            raise WireFormatError(
                f"Invalid magic byte {magic_byte.hex()}; expected {MAGIC_BYTE.hex()}"
            )

        (schema_id,) = struct.unpack(">I", payload[1:5])
        writer_schema = self.get_schema(schema_id)

        in_buf = io.BytesIO(payload[5:])
        reader_schema = self.get_schema(reader_schema_id) if reader_schema_id else None

        record = fastavro.schemaless_reader(
            in_buf, writer_schema=writer_schema, reader_schema=reader_schema
        )
        return schema_id, cast(dict[str, Any], record)
