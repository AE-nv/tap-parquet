"""Stream class for tap-parquet."""
from pathlib import Path
from typing import Optional, List, Iterable

from singer_sdk.streams import Stream
from singer_sdk.typing import (
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
    JSONTypeHelper,
)

import pyarrow.parquet as pq

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def get_jsonschema_type(ansi_type: str) -> JSONTypeHelper:
    """Return a JSONTypeHelper object for the given type name."""
    if "int" in ansi_type:
        return IntegerType()
    if "double" in ansi_type:
        return NumberType()
    if "string" in ansi_type:
        return StringType()
    if "bool" in ansi_type:
        return BooleanType()
    if "timestamp[ns]" in ansi_type:
        return DateTimeType()
    if "timestamp[us]" in ansi_type:
        return DateTimeType()
    raise ValueError(f"Unmappable data type '{ansi_type}'.")


class ParquetStream(Stream):
    """Stream class for Parquet streams."""

    @property
    def filepath(self) -> str:
        """Return the filepath for the parquet stream."""
        return self.config["filepath"]

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.

        This is evaluated prior to any records being retrieved.
        """
        properties: List[Property] = []
        parquet_schema = pq.ParquetFile(self.filepath).schema_arrow
        for i in range(len(parquet_schema.names)):
            name, dtype = parquet_schema.names[i], parquet_schema.types[i]
            properties.append(Property(name, get_jsonschema_type(str(dtype))))
        return PropertiesList(*properties).to_dict()

    def get_records(self, partition: Optional[dict] = None) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        try:
            parquet_file = pq.ParquetFile(self.filepath)
        except Exception as ex:
            raise IOError(f"Could not read from parquet file '{self.filepath}': {ex}")
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            for batch in table.to_batches():
                for row in zip(*batch.columns):
                    yield {
                        table.column_names[i]: val.as_py()
                        for i, val in enumerate(row, start=0)
                    }
