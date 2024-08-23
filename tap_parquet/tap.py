"""Parquet tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType
)

from tap_parquet.streams import ParquetStream


class TapParquet(Tap):
    """Parquet tap class."""

    name = "tap-parquet"

    config_jsonschema = PropertiesList(
        Property("start_date", DateTimeType),
        Property("filepath", StringType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
            ParquetStream(
                tap=self,
                name=filename,
            )
            for filename in [self.config["filepath"]]
        ]


# CLI Execution:

cli = TapParquet.cli
