# """Parquet tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
    NumberType
)

from tap_parquet.streams import ParquetStream

class TapParquet(Tap):
    """Parquet tap class."""

    name = "tap-parquet"

    config_jsonschema = PropertiesList(
        Property("start_date", DateTimeType),
        Property("filepath", StringType, required=True),
        Property("batch_size", NumberType, required=False, default=1000), 
        Property("n_workers", NumberType, required=False, default=40), 
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
            ParquetStream(
                tap=self,
                name=filename,
                batch_size=self.config["batch_size"],
                n_workers = self.config["n_workers"]
            )
            for filename in [self.config["filepath"]]
        ]


cli = TapParquet.cli
