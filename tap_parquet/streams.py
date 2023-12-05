"""Stream class for tap-parquet."""


from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List, Iterable
import simplejson as json
import pandas as pd
from singer_sdk import metrics

from singer_sdk.helpers._util import utc_now

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
    def __init__(self, batchsize = None, nWorkers = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batchsize = batchsize if batchsize else 1000
        self.nWorkers = nWorkers if nWorkers else 40

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
    
    def get_records(self) -> Iterable[dict]:
        def row_to_dict(row): # helper function, converts the row to messages
            record_message = {
                "type": "RECORD",
                "stream": self.config["filepath"],
                "record": self.stream_maps[0].transform(row._asdict()),
                "version" : None,
                "time_extracted" :utc_now()
            }
            return json.dumps(record_message, use_decimal=True, default=str)
        
        try:
            parquet_file = pq.ParquetFile(self.filepath)
            record_counter = metrics.record_counter(self.name)

            # Process the DataFrame in chunks
            for i in parquet_file.iter_batches(batch_size=self.batchsize):
                chunk = i.to_pandas()

                # execute with workers, multithreaded
                with ThreadPoolExecutor(max_workers=self.nWorkers) as executor:
                    result_list = list(executor.map(row_to_dict, chunk.itertuples(index=False)))

                # covnvert to pd df and then to string 
                result_df = pd.DataFrame(result_list)
                concatenated_string = result_df.iloc[:, 0].astype(str).str.cat(sep="\n")

                # write the string to sysout
                print(concatenated_string)
                record_counter.increment(self.batchsize)

        except Exception as ex:
            raise IOError(f"Could not read from Parquet file '{self.filepath}': {ex}")
        
        return [] # return iterable
        
    def _sync_records(self, *args, **kwargs):
        """Sync records, emitting RECORD messages.

        Args:
            context: Stream partition or context dictionary.
            write_messages: Whether to write Singer messages to stdout.

        Yields:
            Empty array
        """
        # Initialize metrics
        timer = metrics.sync_timer(self.name)

        with timer: # add timer for timing
            # this function is stripped a lot
            # we don't need functionality for partitions, or sorting, we can speedup the process
            # we only use one context
            return self.get_records() 

        
