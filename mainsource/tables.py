"""
Generic class to maintain the table configuration and schema
Author : Jigyasu Nayyar
"""

from dataclasses import dataclass
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)
from typing import Tuple
from pyspark.sql import functions as func, DataFrame


@dataclass()
class Table:
    output_name: str
    schema: StructType
    time_fields: Tuple[Tuple[str, str]] = None
    partitions: int = 5
    write_mode: str = "overwrite"
    input_path: str = "../input/"
    output_path: str = "output/"
    dataframe: DataFrame = None
    output_format: str = None
    input_format: str = None
    remove_headers: bool = False
    _dataframe_parsed: DataFrame = None

    @property
    def dataframe_parsed(self) -> DataFrame:
        """
        parsing the Dataframe with the required Schema.
        :return
        Dataframe: Parsed Dataframe
        """
        if self._dataframe_parsed:
            return self._dataframe_parsed
        else:
            self._dataframe_parsed = self.dataframe

            if self.time_fields:
                self._dataframe_parsed = self.dataframe
                for field in self.time_fields:
                    self._dataframe_parsed = self._dataframe_parsed.withColumn(field[0],
                                                                               func.to_timestamp(field[0], field[1]))

            if self.remove_headers:
                df = self._dataframe_parsed
                first = df.first()
                for col in self.schema:
                    self._dataframe_parsed = df \
                        .where(func.col(col.name) != first[col.name])

            return self._dataframe_parsed

    @dataframe_parsed.setter
    def dataframe_parsed(self, df: DataFrame):
        self._dataframe_parsed = df


# # first Table to provide semantic structure to the csv file
events_table = Table(
    output_name="events_aggregated",
    schema=StructType([
        StructField("time", TimestampType(), True),
        StructField("action", StringType(), True)]),
    time_fields=(("time", "yyyy-MM-ddTHH:mm:ss.SSSS"),),
    input_path="input/",
    output_path="output/",
    partitions=10,
    input_format="csv",
    output_format="parquet",
    remove_headers=True,
)
