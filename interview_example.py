from functools import reduce
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, split, cast
from pyspark.sql.types import (
    FloatType,
)

spark = SparkSession.builder.appName("test").getOrCreate()


def load_csv(path: str):
    try:
        df = spark.read.option("header", True).csv(path)
    except IOError as e:
        raise IOError("Could not load the file. Exception is {}".format(e))
    else:
        logging.info("Csv loaded successfully")
        return df


def isfloat(str):
    try:
        float(str)
    except ValueError:
        return False
    return True


udf1 = udf(
    lambda arr: [
        int(x) if x.isdigit() else (float(x) if isfloat(x) else None) for x in arr
    ]
    if arr is not None
    else arr,
)


def multiply_array_items(arr):
    if arr is not None:
        arr = reduce(
            lambda x, y: x * y, list(filter(lambda item: item is not None, arr))
        )
    return arr


udf2 = udf(multiply_array_items)

udf3 = udf(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x)


def populate_total_weight(df):
    try:

        df2 = df.withColumn(
            "total_weight2",
            when(
                (df.total_weight.isNull()) & (df.pack_size_text.isNotNull()),
                udf3(udf2(udf1(split(df.pack_size_text, " ")))),
            )
            .when(
                (df.total_weight.isNull())
                & (df.pack_size.isNotNull())
                & (df.item_weight.isNotNull()),
                udf3(df.pack_size * df.item_weight),
            )
            .when(
                (df.total_weight.isNull())
                & (df.pack_size.isNotNull())
                & (df.item_volume.isNotNull())
                & (df.item_density.isNotNull()),
                udf3(df.pack_size * df.item_volume * df.item_density),
            )
            .otherwise(df.total_weight),
        )

        df2.show(20, False)
    except Exception as e:
        raise Exception("Error in populating total weight. Exception is {}".format(e))
    else:
        logging.info("Populated total weight successfully")
        return df


def main(path: list):
    df = load_csv(path[0])
    df = populate_total_weight(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    path = ["C:\\Users\\....\\ingredient_quantities.csv"]
    main(path)
