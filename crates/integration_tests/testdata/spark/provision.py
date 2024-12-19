# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_add, expr

spark = (
    SparkSession
        .builder
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

spark.sql(
    f"""
INSERT INTO rest.default.test_positional_merge_on_read_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_deletes WHERE number = 9")

spark.sql(
    f"""
  CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_double_deletes (
    dt     date,
    number integer,
    letter string
  )
  USING iceberg
  TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
  );
"""
)

spark.sql(
    f"""
INSERT INTO rest.default.test_positional_merge_on_read_double_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

#  Creates two positional deletes that should be merged
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE number = 9")
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE letter == 'f'")

#  Create a table, and do some renaming
spark.sql("CREATE OR REPLACE TABLE rest.default.test_rename_column (lang string) USING iceberg")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Python')")
spark.sql("ALTER TABLE rest.default.test_rename_column RENAME COLUMN lang TO language")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Java')")

#  Create a table, and do some evolution
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_column (foo int) USING iceberg")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (19)")
spark.sql("ALTER TABLE rest.default.test_promote_column ALTER COLUMN foo TYPE bigint")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (25)")
