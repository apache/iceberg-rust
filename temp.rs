r#"
        {
            "format-version" : 1,
            "table-uuid" : "df838b92-0b32-465d-a44e-d39936e538b7",
            "location" : "/home/iceberg/warehouse/nyc/taxis",
            "last-updated-ms" : 1662532818843,
            "last-column-id" : 5,
            "schema" : {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            },
            "current-schema-id" : 0,
            "schemas" : [ {
              "type" : "struct",
              "schema-id" : 0,
              "fields" : [ {
                "id" : 1,
                "name" : "vendor_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 2,
                "name" : "trip_id",
                "required" : false,
                "type" : "long"
              }, {
                "id" : 3,
                "name" : "trip_distance",
                "required" : false,
                "type" : "float"
              }, {
                "id" : 4,
                "name" : "fare_amount",
                "required" : false,
                "type" : "double"
              }, {
                "id" : 5,
                "name" : "store_and_fwd_flag",
                "required" : false,
                "type" : "string"
              } ]
            } ],
            "partition-spec" : [ {
              "name" : "vendor_id",
              "transform" : "identity",
              "source-id" : 1,
              "field-id" : 1000
            } ],
            "default-spec-id" : 0,
            "partition-specs" : [ {
              "spec-id" : 0,
              "fields" : [ {
                "name" : "vendor_id",
                "transform" : "identity",
                "source-id" : 1,
                "field-id" : 1000
              } ]
            } ],
            "last-partition-id" : 1000,
            "default-sort-order-id" : 0,
            "sort-orders" : [ {
              "order-id" : 0,
              "fields" : [ ]
            } ],
            "properties" : {
              "owner" : "root"
            },
            "current-snapshot-id" : 638933773299822130,
            "refs" : {
              "main" : {
                "snapshot-id" : 638933773299822130,
                "type" : "branch"
              }
            },
            "snapshots" : [ {
              "snapshot-id" : 638933773299822130,
              "timestamp-ms" : 1662532818843,
              "summary" : {
                "operation" : "append",
                "spark.app.id" : "local-1662532784305",
                "added-data-files" : "4",
                "added-records" : "4",
                "added-files-size" : "6001",
                "changed-partition-count" : "2",
                "total-records" : "4",
                "total-files-size" : "6001",
                "total-data-files" : "4",
                "total-delete-files" : "0",
                "total-position-deletes" : "0",
                "total-equality-deletes" : "0"
              },
              "manifest-list" : "/home/iceberg/warehouse/nyc/taxis/metadata/snap-638933773299822130-1-7e6760f0-4f6c-4b23-b907-0a5a174e3863.avro",
              "schema-id" : 0
            } ],
            "snapshot-log" : [ {
              "timestamp-ms" : 1662532818843,
              "snapshot-id" : 638933773299822130
            } ],
            "metadata-log" : [ {
              "timestamp-ms" : 1662532805245,
              "metadata-file" : "/home/iceberg/warehouse/nyc/taxis/metadata/00000-8a62c37d-4573-4021-952a-c0baef7d21d0.metadata.json"
            } ]
          }
        "#