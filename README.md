# s3-api

An API route that allows advanced filtering of data comming from both level 2
metadata coming from a traditional database and data and level 1 metadata from
geojson files in an s3 bucket.

## Glossary

- Level 1 metadata: Foundational data, that cannot be changed and is stored as
  geojson files in a bucket.
- Level 2 metadata: Data stored in a database that is added as an extra layer of
  data to the level 1 metadata. This data can evolve as time passes.

## Requirements

- [Node.js](https://nodejs.org/en/) - version: ^13.5
- [aws-cli](https://aws.amazon.com/cli/) - version : ^1.16.218
- [serverless-cli](https://serverless.com/) - version: ^1.67

## Installation

- Install nodejs
- Install and configure you serverless cli with your aws programmatic login credentials
- Install npm dependencies

```BASH
npm i
```

- Deploy the project

```BASH
sls deploy
```

- test using postman and sample data from [Sample Query](###Sample-Query)

## Usage

This api is vaguely inspired by graphql rather than the usual RESTful API style.
This means that the consumer is expected to describe the data he desires to
receive in the body of his query to slim down the amount of data transferred to
him. He is also expected to send in filters to apply to the data in the backend
using [regex](https://en.wikipedia.org/wiki/Regular_expression) pattern matching.

Path is described using the dot notation path to the field in the javascript
object returned by the database.

### Sample Query

- Query type: GET
- Sample url: https://zq7vthl3ye.execute-api.ca-central-1.amazonaws.com/sta/geo?regex=(?i).*water.*&select=["properties.description", "properties.title","tags" ]&tags=["air", "water" ]

### Sample Result

```JSON
{
    "Items": [
        {
            "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "properties": {
                "title": {
                    "en": "xxx",
                    "fr": "xxx"
                },
                "description": {
                    "en": "xxx",
                    "fr": "xxx"
                }
            },
            "tags": [
                "xxx",
                "xxx"
            ],
            "popularityindex": "111"
        },
        {
            "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "properties": {
                "title": {
                    "en": "xxx",
                    "fr": "xxx"
                },
                "description": {
                    "en": "xxx",
                    "fr": "xxx"
                }
            },
            "tags": [
                "xxx",
                "xxx xxx",
                "xxx",
                "xxx"
            ],
            "popularityindex": "111"
        }
    ],
    "DataScannedInMB": 8,
    "QueryCostInUSD": 0.00004768,
    "EngineExecutionTimeInMillis": 1589,
    "Count": 2,
    "QueryExecutionId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "S3Location": "s3://xxx-xxx-xxx/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.csv"
}
```

### Updating the level 2 metadata

Since this system uses _Amazon Athena_ to query data. The data to be queried
must be placed in a bucket. To this end, we use aws glue to infer the schema of
our level 2 data and, then, use an aws glue job to transfer the data to an
amazon s3 bucket.

From the AWS console under services/AWS glue:

1. If one is not yet created, create a connection to your level 2 metadata
   database.
2. If one is not yet created, create a database in glue to store your tables.
3. create and run a crawler for each level 2 table that you wish to query with
   athena.
4. Create and an etl job using the schemas generated on step 3. As a data target,
   choose s3 as your data store, parquet as the format and point to the desired
   storage location
5. Your data should be updated. You can put jobs on a timer to automate this
   process.