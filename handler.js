const AthenaExpress = require("athena-express");
const aws = require("aws-sdk");

const athenaExpressConfig = {
  aws,
  s3: "s3://cgp-metadata-search-dev",
  getStats: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

//Invoking a query on Amazon Athena
export async function getAll(event) {
  let myQuery = {
    sql: "SELECT type, geometry, properties FROM metadata LIMIT 3",
    db: "cgp-metadata-search-dev"
  };

  try {
    let results = await athenaExpress.query(myQuery);
    console.log(results);
  } catch (error) {
    console.log(error);
  }
}

export default { getAll };
