const AthenaExpress = require("athena-express");
const aws = require("aws-sdk");

const athenaExpressConfig = {
  aws,
  s3: "s3://cgp-metadata-search-dev-athena-output",
  getStats: true,
  formatJson: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

//Invoking a query on Amazon Athena
export async function getAll(event) {
  // let Bbox =
  // '{type=Polygon, coordinates=[[["-127.737823","52.127108"],["-102.774389","52.127108"],["-102.774389","65.165894"],["-127.737823","65.165894"],["-127.737823","52.127108"]]]}';
  let myQuery = {
    sql:
      "SELECT * FROM metadata where properties.id = '05870f11-a52a-4bf4-bc15-910fd0b8a1a3' limit 10",
    db: "cgp-metadata-search-dev"
  };

  try {
    let results = await athenaExpress.query(myQuery);
    return {
      statusCode: 200,
      body: JSON.stringify(results)
    };
  } catch (err) {
    return {
      statusCode: err.statusCode || 500,
      headers: { "Content-Type": "text/plain" },
      body: err.message || "Could not fetch results."
    };
  }
}

export default { getAll };
