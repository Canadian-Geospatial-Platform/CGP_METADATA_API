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
  let input = {
    properties: {
      title: {
        en: "(?i).*Seismic.*",
        fr: "(?i).*Eau.*"
      }
    }
  };

  let flatInput = input.entries();

  let myQuery = {
    sql:
      "SELECT * FROM metadata WHERE regexp_like(properties.title.en,'(?i).*Seismic.*') limit 10;",
    db: "cgp-metadata-search-dev"
  };

  try {
    // let results = await athenaExpress.query(myQuery);
    return {
      statusCode: 200,
      body: JSON.stringify(flatInput)
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
