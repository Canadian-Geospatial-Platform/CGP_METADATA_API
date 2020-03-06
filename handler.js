import flatten from "flat";
const AthenaExpress = require("athena-express");
const aws = require("aws-sdk");

const athenaExpressConfig = {
  aws,
  s3: "s3://cgp-metadata-search-dev-athena-output",
  getStats: true,
  formatJson: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

export async function getAll(event) {
  try {
    let input = JSON.parse(event.body);
    const allowed = ["properties"];
    Object.keys(input)
      .filter(key => !allowed.includes(key))
      .forEach(key => delete input[key]);
    let where = "";

    const flattenedInput = flatten(input);
    for (let key in flattenedInput) {
      if (where) {
        where += " AND ";
      }
      where += "regexp_like(" + key + ",'" + flattenedInput[key] + "')";
    }

    let myQuery = {
      sql: "SELECT * FROM metadata WHERE " + where + " limit 10;",
      db: "cgp-metadata-search-dev"
    };

    let results = await athenaExpress.query(myQuery);

    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*"
      },
      body: JSON.stringify(results)
    };
  } catch (err) {
    return {
      statusCode: err.statusCode || 500,
      headers: {
        "Content-Type": "text/plain",
        "Access-Control-Allow-Origin": "*"
      },
      body: err.message || "Could not fetch results."
    };
  }
}

export default { getAll };
