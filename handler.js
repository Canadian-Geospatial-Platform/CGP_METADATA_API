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

    let where = "";
    input.where.forEach(e => {
      if (!where) {
        where += "WHERE";
      } else {
        where += " AND";
      }
      let base = e.path.split(".")[0];
      let rest = e.path
        .split(".")
        .slice(1)
        .join(".");
      where +=
        " regexp_like(CAST(json_extract(" +
        base +
        ", '$." +
        rest +
        "') AS VARCHAR), '" +
        e.regex +
        "')";
    });

    var myQuery = "";
    myQuery = {
      sql:
        'SELECT * FROM  "metalevelonecgp_metadata_search_dev" ' +
        where +
        " limit 10",
      db: "cgp-combined-metadata"
    };

    let results = await athenaExpress.query(myQuery);

    // Parse fields containing nested json
    results.Items.forEach(e => {
      e.geometry = JSON.parse(e.geometry);
      e.properties = JSON.parse(e.properties);
    });

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
      body: JSON.stringify(myQuery) || "Could not fetch results"
    };
  }
}

export default { getAll };
