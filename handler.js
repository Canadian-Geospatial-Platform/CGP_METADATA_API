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
    let join = "";
    input.where.forEach(e => {
      let base = e.path.split(".")[0];
      let rest = e.path
        .split(".")
        .slice(1)
        .join(".");
      let queryContent =
        "regexp_like(CAST(json_extract(" +
        base +
        ", '$." +
        rest +
        "') AS VARCHAR), '" +
        e.regex +
        "')";
      where = queryBuilder(where, queryContent, "WHERE", "AND");
    });

    let joinContent =
      '"l2_relations_container_tag_resource" ON CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) = "l2_relations_container_tag_resource".resourceid LEFT JOIN "l2_tags" ON tagid = "l2_tags".id';

    join = queryBuilder(join, joinContent, "LEFT JOIN");

    var myQuery = "";
    myQuery = {
      sql:
        'SELECT json_extract("cgp_metadata_search_dev".properties, \'$.id\') as Id, array_agg("l2_tags".title) as tags FROM  "cgp_metadata_search_dev" ' +
        join +
        where +
        "GROUP BY json_extract(\"cgp_metadata_search_dev\".properties, '$.id'), geometry, properties",
      db: "meta_combined"
    };

    let results = await athenaExpress.query(myQuery);

    // Parse fields containing nested json
    // results.Items.forEach(e => {
    //   e.geometry = JSON.parse(e.geometry);
    //   e.properties = JSON.parse(e.properties);
    // });

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

function queryBuilder(baseString, content, keyword, separator) {
  let ret = baseString;
  if (!baseString.includes(content)) {
    if (baseString) ret += separator + " ";
    else ret += keyword + " ";
    ret += baseString + content + " ";
  }
  return ret;
}

export default { getAll };
