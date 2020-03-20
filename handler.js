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

    // query variables
    let qVars = {
      select:
        'SELECT json_extract("cgp_metadata_search_dev".properties, \'$.id\') as id, array_agg("l2_tags".title) as tags FROM  "cgp_metadata_search_dev" ',
      join: "",
      where: "",
      groupBy:
        "GROUP BY json_extract(\"cgp_metadata_search_dev\".properties, '$.id'), geometry, properties ",
      having: "HAVING contains(array_agg(\"l2_tags\".title), 'water') limit 10"
    };

    input.regex.forEach(e => {
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
      qVars.where = queryBuilder(qVars.where, queryContent, "WHERE", "AND");
    });

    let joinContent1 =
      '"l2_relations_container_tag_resource" ON CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) = "l2_relations_container_tag_resource".resourceid ';

    let joinContent2 = '"l2_tags" ON tagid = "l2_tags".id';

    qVars.join = queryBuilder(
      qVars.join,
      joinContent1,
      "LEFT JOIN",
      "LEFT JOIN"
    );
    qVars.join = queryBuilder(
      qVars.join,
      joinContent2,
      "LEFT JOIN",
      "LEFT JOIN"
    );

    var myQuery = "";
    myQuery = {
      sql:
        qVars.select + qVars.join + qVars.where + qVars.groupBy + qVars.having,
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
    ret += content + " ";
  }
  return ret;
}

export default { getAll };
