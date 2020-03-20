const _ = require("lodash");
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
        'SELECT CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) AS id, array_agg("l2_tags".title) AS tags ',
      from: 'FROM  "cgp_metadata_search_dev" ',
      join: "",
      where: "",
      groupBy:
        "GROUP BY json_extract(\"cgp_metadata_search_dev\".properties, '$.id'), geometry, properties ",
      having: ""
    };

    filterOnTags(qVars, input.tags);

    input.select.forEach(e => {
      selectProperty(qVars, e);
    });

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

    var myQuery = "";
    myQuery = {
      sql:
        qVars.select +
        qVars.from +
        qVars.join +
        qVars.where +
        qVars.groupBy +
        qVars.having +
        "limit 10",
      db: "meta_combined"
    };

    let results = await athenaExpress.query(myQuery);

    // Parse fields containing nested json
    input.select.forEach(path => {
      results.Items.forEach(item => {
        _.set(item, path, JSON.parse(_.get(item, path, null)));
      });
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

function queryBuilder(baseString, content, keyword, separator) {
  let ret = baseString;
  if (!baseString.includes(content)) {
    if (baseString) ret += separator + " ";
    else ret += keyword + " ";
    ret += content + " ";
  }
  return ret;
}

function filterOnTags(qVars, tags = []) {
  joinTags(qVars);
  tags.forEach(e => {
    let queryString = 'contains(array_agg("l2_tags".title), \'' + e + "')";
    qVars.having = queryBuilder(qVars.having, queryString, "HAVING", "AND");
  });
}

function joinTags(qVars) {
  let joinTagResourceRelations =
    '"l2_relations_container_tag_resource" ON CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) = "l2_relations_container_tag_resource".resourceid ';

  let joinTags = '"l2_tags" ON tagid = "l2_tags".id';

  qVars.join = queryBuilder(
    qVars.join,
    joinTagResourceRelations,
    "LEFT JOIN",
    "LEFT JOIN"
  );
  qVars.join = queryBuilder(qVars.join, joinTags, "LEFT JOIN", "LEFT JOIN");
}

function selectProperty(qVars, path) {
  let splitPath = path.split(".");
  let selectString =
    'json_extract("cgp_metadata_search_dev".' +
    splitPath.shift() +
    ", '$." +
    splitPath.join(".") +
    "') AS \"" +
    path +
    '"';
  qVars.select = queryBuilder(qVars.select, selectString, "SELECT", ",");
}

export default { getAll };
