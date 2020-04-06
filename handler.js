const _ = require("lodash");
const AthenaExpress = require("athena-express");
const aws = require("aws-sdk");

const athenaExpressConfig = {
  aws,
  s3: "s3://cgp-metadata-search-dev-athena-output",
  getStats: true,
  formatJson: true,
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

export async function simpleSearch(event) {
  try {
    // query variables
    let qVars = {
      select:
        "SELECT CAST(json_extract(\"cgp_metadata_search_dev\".properties, '$.id') AS VARCHAR) AS id ",
      from: 'FROM "cgp_metadata_search_dev" ',
      join: "",
      where: "",
      groupBy:
        "GROUP BY json_extract(\"cgp_metadata_search_dev\".properties, '$.id'), geometry, properties ",
      having: "",
      joinFlags: { l2_tags: false, l2_metadata: false },
      nestedJsonPaths: [],
    };

    applySelect(qVars, JSON.parse(event.queryStringParameters.select));
    applySimpleRegexToJsonField(qVars, event.queryStringParameters.regex);
    filterOnTags(qVars, JSON.parse(event.queryStringParameters.tags));
    applyJoinFlags(qVars);

    var myQuery = "";
    myQuery = {
      sql:
        qVars.select +
        qVars.from +
        qVars.join +
        qVars.where +
        qVars.groupBy +
        qVars.having,
      db: "meta_combined",
    };

    let results = await athenaExpress.query(myQuery);

    parseJsonFields(qVars.nestedJsonPaths, results);

    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify(results),
    };
  } catch (err) {
    return {
      statusCode: err.statusCode || 500,
      headers: {
        "Content-Type": "text/plain",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify(myQuery) || "Could not fetch results",
    };
  }
}

// export async function advancedSearch(event) {
//   try {
//     let input = JSON.parse(event.body);
//     // query variables
//     let qVars = {
//       select:
//         "SELECT CAST(json_extract(\"cgp_metadata_search_dev\".properties, '$.id') AS VARCHAR) AS id ",
//       from: 'FROM "cgp_metadata_search_dev" ',
//       join: "",
//       where: "",
//       groupBy:
//         "GROUP BY json_extract(\"cgp_metadata_search_dev\".properties, '$.id'), geometry, properties ",
//       having: "",
//       joinFlags: { l2_tags: false, l2_metadata: false },
//       nestedJsonPaths: [],
//     };

//     applySelect(qVars, input);
//     applyAdvancedRegexToJsonField(qVars, input);
//     filterOnTags(qVars, input.tags);
//     applyJoinFlags(qVars);

//     var myQuery = "";
//     myQuery = {
//       sql:
//         qVars.select +
//         qVars.from +
//         qVars.join +
//         qVars.where +
//         qVars.groupBy +
//         qVars.having,
//       db: "meta_combined",
//     };

//     let results = await athenaExpress.query(myQuery);

//     parseJsonFields(qVars.nestedJsonPaths, results);

//     return {
//       statusCode: 200,
//       headers: {
//         "Access-Control-Allow-Origin": "*",
//       },
//       body: JSON.stringify(results),
//     };
//   } catch (err) {
//     return {
//       statusCode: err.statusCode || 500,
//       headers: {
//         "Content-Type": "text/plain",
//         "Access-Control-Allow-Origin": "*",
//       },
//       body: JSON.stringify(myQuery) || "Could not fetch results",
//     };
//   }
// }

/**
 * @input nestedJsonPaths A list containing the path to json fields to parse in the results object
 * @input results the object containing a list of items returned by the athena query
 * @post the json fields in results, referenced in nestedJsonPaths are parsed
 */
function parseJsonFields(nestedJsonPaths, results) {
  nestedJsonPaths.forEach((path) => {
    results.Items.forEach((item) => {
      _.set(item, path, JSON.parse(_.get(item, path, null)));
    });
  });
}

function applySelect(qVars, fields) {
  fields.forEach((e) => {
    selectProperty(qVars, e);
  });
}

/**
 * @input qVars the shared data used to construct the query
 * @input input the query object containing a list containing the regex to apply
 * @post filters based on regex against json columns will be applied
 */
function applySimpleRegexToJsonField(qVars, regex) {
  const fields = [
    "properties.title.en",
    "properties.title.fr",
    "properties.description.en",
    "properties.description.fr",
    "properties.organisationname.en",
    "properties.organisationname.fr",
  ];
  let keyword = "AND";
  fields.forEach((e) => {
    let splitPath = e.split(".");
    if (["properties"].includes(splitPath[0])) {
      let content =
        "regexp_like(CAST(json_extract(" +
        splitPath.shift() +
        ", '$." +
        splitPath.join(".") +
        "') AS VARCHAR), '" +
        regex +
        "')";
      qVars.where = queryString(qVars.where, content, "WHERE", keyword);
    }
    keyword = "OR";
  });
}

/**
 * @input qVars the shared data used to construct the query
 * @input input the query object containing a list containing routes and regex to apply
 * @post filters based on regex against json columns will be applied
 */
// function applyAdvancedRegexToJsonField(qVars, input) {
//   input.regex.forEach((e) => {
//     let splitPath = e.path.split(".");
//     if (["properties"].includes(splitPath[0])) {
//       let content =
//         "regexp_like(CAST(json_extract(" +
//         splitPath.shift() +
//         ", '$." +
//         splitPath.join(".") +
//         "') AS VARCHAR), '" +
//         e.regex +
//         "')";
//       qVars.where = queryString(qVars.where, content, "WHERE", "AND");
//     }
//   });
// }

/**
 * @input baseString the initial string to witch an extension will be made
 * @input content the string representing the query we wish to add to the baseString
 * @input keyword the word to put before the content if it is the first element
 * of it's type. That would be SELECT, GROUP BY, JOIN, etc
 * @input separator the joining symbol between two elements of the same type of
 * content. For a select, it's a ",", for WHERE it's AND, etc.
 * @post returns a string concatenating baseString and content with the correct separator
 *
 * This function has the goal of simplifying the process of  applying the
 * proper conditional separator to parts of sql statements
 */
function queryString(baseString, content, keyword, separator) {
  let ret = baseString;
  if (baseString) ret += separator + " ";
  else ret += keyword + " ";
  ret += content + " ";
  return ret;
}

function filterOnTags(qVars, tags = []) {
  qVars.joinFlags.l2_tags = true;
  tags.forEach((e) => {
    let content = 'contains(array_agg("l2_tags".title), \'' + e + "')";
    qVars.having = queryString(qVars.having, content, "HAVING", "AND");
  });
}

function selectOnTags(qVars) {
  qVars.joinFlags.l2_tags = true;
  qVars.select += ', CAST(array_agg("l2_tags".title) AS JSON) AS tags ';
}

/**
 * @input qVars the shared data used to construct the query
 *
 * @post tables required by other queries will be joined to the request
 */
function applyJoinFlags(qVars) {
  if (qVars.joinFlags.l2_tags) {
    joinTags(qVars);
  }
  if (qVars.joinFlags.l2_tags) {
    joinL2Metadata(qVars);
  }
}

/**
 * @input qVars the shared data used to construct the query
 *
 * @post a list of tags will be associated to resources in the query. The list
 * of tags can the be used to filter and/or select
 */
function joinTags(qVars) {
  let joinTagResourceRelations =
    '"l2_relations_container_tag_resource" ON CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) = "l2_relations_container_tag_resource".resourceid ';
  let joinTags = '"l2_tags" ON tagid = "l2_tags".id';

  qVars.join = queryString(
    qVars.join,
    joinTagResourceRelations,
    "LEFT JOIN",
    "LEFT JOIN"
  );
  qVars.join = queryString(qVars.join, joinTags, "LEFT JOIN", "LEFT JOIN");
}

function joinL2Metadata(qVars) {
  let joinL2Metadata =
    '"l2_resources" ON CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) = "l2_resources".id ';
  qVars.join = queryString(
    qVars.join,
    joinL2Metadata,
    "LEFT JOIN",
    "LEFT JOIN"
  );
}

/**
 * @pre the path variable must be lowercase and nesting is described using the
 * dot notation
 *
 * @input qVars the shared data used to construct the query
 *
 * @post the field will be added to the result of the athena query
 *
 *
 * this function dispatches select routes to the correct handler depending
 * on their type
 */
function selectProperty(qVars, path) {
  let selectString = "";
  let splitPath = path.split(".");
  if (["properties"].includes(splitPath[0])) {
    qVars.nestedJsonPaths.push(path);
    if (splitPath.length > 1) {
      selectString =
        'json_extract("cgp_metadata_search_dev".' +
        splitPath.shift() +
        ", '$." +
        splitPath.join(".") +
        "') AS \"" +
        path +
        '"';
    } else {
      selectString = path;
    }
    qVars.select = queryString(qVars.select, selectString, "SELECT", ",");
  } else if (["tags"].includes(splitPath[0])) {
    qVars.nestedJsonPaths.push(path);
    selectOnTags(qVars);
  } else if (
    ["popularityindex", "title", "description", "resourceurl"].includes(
      splitPath[0]
    )
  ) {
    selectFromSql(qVars, "l2_resources", splitPath[0]);
  }
}

/**
 * @pre The field requested must be a simple text or number stored in a regular
 * sql column stored in one of the database tables.
 *
 * @post the field will be added to the result of the athena query
 *
 * @input qVars the shared data used to construct the query
 * @input tableName the name of the table containing the field requested
 * @input fieldName the name of the field that must be added to the result
 *
 */
function selectFromSql(qVars, tableName, fieldName) {
  let selectAndGroupByString = '"' + tableName + '".' + fieldName;
  qVars.joinFlags[tableName] = true;
  qVars.select = queryString(
    qVars.select,
    selectAndGroupByString,
    "SELECT",
    ","
  );
  qVars.groupBy = queryString(
    qVars.groupBy,
    selectAndGroupByString,
    "GROUP BY",
    ","
  );
}

export default { simpleSearch /**, advancedSearch*/ };
