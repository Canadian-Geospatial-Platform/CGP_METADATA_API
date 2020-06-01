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

export async function search(event) {
  try {
    // query variables
    let qVars = {
      l3: {
        rowNumber: {
          start: "",
          end: "",
        },
      },
      l2: {
        where: "",
        select: {
          start: "select *, COUNT(*) OVER() AS totalresultcount from (",
          end: ") ",
        },
      },
      l1: {
        select:
          "SELECT CAST(json_extract(\"cgp_metadata_search_dev\".properties, '$.id') AS VARCHAR) AS id ",
        from: 'FROM "cgp_metadata_search_dev" ',
        join: "",
        where: "",
        groupBy:
          "GROUP BY json_extract(\"cgp_metadata_search_dev\".properties, '$.id'), geometry, properties ",
        having: "",
        orderBy:
          "ORDER BY CAST(json_extract(\"cgp_metadata_search_dev\".properties, '$.id') AS VARCHAR) ASC ",
      },
      joinFlags: { l2_tags: false, l2_resources: false },
      nestedJsonPaths: [],
    };

    if (event.queryStringParameters) {
      if (event.queryStringParameters.select)
        applySelect(qVars, JSON.parse(event.queryStringParameters.select));
      if (event.queryStringParameters.id)
        filterOnId(qVars, JSON.parse(event.queryStringParameters.id));
      if (event.queryStringParameters.regex)
        filterOnRegex(qVars, JSON.parse(event.queryStringParameters.regex));
      if (event.queryStringParameters.tags)
        filterOnTags(qVars, JSON.parse(event.queryStringParameters.tags));
      if (event.queryStringParameters.themes)
        filterOnThemes(qVars, JSON.parse(event.queryStringParameters.themes));
      if (
        event.queryStringParameters.minRN &&
        event.queryStringParameters.maxRN
      )
        filterOnRowNumber(
          qVars,
          event.queryStringParameters.minRN,
          event.queryStringParameters.maxRN
        );
    }
    selectTheme(qVars);
    applyJoinFlags(qVars);

    var myQuery = "";
    myQuery = {
      sql:
        qVars.l3.rowNumber.start +
        qVars.l2.select.start +
        qVars.l1.select +
        qVars.l1.from +
        qVars.l1.join +
        qVars.l1.where +
        qVars.l1.groupBy +
        qVars.l1.having +
        qVars.l1.orderBy +
        qVars.l2.select.end +
        qVars.l2.where +
        qVars.l3.rowNumber.end,
      db: "meta_combined",
    };

    let results = await athenaExpress.query(myQuery);

    parseJsonFields(qVars.nestedJsonPaths, results);
    results.query = myQuery;
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

/**
 * @input qVars the shared data used to construct the query
 * @input min minimum row number of the results to be returned
 * @input max maximum row number of the results to be returned
 * @post the results returned will have a row number between min and max
 */
function filterOnRowNumber(qVars, min, max) {
  qVars.l3.rowNumber.start =
    "SELECT * FROM (SELECT row_number() over() AS rowNumber, * FROM ( ";
  qVars.l3.rowNumber.end =
    ")) WHERE rowNumber BETWEEN " + min + " AND " + max + " ";
}

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

/**
 * @input qVars the shared data used to construct the query
 * @input fields an array containing the field to return XOR the keyword "count"
 * @post if an array of fields was passed, their values will be added to the result.
 * if the "count" keyword was passed, the total result count will instead be returned.
 */
function applySelect(qVars, fields) {
  fields.forEach((e) => {
    selectProperty(qVars, e);
  });
}

function filterOnRegex(qVars, regexes) {
  regexes.forEach((e) => {
    applySimpleRegexToJsonField(qVars, e);
  });
}

/**
 * @pre selectTheme must be called to add the required theme field. selectTheme
 * can be done before or after calling this function
 * @input qVars the shared data used to construct the query
 * @input themes ARRAY[string] to match for filtering
 * @post The result will be filtered on the theme field based on mappings of
 * "l2_metadata".theme and topiccategory. (See: selectTheme function).
 */
function filterOnThemes(qVars, themes) {
  let content = "regexp_like(combinedtheme, '(?i)" + themes.join("|") + "')";
  qVars.l2.where = queryString(qVars.l2.where, content, "WHERE", "AND");
}

/**
 * @input qVars the shared data used to construct the query
 * @input regex the query object containing a list containing the regex to apply
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
  let keyword = "AND (";
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
      qVars.l1.where = queryString(qVars.l1.where, content, "WHERE (", keyword);
    }
    keyword = "OR";
  });
  qVars.l1.where += ") ";
}

/**
 * @input qVars the shared data used to construct the query
 * @input Id an array of id's of values to be returned of the resource to return
 * @post filters based on a given Id
 */
function filterOnId(qVars, id) {
  if (!id.length) return;
  let content = "contains(ARRAY[";
  let firstElementFlag = true;
  id.forEach((e) => {
    if (!firstElementFlag) content += ", ";
    content += "'" + e + "'";
    firstElementFlag = false;
  });
  content +=
    "], CAST(json_extract(\"cgp_metadata_search_dev\".properties, '$.id') AS VARCHAR))";
  qVars.l1.where = queryString(qVars.l1.where, content, "WHERE", "AND");
}

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

/**
 * @input qVars the shared data used to construct the query
 * @input tags an array of strings to match with associated tags
 *
 * @post results will be filtered based on their tags
 */
function filterOnTags(qVars, tags = []) {
  qVars.joinFlags.l2_tags = true;
  tags.forEach((e) => {
    let content = 'contains(array_agg("l2_tags".title), \'' + e + "')";
    qVars.l1.having = queryString(qVars.l1.having, content, "HAVING", "AND");
  });
}

/**
 * @input qVars the shared data used to construct the query
 *
 * @post a tag array will be added to the query results
 */
function selectTags(qVars) {
  qVars.joinFlags.l2_tags = true;
  qVars.l1.select += ', CAST(array_agg("l2_tags".title) AS JSON) AS tags ';
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
  if (qVars.joinFlags.l2_resources) {
    joinL2Resources(qVars);
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

  qVars.l1.join = queryString(
    qVars.l1.join,
    joinTagResourceRelations,
    "LEFT JOIN",
    "LEFT JOIN"
  );
  qVars.l1.join = queryString(
    qVars.l1.join,
    joinTags,
    "LEFT JOIN",
    "LEFT JOIN"
  );
}

/**
 * @input qVars the shared data used to construct the query
 *
 * @post the fields from the level 2 resources will be associated to level 1
 * resources in the query.
 */
function joinL2Resources(qVars) {
  let joinL2Resources =
    '"l2_resources" ON CAST(json_extract("cgp_metadata_search_dev".properties, \'$.id\') AS VARCHAR) = "l2_resources".id ';
  qVars.l1.join = queryString(
    qVars.l1.join,
    joinL2Resources,
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
    qVars.l1.select = queryString(qVars.l1.select, selectString, "SELECT", ",");
  } else if (splitPath[0] == "tags") {
    qVars.nestedJsonPaths.push(path);
    selectTags(qVars);
  } else if (
    ["popularityindex", "title", "description", "resourceurl"].includes(
      splitPath[0]
    )
  ) {
    selectFromSql(qVars, "l2_resources", splitPath[0]);
  }
}

/**
 * This function adds a custom field to the resulting dataset. It's reasoning
 * is the following:
 *
 * 1. If "l2_resources".theme is not null, return the content of
 * "l2_resources".theme.
 * 2. If "l2_resources".theme is null, take the content of
 * properties.topiccategory and apply the following mapping:
 *
 * +----------------------------------------------------------+
 * | topiccategory(ISO)                 | combinedtheme(CGP)  |
 * +----------------------------------+-----------------------+
 * | Boundaries                         | Administration      |
 * | Planning Cadastre                  | Administration      |
 * | Economy                            | Economy             |
 * | Farming                            | Economy             |
 * | Biota                              | Environment         |
 * | climate & environment              | Environment         |
 * | Climatology Meteorology Atmosphere | Environment         |
 * | elevation                          | Environment         |
 * | environment                        | Environment         |
 * | Inland Waters                      | Environment         |
 * | oceans                             | Environment         |
 * | Imagery BaseMaps Earth Cover       | Imagery             |
 * | Structure                          | Infrastructure      |
 * | Transportation                     | Infrastructure      |
 * | Utilities Communication            | Infrastructure      |
 * | Geoscientific Information          | Science             |
 * | Location                           | Science             |
 * | Health                             | Society             |
 * | Intelligence Military              | Society             |
 * | Society                            | Society             |
 * +----------------------------------------------------------+
 * | The two following values can only exist in theme from    |
 * | l2_metadata                                              |
 * +----------------------------------------------------------+
 * | -                                  | Legal               |
 * | -                                  | Emergency           |
 * +----------------------------------------------------------+
 *
 * @input qVars the shared data used to construct the query
 * @post a theme field will be added to the query.
 *
 * */
function selectTheme(qVars) {
  qVars.joinFlags.l2_resources = true;
  let selectString =
    "CASE " +
    'WHEN "l2_resources".theme IS NOT NULL THEN ' +
    '"l2_resources".theme ' +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(Boundaries|PlanningCadastre)') THEN " +
    "'Administration'" +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(Economy|Farming)') THEN " +
    "'Economy'" +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(Biota|Climate & Environment|ClimatologyMeteorologyAtmosphere|Elevation|Environment|InlandWaters|Oceans)') THEN " +
    "'Environment'" +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(ImageryBaseMapsEarthCover)') THEN " +
    "'Imagery'" +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(Structure|Transportation|UtilitiesCommunication)') THEN " +
    "'Infrastructure'" +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(GeoscientificInformation|Location)') THEN " +
    "'Science'" +
    "WHEN regexp_like(CAST(json_extract(\"properties\", '$.topiccategory') AS VARCHAR), '(?i)(Health|IntelligenceMilitary|Society)') THEN " +
    "'Society'" +
    "END as combinedtheme";

  qVars.l1.select = queryString(qVars.l1.select, selectString, "SELECT", ",");
  qVars.l1.groupBy = queryString(
    qVars.l1.groupBy,
    '"l2_resources".theme',
    "GROUP BY",
    ","
  );
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
  qVars.l1.select = queryString(
    qVars.l1.select,
    selectAndGroupByString,
    "SELECT",
    ","
  );
  qVars.l1.groupBy = queryString(
    qVars.l1.groupBy,
    selectAndGroupByString,
    "GROUP BY",
    ","
  );
}

export default { search };
