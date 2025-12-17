function OnUpdate(doc, meta) {
  try {
  log("Match query");
  var matchQuery = couchbase.SearchQuery.match("location hostel").operator(
      "and").field("reviews.content").analyzer("standard").fuzziness(2)
    .prefixLength(4);
  runQuery(matchQuery, { "size": 5 }, 5);

  log("Match phase query");
  var matchPhaseQuery = couchbase.SearchQuery.matchPhrase("very nice").field(
    "reviews.content");
  runQuery(matchPhaseQuery, { "size": 5 }, 5);

  log("regex query");
  var regexQuery = couchbase.SearchQuery.regexp("inter.+").field(
    "reviews.content");
  runQuery(regexQuery, { "size": 5 }, 5);

  log("query string");
  var queryStringQuery = couchbase.SearchQuery.queryString("Cleanliness");
  runQuery(queryStringQuery, { "size": 5 }, 5);

  log("numeric range");
  var numericRangeQuery = couchbase.SearchQuery.numericRange().field(
    "reviews.ratings.Cleanliness").min(4, true);
  runQuery(numericRangeQuery, { "size": 5 }, 5);

  log("conjucts query");
  var matchQueryForCompound = couchbase.SearchQuery.match("location").field(
    "reviews.content");
  var booleanQueryForCompound = couchbase.SearchQuery.booleanField(true).field(
    "free_breakfast");
  var conjunctsQuery = couchbase.SearchQuery.conjuncts(matchQueryForCompound,
    booleanQueryForCompound);
  runQuery(conjunctsQuery, { "size": 5 }, 5);

  log("disjuncts query");
  var disjunctsQuery = couchbase.SearchQuery.disjuncts(matchQueryForCompound)
    .or(booleanQueryForCompound);
  runQuery(disjunctsQuery, { "size": 5 }, 5);

  log("boolean query");
  var mustQuery = couchbase.SearchQuery.conjuncts(matchQueryForCompound);
  var mustNotQuery = couchbase.SearchQuery.disjuncts(couchbase.SearchQuery
    .booleanField(false).field("free_breakfast"));
  var shouldQuery = couchbase.SearchQuery.disjuncts(booleanQueryForCompound);
  var booleanQuery = couchbase.SearchQuery.boolean().must(mustQuery).should(
    shouldQuery).mustNot(mustNotQuery);
  runQuery(booleanQuery, { "size": 5 }, 5);

  log("wildcard query");
  var wildcardQuery = couchbase.SearchQuery.wildcard("inter*").field(
    "reviews.content");
  runQuery(wildcardQuery, { "size": 5 }, 5);

  log("docIds query");
  var docIdsQuery = couchbase.SearchQuery.docIds("airport_8850",
  "airport_8851");
  runQuery(docIdsQuery, {}, 2);

  log("boolean field query");
  runQuery(booleanQueryForCompound, { "size": 5 }, 5);

  log("term query");
  var termQuery = couchbase.SearchQuery.term("locate").field("reviews.content");
  runQuery(termQuery, { "size": 5 }, 5);

  log("phrase query");
  var phraseQuery = couchbase.SearchQuery.phrase("nice", "view").field(
    "reviews.content");
  runQuery(phraseQuery, { "size": 5 }, 5);

  log("prefix query");
  var prefixQuery = couchbase.SearchQuery.prefix("inter").field(
    "reviews.content");
  runQuery(prefixQuery, { "size": 5 }, 5);

  log("matchAll query");
  var matchAllQuery = couchbase.SearchQuery.matchAll();
  runQuery(matchAllQuery, { "size": 5 }, 5);

  log("matchNone query");
  var matchNoneQuery = couchbase.SearchQuery.matchNone();
  runQuery(matchNoneQuery, { "size": 5 }, 0);

  dst_bucket[meta.id] = 'yes';
} catch(e) {
  log(e);
}
}

function runQuery(query, options, expected) {
  var it = couchbase.searchQuery("travel-sample._default.travel_sample_test",
    query, options);
  var count = 0;
  for (let row of it) {
    count++;
  }
  if (count != expected) {
    throw "not expected result;"
  }
}
