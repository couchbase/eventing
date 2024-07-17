function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  var meta = { "id": "1" };
  dst_bucket[meta.id] = {};

  couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.insert("testField", "insert")
  ]);
  var doc = dst_bucket[meta.id];
  if (doc.testField != "insert") {
    log(doc);
    return;
  }

  couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.replace("testField", "replace")
  ]);
  doc = dst_bucket[meta.id];
  if (doc.testField != "replace") {
    log(doc);
    return;
  }

  couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.remove("testField")
  ]);
  doc = dst_bucket[meta.id];
  if ("testField" in doc) {
    log(doc);
    return;
  }

  couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.upsert("arrayTest", []),
    couchbase.MutateInSpec.arrayAppend("arrayTest", 2),
    couchbase.MutateInSpec.arrayPrepend("arrayTest", 1),
    couchbase.MutateInSpec.arrayInsert("arrayTest[0]", 0),
    couchbase.MutateInSpec.arrayAddUnique("arrayTest", 3)
  ]);
  doc = dst_bucket[meta.id];
  var array = doc["arrayTest"];

  if (array.length != 4) {
    log(doc);
    return;
  }

  for (var i = 0; i < 4; i++) {
    if (array[i] != i) {
      log(doc);
      return;
    }
  }
  delete dst_bucket[meta.id];
}
