function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  var meta = { "id": "1" };
  dst_bucket[meta.id] = {};

  var result = couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.insert(
  "user_xattrs", { "testField": "insert" }, { "xattrs": true })
  ]);
  if (!result.success) {
    log(result);
    return;
  }

  result = couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.upsert(
  "user_xattrs", { "testField": "upsert" }, { "xattrs": true })
  ]);
  if (!result.success) {
    log(result);
    return;
  }

  result = couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.replace(
    "user_xattrs", { "testField": "replace" }, { "xattrs": true })
  ]);
  if (!result.success) {
    log(result);
    return;
  }

  result = couchbase.mutateIn(dst_bucket, meta, [
    couchbase.MutateInSpec.upsert("user_xattrs.arrayTest",
  [], { "create_path": true, "xattrs": true }),
    couchbase.MutateInSpec.arrayAppend("user_xattrs.arrayTest",
    2, { "create_path": true, "xattrs": true }),
    couchbase.MutateInSpec.arrayPrepend("user_xattrs.arrayTest",
    1, { "create_path": true, "xattrs": true }),
    couchbase.MutateInSpec.arrayInsert("user_xattrs.arrayTest[0]",
    0, { "create_path": true, "xattrs": true }),
    couchbase.MutateInSpec.arrayAddUnique("user_xattrs.arrayTest",
    3, { "create_path": true, "xattrs": true })
  ]);
  if (!result.success) {
    log(result);
    return;
  }

  // Check values of XATTR operations done above
  result = couchbase.lookupIn(dst_bucket, meta, [
    couchbase.LookupInSpec.get("user_xattrs.arrayTest", { "xattrs": true }),
    couchbase.LookupInSpec.get("user_xattrs.testField", { "xattrs": true })
  ]);
  if (!result.success) {
    log(result);
    return;
  }

  var array = result.doc[0].value;
  var success = result.doc[0].success;
  if (array.length !== 4 || success === false) {
    log(result);
    return;
  }
  for (var i = 0; i < 4; i++) {
    if (array[i] !== i) {
      log(result);
      return;
    }
  }
  var test_field = result.doc[1].value;
  success = result.doc[1].success;
  if (test_field !== "replace" || success === false) {
    log(result);
    return;
  }

  delete dst_bucket[meta.id];
}
