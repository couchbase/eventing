function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  var meta = { "id": "1" };

  // ADVANCED GET
  dst_bucket[meta.id] = "Created doc";
  var res = couchbase.get(dst_bucket, meta);
  delete dst_bucket[meta.id];
  if (res.success) {
    dst_bucket[meta.id + "_get"] = "success";
  }

  // ADVANCED INSERT
  var req = { "id": meta.id + "_insert" };
  res = couchbase.insert(dst_bucket, req, "insert");
  var doc = dst_bucket[req.id];
  if (!res.success || doc !== "insert") {
    return;
  }

  // ADVANCED UPSERT
  req = { "id": meta.id + "_upsert" };
  res = couchbase.upsert(dst_bucket, req, "upsert");
  doc = dst_bucket[req.id];
  if (!res.success || doc !== "upsert") {
    return;
  }

  // ADVANCED REPLACE
  req = { "id": meta.id + "_replace" };
  dst_bucket[req.id] = "Created doc";
  res = couchbase.replace(dst_bucket, req, "replace");
  doc = dst_bucket[req.id];
  if (!res.success || doc !== "replace") {
    return;
  }

  // ADVANCED DELETE
  req = { "id": meta.id + "_delete" };
  dst_bucket[req.id] = "Created doc";
  res = couchbase.delete(dst_bucket, req);
  if (res.success) {
    dst_bucket[req.id] = "success";
  }
}
