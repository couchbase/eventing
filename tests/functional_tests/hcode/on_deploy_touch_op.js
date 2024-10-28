function OnUpdate(doc, meta) {}

function OnDelete(meta, options) {}

function OnDeploy(action) {
  var meta = { "id": "1" };

  dst_bucket[meta.id] = "Created doc";
  var expiry = new Date();
  expiry.setSeconds(expiry.getSeconds() + 10);
  var req = { "id": meta.id, "expiry_date": expiry };
  // The document should expire after 10 seconds if touch operation was successful
  couchbase.touch(dst_bucket, req);
}
