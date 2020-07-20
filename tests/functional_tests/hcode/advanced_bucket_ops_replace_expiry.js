function OnUpdate(doc, meta) {
  dst_bucket[meta.id] = doc;

  var expiry = new Date();
  expiry.setSeconds(expiry.getSeconds() + 10);
  var req = {"id": meta.id, "expiry_date": expiry};
  couchbase.replace(dst_bucket, req, 'success');
}
