function OnUpdate(doc, meta) {
  var expiry = new Date();
  expiry.setSeconds(expiry.getSeconds() + 10);

  var req = {"id": meta.id, "expiry_date": expiry};
  couchbase.upsert(dst_bucket, req, 'success');
}
