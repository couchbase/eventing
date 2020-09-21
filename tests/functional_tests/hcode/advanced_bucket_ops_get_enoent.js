function OnUpdate(doc, meta) {
  var req = {"id": meta.id + "_Handler"};
  var {success, error} = couchbase.get(dst_bucket, req);
  if(!success && error.key_not_found) {
    dst_bucket[req.id] = 'success';
  }
}
