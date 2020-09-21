function OnUpdate(doc, meta) {
  var req = {"id": meta.id + "_Replace"};
  var {success, error} = couchbase.replace(dst_bucket, req, doc);
  if(!success && error.key_not_found) {
    dst_bucket[req.id] = 'success';
  }
}
