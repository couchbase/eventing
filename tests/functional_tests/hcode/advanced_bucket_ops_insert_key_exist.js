function OnUpdate(doc, meta) {
  var req = {"id": meta.id};
  var {success, error} = couchbase.insert(dst_bucket, req, 'success');
  if(!success && error.key_already_exists) {
    delete dst_bucket[meta.id];
  }
}
