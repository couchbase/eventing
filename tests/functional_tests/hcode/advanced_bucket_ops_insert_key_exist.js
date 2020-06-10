function OnUpdate(doc, meta) {
  var req = {"id": meta.id};
  var {success, error} = couchbase.insert(dst_bucket, req, 'success');
  if(!success) {
    if(error.key_already_exists && error.name == "LCB_KEY_EEXISTS") {
      delete dst_bucket[meta.id];
    }
  }
}
