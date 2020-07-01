function OnUpdate(doc, meta) {
  var req = {"id": meta.id + "_Replace"};
  var {success, error} = couchbase.replace(dst_bucket, req, doc);
  if(!success) {
    if(error.key_not_found && error.name == "LCB_KEY_ENOENT") {
      dst_bucket[req.id] = 'success';
    }
  }
}
