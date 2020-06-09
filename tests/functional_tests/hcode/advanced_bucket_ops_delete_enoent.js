function OnUpdate(doc, meta) {
  var req = {"id": meta.id+"_Handler"};
  var {success, error} = couchbase.delete(dst_bucket, req);
  if(!success) {
    if (error.key_not_found && error.name == "LCB_KEY_ENOENT") {
      dst_bucket[req.id] = 'success';
    }
  }
}
