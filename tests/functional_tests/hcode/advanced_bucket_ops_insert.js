function OnUpdate(doc, meta) {
  var req = {"id": meta.id+"_Handler"};
  couchbase.insert(dst_bucket, req, 'success');
}
