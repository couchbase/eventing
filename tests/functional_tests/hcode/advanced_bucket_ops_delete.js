function OnUpdate(doc, meta) {
  var del_meta = {"id": meta.id};
  couchbase.delete(dst_bucket, del_meta);
}
