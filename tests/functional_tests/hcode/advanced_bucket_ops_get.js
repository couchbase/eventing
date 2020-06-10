function OnUpdate(doc, meta) {
  var {success} = couchbase.get(src_bucket, meta);
  if(success) {
    dst_bucket[meta.id] = 'success';
  }
}
