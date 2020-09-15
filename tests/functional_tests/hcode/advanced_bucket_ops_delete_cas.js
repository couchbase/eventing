function OnUpdate(doc, meta) {
  var {meta} = couchbase.get(dst_bucket, meta);

  // Change the cas
  dst_bucket[meta.id] = 'Changed';
  var {success, error} = couchbase.delete(dst_bucket, meta);
  if(!success && error.cas_mismatch) {
    var {meta} = couchbase.get(dst_bucket, meta);
    couchbase.delete(dst_bucket, meta);
  }
}
