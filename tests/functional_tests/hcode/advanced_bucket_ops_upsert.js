function OnUpdate(doc, meta) {
  var {success} = couchbase.upsert(dst_bucket, {"id": meta.id}, 'success');
  if(success) {
    var upsertedDoc = dst_bucket[meta.id];
    if(upsertedDoc === 'success') {
      delete dst_bucket[meta.id];
    }
  }
}
