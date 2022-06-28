function OnUpdate(doc, meta) {
  var req = {"id": meta.id+"_Handler", "keyspace": meta.keyspace};
	log(meta);
  couchbase.insert(dst_bucket, req, 'success');
}
