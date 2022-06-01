function OnUpdate(doc, meta) {
  try {
    var req = {"id": meta.id+"_Handler", "keyspace": {"scope_name": "NotExist", "collection_name": "NotExist"}};
    couchbase.upsert(dst_bucket, req, 'success');
  }
  catch(e) {
    dst_bucket1[meta.id] = "success";
  }
}

function OnDelete(meta, options) {
	try {
		dst_bucket[meta.id] = "success";
	}
        catch(e) {
		dst_bucket1[meta.id] = "success";
	}
} 
