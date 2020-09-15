function OnUpdate(doc, meta) {
  var keyReplace = meta.id+"_Replace";
  var {success, error} = couchbase.replace(dst_bucket, {"id": keyReplace}, 'success');
  if(!success && error.key_not_found) {
    dst_bucket[keyReplace] = "Created Doc";
    var {success} = couchbase.replace(dst_bucket, {"id": keyReplace}, 'Replaced');
    doc = dst_bucket[keyReplace];
    if(success && doc == "Replaced") {
      delete dst_bucket[keyReplace];
      delete dst_bucket[meta.id];
    }
  }
}
