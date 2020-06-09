function OnUpdate(doc, meta) {
  var expiry = new Date();
  expiry.setSeconds(expiry.getSeconds() + 10);

  var req = {"id": meta.id+"_Handler", "expiry_date": expiry};
  var insertResponse = couchbase.insert(dst_bucket, req, 'success');
  if(!insertResponse.success) {
    return;
  }

  var res = couchbase.get(dst_bucket, req);
  if(res.success && res.meta.expiry_date.getTime() === insertResponse.meta.expiry_date.getTime()) {
    delete dst_bucket[meta.id];
  }
}
