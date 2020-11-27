function OnUpdate(doc, meta) {
  try {
   // create a temporary doc
   var workdoc = meta.id + "_workdoc";
   dst_bucket[workdoc] = {"state": "original"};

   // do a cached get
   var start = new Date().getTime();
   var obj = couchbase.get(dst_bucket, {"id": workdoc}, {"cache": true});
   log("cached value", obj);

   // cache does not understand N1QL and hence below makes it stale
   UPDATE `hello-world` SET state = "final" WHERE META().id = $workdoc;

   // watch it for 2 seconds
   var prev;
   var flipped = "never";
   while (new Date().getTime() - start < 2000) {
     prev = obj;
     obj = couchbase.get(dst_bucket, {"id": workdoc}, {"cache": true});
     if (obj.doc.state !== prev.doc.state) {
       flipped = new Date().getTime() - start;
       log("cached value expired");
     }
   }

   // ensure we saw value flip around 1 second
   if (!Number.isInteger(flipped) || flipped > 1500 || flipped < 500) {
     log("cache test failed. expected to see it expire around 1000ms but measured", flipped);
     return;
   }

   // success
   dst_bucket[meta.id] = "pass";
   log("success", workdoc);
  } catch (err) {
    log(err);
  }
}
