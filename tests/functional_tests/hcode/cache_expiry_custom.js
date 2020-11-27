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

   // watch it for 2 sec. we should not see updates as we set expiry to 1 hr
   var prev;
   var flipped = "never";
   while (new Date().getTime() - start < 2000) {
     prev = obj;
     obj = couchbase.get(dst_bucket, {"id": workdoc}, {"cache": true});
     if (obj.doc.state !== prev.doc.state) {
       flipped = new Date().getTime() - start;
     }
   }

   // ensure we did not see it flip as we set maxAge to a big number
   if (Number.isInteger(flipped)) {
     log("cache test failed. expected to not see it expire but measured", flipped);
     return;
   }

   // success
   dst_bucket[meta.id] = "pass";
   log("success", workdoc);
  } catch (err) {
    log(err);
  }
}
