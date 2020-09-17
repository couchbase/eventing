function OnUpdate(doc, meta) {
  log("Begin");
  dst_bucket[meta.id] = doc;
  log("End");
  if (Math.random() > 0.5) {
    no_such_function();
  } else {
    throw "party";
  }
}
