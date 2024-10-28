function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  let fireAt = new Date();
  fireAt.setSeconds(fireAt.getSeconds() + 5);

  let context = { docID: "test_doc" };
  createTimer(Callback, fireAt, "test_doc_timer", context);
}

function Callback(context) {
  dst_bucket[context.docID] = 'From Callback';
}
