function OnUpdate(doc, meta) {
    log('document', doc);
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 15);

    let context = {docID: uuidv4()};
    createTimer(Callback, fireAt, meta.id, context);
}

function Callback(context) {
    dst_bucket[context.docID] = "hello world";
}

function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    let r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}
