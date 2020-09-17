function OnUpdate(doc,meta) {
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 100);

    let context = {docID : meta.id};
    createTimer(Callback, fireAt, meta.id, context);
}

function Callback(context) {
    dst_bucket[context.docID] = 'From Callback';
}

function OnDelete(meta) {
    log('deleting document', meta.id);
    delete dst_bucket[meta.id]; // DELETE operation
}
