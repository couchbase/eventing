function OnUpdate(doc,meta) {
    var fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 5);

    var context = {docID : meta.id};
    createTimer(Callback, fireAt, meta.id, context);
}

function Callback(context) {
    dst_bucket[context.docID] = 'From Callback';
}

function OnDelete(meta) {
    log('deleting document', meta.id);
    delete dst_bucket[meta.id]; // DELETE operation
}
