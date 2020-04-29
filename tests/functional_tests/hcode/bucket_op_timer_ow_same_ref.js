function OnUpdate(doc,meta) {
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 20);

    let context = {docID : meta.id};
    createTimer(Callback, fireAt, "same reference", context);
}

function Callback(context) {
    dst_bucket[context.docID] = 'From Callback';
}

function OnDelete(meta) {
    log('cancelling timer', meta.id);
    cancelTimer(Callback, meta.id);
}
