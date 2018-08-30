function OnUpdate(doc,meta) {
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() - 500);

    let context = {docID : meta.id};
    createTimer(Callback, fireAt, meta.id, context);
}

function Callback(context) {
    dst_bucket[context.docID] = 'From Callback';
}
