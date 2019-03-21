function OnUpdate(doc,meta) {
    var fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 5);

    var context = {docID : meta.id};
    createTimer(Callback, fireAt, meta.id, context);
}

function Callback(context) {
    src_bucket[context.docID + "111111"] = 'From Callback';
}
