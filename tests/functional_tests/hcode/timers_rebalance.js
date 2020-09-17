function OnUpdate(doc, meta) {
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 5);

    let context = {docID: meta.id};
    createTimer(setCallback, fireAt, meta.id, context);
}
function OnDelete(meta) {
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 30);

    let context = {docID: meta.id};
    createTimer(delCallback, fireAt, meta.id, context);
}

function delCallback(context) {
    delete dst_bucket[context.docID];
}

function setCallback(context) {
    dst_bucket[context.docID] = 'from timerCallback';
}
