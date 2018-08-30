function OnUpdate(doc, meta) {
    let expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);

    let context = {docID : meta.id};
    createTimer(timerCallback,  expiry, meta.id, context);
}
function OnDelete(meta) {
    let expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 5);

    let context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}
function NDtimerCallback(context) {
    delete dst_bucket[context.docID];
}
function timerCallback(context) {
    dst_bucket[context.docID] = 'from timerCallback';
}