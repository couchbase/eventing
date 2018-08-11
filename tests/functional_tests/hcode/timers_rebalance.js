function OnUpdate(doc, meta) {
    let expiry = Math.round((new Date()).getTime() / 1000) + 5;
    let payload = 'abcd';
    createTimer(timerCallback, expiry, meta.id, payload);
}
function OnDelete(meta) {
    let expiry = Math.round((new Date()).getTime() / 1000) + 5;
    createTimer(NDtimerCallback, expiry, meta.id);
}
function NDtimerCallback(docid) {
    delete dst_bucket[docid];
}
function timerCallback(docid) {
    dst_bucket[docid] = 'from timerCallback';
}
