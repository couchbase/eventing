function OnUpdate(doc, meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    var payload = 'abcd'
    docTimer(timerCallback, expiry, meta.id, payload);
}
function OnDelete(meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, expiry, meta.id);
}
function NDtimerCallback(docid) {
    delete dst_bucket[docid];
}
function timerCallback(docid) {
    dst_bucket[docid] = 'from timerCallback';
}
