function OnUpdate(doc,meta) {
    expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, meta.id, expiry);
}
function timerCallback(docid, expiry) {
    dst_bucket[docid] = 'from timerCallback';
}