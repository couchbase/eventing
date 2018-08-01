function OnUpdate(doc, meta) {
    //log('creating document on dst : ', meta.id);
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, expiry, meta.id);
}

function OnDelete(meta) {
    //log('deleting document: ', meta.id);
    var expiry = Math.round((new Date()).getTime() / 1000) + 30;
    cronTimer(NDtimerCallback, expiry, meta.id);
}

function timerCallback(docid) {
    dst_bucket2[docid] = 'from timerCallback';
}

function NDtimerCallback(docid) {
    delete dst_bucket2[docid];
}
