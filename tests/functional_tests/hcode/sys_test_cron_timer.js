function OnUpdate(doc, meta) {
    //log('creating document on dst : ', meta.id);
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallbackCreate, meta.id, expiry);
}

function OnDelete(meta) {
    //log('deleting document: ', meta.id);180
    var expiry = Math.round((new Date()).getTime() / 1000) + 30;
    cronTimer(NDtimerCallbackN1QL, meta.id, expiry);
}

function NDtimerCallbackCreate(docid) {
    dst_bucket3[docid] = 'from NDtimerCallbackCreate';
}

function NDtimerCallbackN1QL(docid) {
    delete dst_bucket3[docid];
}
