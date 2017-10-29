function OnUpdate(doc,meta) {
    expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, meta.id, expiry);
}
function NDtimerCallback(docid) {
    var query = INSERT INTO `hello-world` ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback');
    query.execQuery();
}
