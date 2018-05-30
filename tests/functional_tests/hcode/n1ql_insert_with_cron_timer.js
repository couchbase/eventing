function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, expiry, meta.id);
}
function NDtimerCallback(docid) {
    INSERT INTO `hello-world` ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback');
}
