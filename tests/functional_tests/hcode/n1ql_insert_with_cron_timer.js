function OnUpdate(doc,meta) {
    expiry = Math.round((new Date()).getTime() / 1000) + 5;
    cronTimer(NDtimerCallback, meta.id, expiry);
}
function NDtimerCallback(docid) {
    INSERT INTO `hello-world` ( KEY, VALUE ) VALUES ( UUID() ,'NDtimerCallback');
}
