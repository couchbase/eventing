function OnUpdate(doc,meta) {
    var expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, expiry, meta.id);
}
function timerCallback(docid) {
    INSERT INTO `hello-world` ( KEY, VALUE ) VALUES ( UUID() ,'timerCallback');
}
