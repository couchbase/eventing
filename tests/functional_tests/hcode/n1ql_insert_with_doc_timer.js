function OnUpdate(doc,meta) {
    let expiry = Math.round((new Date()).getTime() / 1000) + 5;
    docTimer(timerCallback, expiry, meta.id);
}
function timerCallback(meta) {
    INSERT INTO `hello-world` ( KEY, VALUE ) VALUES ( UUID() ,'timerCallback');
}
