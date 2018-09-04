function OnUpdate(doc,meta) {
    let expiry1 = new Date();
    expiry1.setSeconds(expiry1.getSeconds() + 15);

    let expiry2 = new Date();
    expiry2.setSeconds(expiry2.getSeconds() + 30);

    let expiry3 = new Date();
    expiry3.setSeconds(expiry3.getSeconds() + 45);

    let context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry1, meta.id, context);
    createTimer(timerCallback,  expiry2, meta.id, context);
    createTimer(timerCallback,  expiry3, meta.id, context);
}

function timerCallback(context) {
    let time_rand = random_gen();
    let doc_id = time_rand + ' ';
    dst_bucket[doc_id] = context.random_text;
}

function random_gen(){
    let rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    return Math.round((new Date()).getTime() / 1000) + rand;
}