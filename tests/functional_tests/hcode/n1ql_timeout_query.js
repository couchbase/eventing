function OnUpdate(doc, meta) {
    var docId = meta.id;
    try{
        log ('executing n1ql, docId:', docId);
        var q1 = SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`) UNION
                 SELECT nosuchfield1 from `default` WHERE meta().id in (SELECT nosuchfield2 from `hello-world`);
        //var q1 = SELECT * from `default` USE KEYS[$docId];
        for (var r of q1) {
        }
        q1.close();
        log ('onupdate success, docId:', docId);
        var val = 'onupdate success, docId: ' + docId;
        dst_bucket['result_key'] = val;
    } catch(e) {
        log ('execption, docId:', docId, 'error:', e);
        var val = 'execption, docId:' + docId + 'error:' + JSON.stringify(e);
        dst_bucket['result_key'] = val;
        //q1.close();
    }
}

function OnDelete(meta) {
}