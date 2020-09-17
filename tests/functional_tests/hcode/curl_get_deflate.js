function OnUpdate(doc, meta) {
    var request = {
        headers: {
        'Accept': 'deflate'
        }
    };

    try {
        var response = curl("GET", localhost, request);
        log(response);
        if (!verifyResponse(response)) {
            throw 'inconsistent response';
        }
        dst_bucket[meta.id] = JSON.stringify(response);
    } catch (e) {
        log('error', e);
    }
}

function verifyResponse(response) {
    var expected = {
        status: 200,
        headers: {
            "Content-Type":" application/json; charset=utf-8\r\n",
            "Content-Encoding":" deflate\r\n",
            },
        body: "{\"key\": \"here comes some value as application/json\"}",
    };

    var bodyLength = parseInt(response.headers['X-Original-Length'].match(/(\d+)/));
    if(response.status !== expected.status) {
        return false;
    }
    if(response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    if(response.body.length > bodyLength) {
        return false;
    }
    if(response.headers['Content-Encoding'] !== expected.headers['Content-Encoding']) {
        return false;
    }
    if (response.body !== expected.body) {
        return false;
    }
    return true;
}
