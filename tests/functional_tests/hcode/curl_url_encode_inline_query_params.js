function OnUpdate(doc, meta) {
    var request = {
        path : '/p@thA/p@thB?1=2&key1=value@1&key2=value=2',
        url_encode_version: url_encoding_val
    };
    try {
        var response = curl('GET', localhost, request);
        log(response);
        if (!verifyResponse(response)) {
            throw 'inconsistent response';
        }
        dst_bucket[meta.id
        ] = JSON.stringify(response);
    } catch (e) {
        log('error', e);
    }
}

function verifyResponse(response) {
    var expected = {
        status: 200,
        headers: {
            "Content-Type": " application/json;charset=UTF-8\r\n",
        }
    };
    if(response.status !== expected.status) {
        return false;
    }
    if(response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    return true;
}