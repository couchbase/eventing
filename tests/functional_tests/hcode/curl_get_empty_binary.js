function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'image/png'
        }
    };

    try {
        var response = curl('GET', localhost, request);
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
            "Content-Type": " image/png\r\n",
            "Content-Length": " 0\r\n"
        },
        body : null
    };

    if (response.status !== expected.status) {
        return false;
    }
    if (response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    if (response.headers['Content-Length'] !== expected.headers['Content-Length']) {
        return false;
    }
    if (response.body !== expected.body) {
        return false;
    }
    return true;
}