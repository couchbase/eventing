function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'application/x-www-form-urlencoded'
        }
    };

    try {
        var response = curl('DELETE', localhost, request);
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
            "Content-Type": " application/x-www-form-urlencoded; charset=utf-8\r\n",
            "Content-Length": " 45\r\n"
        },
        body: {
            "some-key": "some-value",
            "another-key": "another-value"
        }
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
    if (response.body['some-key'] !== expected.body['some-key']) {
        return false;
    }
    if (response.body['another-key'] !== expected.body['another-key']) {
        return false;
    }
    return true;
}