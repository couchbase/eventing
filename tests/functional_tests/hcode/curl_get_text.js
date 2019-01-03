function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'text/plain'
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
            "Content-Type": " text/plain; charset=utf-8\r\n",
            "Content-Length": " 35\r\n"
        },
        body: 'here comes some value as text/plain'
    };

    if(response.status !== expected.status) {
        return false;
    }
    if(response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    if(response.headers['Content-Length'] !== expected.headers['Content-Length']) {
        return false;
    }
    if (response.body !== expected.body) {
        return false;
    }
    return true;
}
