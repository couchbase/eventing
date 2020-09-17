function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'image/png',
            'Accept-Cookies': 'true'
        }
    };

    try {
        var response = curl('GET', localhost, request);
        log(response);
        if (!verifyResponse(response)) {
            throw 'inconsistent response';
        }
        if (!verifyCookieHeader(response)) {
            throw 'inconsistent cookie header';
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
            "Content-Length": " 16\r\n"
        },
        body: new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0])
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
    if (!(response.body instanceof ArrayBuffer)) {
        return false;
    }
    if (expected.body.toString() !== new Uint8Array(response.body).toString()) {
        return false;
    }
    return true;
}

function verifyCookieHeader(response) {
    var expected = {
        headers: {
            "Set-Cookie": " cookie-key=cookie-value; Path=/\r\n"
        }
    };

    if (response.headers['Set-Cookie'] !== expected.headers['Set-Cookie']) {
        return false;
    }
    return true;
}