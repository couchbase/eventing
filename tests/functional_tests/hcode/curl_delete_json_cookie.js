function OnUpdate(doc, meta) {
    var request = {
        headers: {
            'Accept': 'application/json',
            'Accept-Cookies': 'true'
        }
    };

    try {
        var response = curl('DELETE', localhost, request);
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
            "Content-Type": " application/json; charset=utf-8\r\n",
            "Content-Length": " 51\r\n",
            "Set-Cookie": " cookie-key=cookie-value; Path=/"
        },
        body: {
            key: "here comes some value as application/json"
        }
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
    if (response.body.key !== expected.body.key) {
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