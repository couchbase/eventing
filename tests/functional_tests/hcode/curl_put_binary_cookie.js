function OnUpdate(doc, meta) {
    var request = {
        headers : {
            'Accept-Cookies': 'true'
        },
        body: GetSomeBinaryData()
    };

    try {
        var response = curl('PUT', localhost, request);
        log(response);
        if (!verifyFirstResponse(response)) {
            throw 'inconsistent first response';
        }
        // The "Set-Cookie" header must be received from the server in the first request
        if (!verifyCookieHeaderReceived(response)) {
            throw 'inconsistent cookie header';
        }

        response = curl('PUT', localhost, request);
        log(response);
        // The cookies header must be sent by the Function as part of the second request,
        // which is verified by the server
        if (!verifySecondResponse(response)) {
            throw 'inconsistent second response';
        }

        dst_bucket[meta.id] = JSON.stringify(response);
    } catch (e) {
        log(e);
    }
}

function GetSomeBinaryData() {
    return new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0]).buffer;
}

function verifyFirstResponse(response) {
    var expected = {
        status: 200,
        headers: {
            "Content-Type": " application/json; charset=utf-8\r\n"
        },
        body: {
            is_body_consistent: true
        }
    };

    if (response.status !== expected.status) {
        return false;
    }
    if (response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    if (response.body.is_body_consistent !== expected.body.is_body_consistent) {
        return false;
    }
    return true;
}

function verifySecondResponse(response) {
    var expected = {
        status: 200,
        headers: {
            "Content-Type": " application/json; charset=utf-8\r\n"
        },
        body: {
            is_body_consistent: true,
            is_cookie_consistent: true
        }
    };

    if (response.status !== expected.status) {
        return false;
    }
    if (response.headers['Content-Type'] !== expected.headers['Content-Type']) {
        return false;
    }
    if (response.body.is_body_consistent !== expected.body.is_body_consistent) {
        return false;
    }
    if (response.body.is_cookie_consistent !== expected.body.is_cookie_consistent) {
        return false;
    }
    return true;
}


// Verifies that a "Set-Cookie" header has been sent by the server
function verifyCookieHeaderReceived(response) {
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