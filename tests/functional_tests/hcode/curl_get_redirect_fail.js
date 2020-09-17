function OnUpdate(doc, meta) {
    var request = {
	redirect: false,
        headers: {
            'Accept': 'application/json',
            'Redirect': 'true'
        }
    };

    try {
        var response = curl('GET', localhost, request);
        log(response);
        if (!verifyResponse(response)) {
            throw 'inconsistent response';
        }
        dst_bucket[meta.id] = 'hello world';
    } catch (e) {
        log('error', e);
    }
}

function verifyResponse(response) {
    var resp_codes = [301,302,303]
    if(resp_codes.includes(response.status)) {
        return true;
    }
    return false;
}
