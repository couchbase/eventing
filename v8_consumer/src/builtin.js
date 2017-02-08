
function n1ql(strings, ...values) {
    var stringsLength = strings.length;
    var query = "";

    query = strings[0];

    for (i = 0; i < stringsLength - 1; i++) {
        if (typeof values[i] === "string" && values[i].indexOf("-") !== -1) {
            query = query.concat('`');
            query = query.concat(values[i]);
            query = query.concat('`');
        } else {
          query = query.concat(values[i]);
        }
        query = query.concat(strings[i + 1]);
    }
    return _n1ql[query];
}
