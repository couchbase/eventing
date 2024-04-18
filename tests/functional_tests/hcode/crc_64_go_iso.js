function OnUpdate(doc, meta) {
    var result = couchbase.crc_64_go_iso("hello");
    if (result != "3c3eeee2d8100000") {
        return;
    }

    result = couchbase.crc_64_go_iso(1);
    if (result != "4320000000000000") {
        return;
    }

    result = couchbase.crc_64_go_iso([1,2,3,4]);
    if (result != "3b8002901291a293") {
        return;
    }

    result = couchbase.crc_64_go_iso({"my_key":"my_value"});
    if (result != "756fe1c0128e6be0") {
        return;
    }

    result = couchbase.crc_64_go_iso(null);
    if (result != "3eeef9ddb0000000") {
        return;
    }

    result = couchbase.crc_64_go_iso(false);
    if (result != "32cc7ee410300000") {
        return;
    }

    result = couchbase.crc_64_go_iso(undefined);
    if (result != "fcd0dd899002d36d") {
        return;
    }

    // JSON stringify of any Symbol gives undefined, so CRC of Symbol is equivalent to CRC of undefined
    result = couchbase.crc_64_go_iso(Symbol("hey"));
    if (result != "fcd0dd899002d36d") {
        return;
    }

    result = couchbase.crc_64_go_iso(new Date("2024-05-10"))
    if (result != "508adbb2e62fe363") {
        return;
    }

    // BigInt datatype gives TypeError when passed as an argument to crc function
    try {
        couchbase.crc_64_go_iso(12345678901234567890123456123456n);
    } catch (err) {
        if (!(err instanceof TypeError)) {
            return;
        }
    }

    dst_bucket[meta.id] = "success";
}