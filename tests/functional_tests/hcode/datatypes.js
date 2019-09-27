function OnUpdate(doc, meta) {
    let tests =
        [
            UpsertSymbolKey, UpsertSymbolValue, SelectSymbol,
            UpsertUndefinedKey, UpsertUndefinedValue, SelectUndefined,
            UpsertFunctionKey, UpsertFunctionValue, SelectFunction,
            SetSymbolKeyValue, SetSymbolKey, SetSymbolValue,
            SetFunctionKeyValue, SetFunctionKey, SetFunctionValue,
            SetUndefinedKeyValue, SetUndefinedKey, SetUndefinedValue
        ];

    for(let test of tests) {
        try {
            test();
        } catch (e) {
            if (!(e instanceof N1QLError || e instanceof KVError || e instanceof EventingError)) {
                throw e;
            }
            dst_bucket[`${meta.id}_${test.name}`] = 'valid';
        }
    }
}

function UpsertSymbolKey() {
    let key = Symbol('new key type');
    UPSERT INTO `hello-world` (KEY, VALUE) VALUES ($key, "value");
}

function UpsertSymbolValue() {
    let value = Symbol('new value type');
    UPSERT INTO `hello-world` (KEY, VALUE) VALUES ("key", $value);
}

function SelectSymbol() {
    let abv = Symbol('5.5');
    SELECT * FROM `hello-world` WHERE abv > $abv;
}

function UpsertUndefinedKey() {
    var key;
    UPSERT INTO `hello-world` (KEY, VALUE) VALUES ($key, "value");
}

function UpsertUndefinedValue() {
    var value;
    UPSERT INTO `hello-world` (KEY, VALUE) VALUES ("key", $value);
}

function SelectUndefined() {
    var abv;
    SELECT * FROM `hello-world` WHERE abv > $abv;
}

function UpsertFunctionKey() {
    function key() {
    }
    UPSERT INTO `hello-world` (KEY, VALUE) VALUES ($key, "value");
}

function UpsertFunctionValue() {
    function value() {
    }
    UPSERT INTO `hello-world` (KEY, VALUE) VALUES ("key", $value);
}

function SelectFunction() {
    function abv() {
    }
    SELECT * FROM `hello-world` WHERE abv > $abv;
}

function SetSymbolKeyValue() {
    let key = Symbol('new key type');
    let value = Symbol('new value type');
    dst_bucket[key] = value;
}

function SetSymbolKey() {
    let key = Symbol('new key type');
    let value = 'some value';
    dst_bucket[key] = value;
}

function SetSymbolValue() {
    let key = 'some key';
    let value = Symbol('new value type');
    dst_bucket[key] = value;
}

function SetFunctionKeyValue() {
    function key() {
    }
    function value() {
    }
    dst_bucket[key] = value;
}

function SetFunctionKey() {
    function key() {
    }
    let value = 'some value';
    dst_bucket[key] = value;
}

function SetFunctionValue() {
    let key = 'some key';
    function value() {
    }
    dst_bucket[key] = value;
}

function SetUndefinedKeyValue() {
    var key;
    var value;
    dst_bucket[key] = value;
}

function SetUndefinedKey() {
    var key;
    let value = 'some value';
    dst_bucket[key] = value;
}

function SetUndefinedValue() {
    let key = 'some key';
    var value;
    dst_bucket[key] = value;
}
