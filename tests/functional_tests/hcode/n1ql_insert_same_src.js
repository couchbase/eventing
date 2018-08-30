function OnUpdate(doc, meta) {
    INSERT INTO default (KEY, VALUE) VALUES (UUID() ,'Hello world');
}
