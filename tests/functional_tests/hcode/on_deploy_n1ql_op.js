function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  INSERT INTO `hello-world`(KEY, VALUE) VALUES(UUID(), 'Hello world');
}
