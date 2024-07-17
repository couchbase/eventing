function OnUpdate(doc, meta) {}

function OnDelete(meta) {}

function OnDeploy(action) {
  throw new Error("this is some user error");
}
