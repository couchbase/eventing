function OnUpdate(doc, meta) {}

function OnDeploy(action) {
  // This empty `for loop` stalls OnDeploy handler for at least 5 seconds
  for (let i = 0; i < 10_000_000_000; i++) {}
}
