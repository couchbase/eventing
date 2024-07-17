function OnUpdate(doc, meta) {}

function OnDelete(meta, options) {}

function OnDeploy(action) {
  var meta = { "id": "1" };

  if (string_binding === undefined) return;
  dst_bucket[meta.id + string_binding] = "from handler";
  if (num_binding === undefined) return;
  dst_bucket[meta.id + num_binding] = num_binding;
  if (function_binding(2, 3) !== 5) return;
  dst_bucket[meta.id + function_binding(2, 3)] = "from handler";
  if (float_binding === undefined) return;
  dst_bucket[meta.id + "unique_id_4km1mqocm7"] = "from handler";
  if (json_binding === undefined) return;
  dst_bucket[meta.id + "unique_id_xcn14n1nxk3"] = json_binding;
  if (array_binding === undefined) return;
  dst_bucket[meta.id + "unique_id_knlkgwss91"] = array_binding;
}
