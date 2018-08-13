function OnUpdate(doc, meta) {
	let lim = 1,
	count = 0;

	// Labeled break
	let res1 = SELECT * FROM default LIMIT $lim;
	for(let row1 of res1) {
		let res2 = SELECT * FROM default LIMIT $lim;
		x: for(let row2 of res2) {
			let res3 = SELECT * FROM default LIMIT $lim;
			for(let row3 of res3) {
				let docId = meta.id + (++count);
				INSERT INTO `hello-world` (KEY, VALUE) VALUES ($docId, 'Hello world');
				break x;
			}
		}
	}
}

function OnDelete(meta) {
}
