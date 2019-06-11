function OnUpdate(doc, meta) {
    log('document', doc);
    //res = test_continue();
    let nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    let count = 0;
    for(let row of nums) {
      for(let row of nums) {
        count++;
        if(count == 5 || count == 7){
          continue;
        }
        ++count;
      }
    }
    let res1 = SELECT * FROM default LIMIT 10;
    let count1 = 0;
    for(let row of res1) {
      for(let row of res1) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue;
        }
        ++count1;
      }
    }
    log("count : ",count);
    log("count1 : ",count1);
    if (count1 === 17){
        dst_bucket[meta.id] = doc;
    }
}
function OnDelete(meta) {
}

//function test_continue(){
//
//}