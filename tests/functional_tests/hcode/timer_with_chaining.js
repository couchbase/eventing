function OnUpdate(doc, meta) {
    // Start a timer chain when the trigger document is inserted
    if (meta.id === "timer_gap_test_trigger") {
        let fireAt = new Date();
        fireAt.setSeconds(fireAt.getSeconds() + 10);
        let context = {seq: 0};
        createTimer(TimerCallback, fireAt, "timer_chain_" + meta.id, context);
        log("Timer chain started for gap recovery test");
    }
}

function TimerCallback(context) {
    // Chain the timer - create a new timer to fire 10 seconds later
    // This creates continuous timer activity to keep span documents active
    let fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + 10);
    let newContext = {seq: context.seq + 1};
    createTimer(TimerCallback, fireAt, "timer_chain_" + context.seq, newContext);
    
    // Log to indicate timer is firing (helps verify function is running)
    if (context.seq % 10 === 0) {
        log("Timer fired: seq=" + context.seq + " at " + new Date().toISOString());
    }
}

function OnDelete(meta) {
    // Clean up - no special handling needed
}
