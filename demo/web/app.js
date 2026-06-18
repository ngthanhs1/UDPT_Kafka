async function sendSuccess(){

    await fetch(
        "http://localhost:8081/send-success"
    );

}

async function sendFailed(){

    await fetch(
        "http://localhost:8081/send-failed"
    );

}

async function loadLogs(){

    const response =
        await fetch(
            "http://localhost:8081/logs"
        );

    const text =
        await response.text();

    document.getElementById(
        "logs"
    ).textContent = text;

    calculateStats(text);
}

function calculateStats(logs){

    const success =
        (logs.match(/SUCCESS/g)||[])
        .length;

    const retry =
        (logs.match(/RETRY/g)||[])
        .length;

    const dlq =
        (logs.match(/DLQ/g)||[])
        .length;

    document.getElementById(
        "successCount"
    ).innerText =
        "Success: " + success;

    document.getElementById(
        "retryCount"
    ).innerText =
        "Retry: " + retry;

    document.getElementById(
        "dlqCount"
    ).innerText =
        "DLQ: " + dlq;
}

setInterval(
    loadLogs,
    1000
);