async function loadData(){

    let response =
        await fetch(
            "data/events.log?t="
            + Date.now()
        );

    let text =
        await response.text();

    let lines =
        text.trim()
            .split("\n");

    let success = 0;
    let retry = 0;
    let dlq = 0;

    let events =
        document.getElementById(
            "events"
        );

    events.innerHTML = "";

    lines.slice(-20)
        .reverse()
        .forEach(line=>{

            let li =
                document.createElement(
                    "li"
                );

            li.innerText =
                line;

            events.appendChild(
                li
            );

            if(
                line.startsWith(
                    "SUCCESS"
                )
            ){
                success++;
            }

            if(
                line.startsWith(
                    "RETRY"
                )
            ){
                retry++;
            }

            if(
                line.startsWith(
                    "DLQ"
                )
            ){
                dlq++;
            }

        });

    document.getElementById(
        "success"
    ).innerText = success;

    document.getElementById(
        "retry"
    ).innerText = retry;

    document.getElementById(
        "dlq"
    ).innerText = dlq;
}

loadData();

setInterval(
    loadData,
    1000
);