const net = require("net");
const fs = require("fs");

const host = "10.250.41.211";
const user = "admin";
const password = "admin";

function test() {
    const socket = net.createConnection({ host, port: 23, timeout: 10000 });
    let buf = "";

    socket.on("data", (d) => {
        buf += d.toString();
        console.log("DATA RECEIVED:", d.toString());
        if (buf.includes("Username:")) {
            socket.write(user + "\r\n");
            buf = "";
        } else if (buf.includes("Password:")) {
            socket.write(password + "\r\n");
            buf = "";
        } else if (buf.includes(">") || buf.includes("#")) {
            if (buf.includes(">")) {
                socket.write("enable\r\n");
            } else {
                socket.write("show onu autofind all\r\n");
                setTimeout(() => {
                    socket.write("exit\r\n");
                    fs.writeFileSync("olt_debug_output.txt", buf);
                    console.log("DEBUG OUTPUT SAVED TO olt_debug_output.txt");
                    socket.end();
                }, 5000);
            }
            buf = "";
        }
    });

    socket.on("error", (err) => {
        console.error("SOCKET ERROR:", err);
    });

    socket.on("timeout", () => {
        console.error("SOCKET TIMEOUT");
        socket.destroy();
    });
}

test();
