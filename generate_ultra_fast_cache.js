/**
 * generate_ultra_fast_cache.js
 * Scraper EPON mejorado: obtiene lista de ONTs, potencia óptica (RxPower, TxPower,
 * TxBias, Temperature, Voltage) y detalles de tiempo online/offline.
 * Compatible con OLTs VSOL/Genova que usan CLI EPON.
 *
 * IMPORTANTE: el escaneo respeta un PRESUPUESTO DE TIEMPO TOTAL (por defecto 20s).
 * Si una OLT no responde, no conecta, o se queda colgada en algún paso,
 * el scraper corta y devuelve lo que haya logrado obtener (o un error rápido),
 * para que el servidor pueda pasar a la siguiente OLT sin quedarse esperando.
 */
const net = require("net");

const DEFAULT_BUDGET_MS = 90000;   // Tiempo máximo total por OLT
// Subido de 20s a 90s: OLTs grandes (200+ ONTs en 8 PONs activos, algunos con
// hasta ~60 equipos cada uno) necesitan más tiempo para listar todo vía
// "show onu info epon 0/X all", que pagina con --More-- varias veces por PON.
// Como el escaneo corre en segundo plano (el resultado llega por WebSocket,
// no bloquea ninguna respuesta HTTP), no hay costo real en darle más tiempo.
const CONNECT_TIMEOUT_MS = 6000;   // Si no conecta en este tiempo, se descarta esta OLT

async function scrapeOLT(host, user, password, budgetMs = DEFAULT_BUDGET_MS) {
    return new Promise((resolve, reject) => {
        const startTs = Date.now();
        const remaining = () => Math.max(0, budgetMs - (Date.now() - startTs));

        const socket = net.createConnection({ host, port: 23 });
        let buf = "";
        const allOnts = [];

        // Presupuesto global: si se agota, devolvemos lo que tengamos (parcial)
        // o rechazamos rápido si no se obtuvo nada. Así nunca nos quedamos
        // esperando más de `budgetMs` por una sola OLT.
        const globalTimeout = setTimeout(() => {
            socket.destroy();
            if (allOnts.length > 0) {
                resolve(buildResult(allOnts, true));
            } else {
                reject(new Error(`Sin respuesta de ${host} dentro de ${Math.round(budgetMs / 1000)}s`));
            }
        }, budgetMs);

        // Si ni siquiera logra establecer la conexión TCP rápido (OLT apagada,
        // caída de red, IP inalcanzable, etc.), no tiene sentido seguir esperando:
        // se descarta esta OLT de inmediato para liberar el turno de la siguiente.
        const connectTimeout = setTimeout(() => {
            clearTimeout(globalTimeout);
            socket.destroy();
            reject(new Error(`No se pudo conectar a ${host}:23 en ${Math.round(CONNECT_TIMEOUT_MS / 1000)}s`));
        }, Math.min(CONNECT_TIMEOUT_MS, budgetMs));

        socket.on("data", (d) => {
            buf += d.toString();
            if (buf.includes("-- Enter Key To Continue --") || buf.includes("--More--") || buf.includes("More: ")) {
                socket.write(" ");
            }
        });

        // waitFor: ESTRICTO — usar solo para pasos de autenticación donde
        // un timeout SÍ significa un error real (credenciales malas, OLT caída).
        // Si truena, rechaza la promesa.
        const waitFor = (regex, timeout = 3000) => new Promise((res, rej) => {
            const cap = Math.max(200, Math.min(timeout, remaining()));
            const start = Date.now();
            const check = () => {
                if (regex.test(buf)) {
                    const out = buf;
                    buf = "";
                    res(out);
                } else if (Date.now() - start > cap) {
                    rej(new Error(`Timeout esperando: ${regex}`));
                } else {
                    setTimeout(check, 150);
                }
            };
            check();
        });

        // waitCapture: TOLERANTE — usar para comandos de datos (listas de ONTs,
        // métricas ópticas, detalles). NUNCA rechaza ni descarta lo que llegó:
        // si se cumple el timeout antes de ver el prompt final, igual devuelve
        // todo lo que se haya acumulado en el buffer hasta ese momento, y
        // siempre limpia `buf` para que no se contamine el siguiente comando.
        // Esto es justo lo que faltaba: antes, un timeout aquí tiraba los datos
        // a la basura (por el .catch(() => "")) y además dejaba el buffer sucio
        // para el siguiente PON, perdiendo ONTs en los PON con más equipos
        // conectados (donde la salida pagina con --More-- y tarda más).
        const waitCapture = (regex, timeout = 3000) => new Promise((res) => {
            const cap = Math.max(200, Math.min(timeout, remaining()));
            const start = Date.now();
            const check = () => {
                if (regex.test(buf) || Date.now() - start > cap) {
                    const out = buf;
                    buf = "";
                    res(out);
                } else {
                    setTimeout(check, 150);
                }
            };
            check();
        });

        socket.on("connect", async () => {
            clearTimeout(connectTimeout);
            try {
                // ── Autenticación ──────────────────────────────────────────
                await waitFor(/Username:/i, 4000); socket.write(`${user}\r\n`);
                await waitFor(/Password:/i, 4000); socket.write(`${password}\r\n`);
                const loginOut = await waitFor(/[>#]/, 5000);
                if (loginOut.includes(">")) {
                    socket.write("enable\r\n");
                    await waitFor(/#/, 4000);
                }

                // ── PASO 1: Lista de ONTs por PON (mientras quede presupuesto) ──
                // Timeout subido a 7s: PONs con muchas ONTs (vimos hasta 59 en un
                // solo PON) paginan la salida (--More--) varias veces y necesitan
                // más tiempo para terminar de imprimir todo. Con waitCapture, aunque
                // se llegue al timeout sin ver el "#" final, no se pierde lo ya
                // recibido — y con el presupuesto total ahora en 90s, alcanza para
                // las 8 interfaces sin cortarse a la mitad.
                let stoppedEarly = false;
                for (let i = 1; i <= 16; i++) {
                    if (remaining() < 1500) { stoppedEarly = true; break; }
                    socket.write(`show onu info epon 0/${i} all\r\n`);
                    await new Promise(r => setTimeout(r, 150));
                    const out = await waitCapture(/#/, 7000);
                    parseOntList(out, allOnts);
                }

                // Deduplicar
                const uniqueMap = new Map();
                allOnts.forEach(o => uniqueMap.set(`${o.pon}:${o.ont}`, o));
                const uniqueOnts = Array.from(uniqueMap.values());

                // ── PASO 2: Métricas ópticas para ONTs online (si sobra tiempo) ──
                const onlineOnts = uniqueOnts.filter(o => o.status === "working");
                for (const ont of onlineOnts) {
                    if (remaining() < 700) { stoppedEarly = true; break; }
                    socket.write(`show onu optical-ddm epon 0/${ont.pon} ${ont.ont}\r\n`);
                    const pOut = await waitCapture(/#/, 1800);
                    parseOptical(pOut, ont);
                }

                // ── PASO 3: Detalles (tiempo online/offline) — solo si sobra tiempo ──
                for (const ont of onlineOnts) {
                    if (remaining() < 700) { stoppedEarly = true; break; }
                    socket.write(`show onu detail epon 0/${ont.pon} ${ont.ont}\r\n`);
                    const dOut = await waitCapture(/#/, 1500);
                    parseDetail(dOut, ont);
                }

                clearTimeout(globalTimeout);
                try { socket.write("exit\r\n"); socket.end(); } catch (e) { /* ignorar */ }
                resolve(buildResult(uniqueOnts, stoppedEarly));
            } catch (e) {
                clearTimeout(globalTimeout);
                socket.destroy();
                if (allOnts.length > 0) {
                    resolve(buildResult(allOnts, true));
                } else {
                    reject(e);
                }
            }
        });

        socket.on("error", (err) => {
            clearTimeout(globalTimeout);
            clearTimeout(connectTimeout);
            reject(err);
        });
    });
}

// ── Parsers ────────────────────────────────────────────────────────────────

function parseOntList(out, allOnts) {
    const lines = out.split("\n");
    lines.forEach(l => {
        const parts = l.trim().split(/\s+/);
        // Formato esperado: 0/X:Y  MAC  Up/Down  ...
        if (parts.length >= 3 && /^0\/\d+:\d+$/.test(parts[0])) {
            const [ponStr, ontStr] = parts[0].split(":");
            const pon = parseInt(ponStr.split("/")[1]);
            const ont = parseInt(ontStr);
            const mac = parts.find(p => /^[0-9a-fA-F]{2}(:[0-9a-fA-F]{2}){5}$/.test(p)) || "N/A";
            const statusPart = parts.find(p => /^(Up|Down|working|offline|online)$/i.test(p)) || "Down";
            const status = /Up|working|online/i.test(statusPart) ? "working" : "offline";

            // En la salida real: 0/1:8  e0:e8:e6:83:99:46  Up  ...  2D 0H 40M  0
            // (el campo de tiempo "Online" puede venir como "Xd Xh Xm" o "Xh Xm Xs",
            // y justo después viene el DeregisterCnt, un número suelto. Antes se
            // calculaba por posición fija desde el primer token con 'H', lo cual
            // agarraba ese DeregisterCnt como si fuera parte del uptime. Ahora se
            // usa un regex directo sobre la línea, que es inmune a esa columna extra.)
            const upM = l.match(/(\d+D\s+)?\d+H\s+\d+M(\s+\d+S)?/);
            const uptime = upM ? upM[0].trim() : null;

            // Intentar extraer potencia directamente de la línea (algunos firmwares la incluyen)
            let rxPower = null;
            for (let j = parts.length - 1; j >= 0; j--) {
                const val = parseFloat(parts[j]);
                if (!isNaN(val) && val < -5 && val > -45) {
                    rxPower = val;
                    break;
                }
            }

            allOnts.push({
                pon,
                ont,
                macAddress: mac.toUpperCase(),
                serial: mac.toUpperCase(),
                status,
                rxPower,
                txPower: null,
                txBias: null,
                temperature: null,
                voltage: null,
                firstOnlineTime: null,
                lastOnlineTime: null,
                lastOfflineTime: null,
                uptime: uptime,
                type: "EPON",
                name: null
            });
        }
    });
}

function parseOptical(out, ont) {
    const rxM = out.match(/RxPower\s*[:\-]?\s*(-?[\d.]+)\s*dBm/i);
    if (rxM) ont.rxPower = parseFloat(rxM[1]);

    const txM = out.match(/TxPower\s*[:\-]?\s*(-?[\d.]+)\s*dBm/i);
    if (txM) ont.txPower = parseFloat(txM[1]);

    const biasM = out.match(/TxBias\s*[:\-]?\s*([\d.]+)\s*mA/i);
    if (biasM) ont.txBias = parseFloat(biasM[1]);

    const tempM = out.match(/Temperature\s*[:\-]?\s*([\d.]+)\s*C/i);
    if (tempM) ont.temperature = parseFloat(tempM[1]);

    const voltM = out.match(/Voltage\s*[:\-]?\s*([\d.]+)\s*V/i);
    if (voltM) ont.voltage = parseFloat(voltM[1]);

    // Fallback: buscar valor numérico en rango de potencia si aún es null
    if (ont.rxPower === null) {
        const lines = out.split("\n");
        for (const line of lines) {
            const m = line.match(/(-[\d]{1,2}\.[\d]{1,2})\s*dBm/);
            if (m) {
                const val = parseFloat(m[1]);
                if (val < -5 && val > -45) {
                    ont.rxPower = val;
                    break;
                }
            }
        }
    }
}

function parseDetail(out, ont) {
    const firstM = out.match(/First\s+Online\s+Time\s*[:\-]\s*(.+?)(?:\r|\n|$)/i);
    if (firstM) ont.firstOnlineTime = firstM[1].trim();

    const lastOnM = out.match(/Last\s+Online\s+Time\s*[:\-]\s*(.+?)(?:\r|\n|$)/i);
    if (lastOnM) ont.lastOnlineTime = lastOnM[1].trim();

    const lastOffM = out.match(/Last\s+Offline\s+Time\s*[:\-]\s*(.+?)(?:\r|\n|$)/i);
    if (lastOffM) ont.lastOfflineTime = lastOffM[1].trim();

    const uptimeM = out.match(/Up\s*Time\s*[:\-]\s*(.+?)(?:\r|\n|$)/i);
    if (uptimeM) ont.uptime = uptimeM[1].trim();

    const ipM = out.match(/Management\s+IP\s*[:\-]\s*([\d.]+)/i);
    if (ipM) ont.managementIp = ipM[1];
}

// ── Construir resultado final ──────────────────────────────────────────────

function buildResult(onts, partial) {
    const activePons = [...new Set(onts.map(o => o.pon))].sort((a, b) => a - b);

    const pons = activePons.map(pId => {
        const pOnts = onts.filter(o => o.pon === pId);
        const withPower = pOnts.filter(o => o.rxPower !== null);
        const avgPower = withPower.length > 0
            ? parseFloat((withPower.reduce((s, o) => s + o.rxPower, 0) / withPower.length).toFixed(2))
            : null;
        const minPower = withPower.length > 0
            ? parseFloat(Math.min(...withPower.map(o => o.rxPower)).toFixed(2))
            : null;
        const maxPower = withPower.length > 0
            ? parseFloat(Math.max(...withPower.map(o => o.rxPower)).toFixed(2))
            : null;

        // ONTs con potencia crítica (< -28 dBm)
        const criticalOnts = pOnts.filter(o => o.rxPower !== null && o.rxPower < -28).length;

        // Temperatura promedio
        const withTemp = pOnts.filter(o => o.temperature !== null);
        const avgTemp = withTemp.length > 0
            ? parseFloat((withTemp.reduce((s, o) => s + o.temperature, 0) / withTemp.length).toFixed(1))
            : null;

        return {
            pon: pId,
            count: pOnts.length,
            online: pOnts.filter(o => o.status === "working").length,
            avgPower,
            minPower,
            maxPower,
            criticalOnts,
            avgTemp,
            onts: pOnts
        };
    });

    // ONTs desenganchadas: offline con lastOnlineTime reciente (si disponible)
    const disconnectedOnts = onts.filter(o => o.status === "offline" && o.lastOnlineTime);

    return {
        lastUpdate: new Date().toISOString(),
        totalOnts: onts.length,
        totalWorking: onts.filter(o => o.status === "working").length,
        totalOffline: onts.filter(o => o.status === "offline").length,
        totalCritical: onts.filter(o => o.rxPower !== null && o.rxPower < -28).length,
        avgTemperature: (() => {
            const withTemp = onts.filter(o => o.temperature !== null);
            return withTemp.length > 0
                ? parseFloat((withTemp.reduce((s, o) => s + o.temperature, 0) / withTemp.length).toFixed(1))
                : null;
        })(),
        pons,
        disconnectedOnts,
        partial: partial || false
    };
}

// ── Ejecución ──────────────────────────────────────────────────────────────
if (require.main === module) {
    const [,, host, user, password, budgetArg] = process.argv;
    if (!host || !user || !password) {
        console.error(JSON.stringify({ success: false, message: "Uso: node generate_ultra_fast_cache.js <host> <user> <password> [budgetMs]" }));
        process.exit(1);
    }
    const budgetMs = parseInt(budgetArg, 10) || DEFAULT_BUDGET_MS;

    scrapeOLT(host, user, password, budgetMs)
        .then(data => process.stdout.write(JSON.stringify(data)))
        .catch(err => {
            process.stderr.write(JSON.stringify({ success: false, message: err.message }));
            process.exit(1);
        });
} else {
    module.exports = { scrapeOLT, DEFAULT_BUDGET_MS };
}