const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs").promises;
const path = require("path");
const http = require("http");
const https = require("https");
const WebSocket = require("ws");
const net = require("net");

const app = express();
const PORT = 3001;
const OLT_DATA_FILE = path.join(__dirname, "olts.json");
const ONT_CACHE_FILE = path.join(__dirname, "ont_cache.json");
const HISTORY_FILE = path.join(__dirname, "power_history.json");
const UNAUTH_FILE = path.join(__dirname, "unauth_onts.json");

const SCAN_BUDGET_MS = 90000; // 90s — ver nota en generate_ultra_fast_cache.js
const CACHE_TTL_MS = 60000;

app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, "public")));

let olts = [];
let ontCache = {};
let activeScans = {};
let activeOpticalRefresh = {}; // { [oltId]: true } — evita refrescos duplicados en paralelo
let powerHistory = {};
let unauthOnts = {};

// ── WebSocket ──────────────────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });
const clients = new Set();

wss.on("connection", (ws) => {
    clients.add(ws);
    ws.on("close", () => clients.delete(ws));
    ws.on("error", () => clients.delete(ws));
});

function broadcast(type, data) {
    const msg = JSON.stringify({ type, data });
    clients.forEach(c => {
        if (c.readyState === WebSocket.OPEN) c.send(msg);
    });
}

// ── Carga / guardado de archivos ───────────────────────────────────────────
async function loadOLTs() {
    try { const d = await fs.readFile(OLT_DATA_FILE, "utf8"); olts = JSON.parse(d); }
    catch { olts = []; }
}
async function saveOLTs() {
    try { await fs.writeFile(OLT_DATA_FILE, JSON.stringify(olts, null, 2)); }
    catch (e) { console.error("Error guardando OLTs:", e.message); }
}
async function loadHistory() {
    try { const d = await fs.readFile(HISTORY_FILE, "utf8"); powerHistory = JSON.parse(d); }
    catch { powerHistory = {}; }
}
async function loadUnauth() {
    try { const d = await fs.readFile(UNAUTH_FILE, "utf8"); unauthOnts = JSON.parse(d); }
    catch { unauthOnts = {}; }
}
async function saveUnauth() {
    try { await fs.writeFile(UNAUTH_FILE, JSON.stringify(unauthOnts, null, 2)); }
    catch (e) { console.error("Error guardando unauth:", e.message); }
}
async function loadOntCache() {
    try { const d = await fs.readFile(ONT_CACHE_FILE, "utf8"); ontCache = JSON.parse(d); }
    catch { ontCache = {}; }
}
async function saveOntCache() {
    try { await fs.writeFile(ONT_CACHE_FILE, JSON.stringify(ontCache, null, 2)); }
    catch (e) { console.error("Error guardando cache:", e.message); }
}
async function saveHistory() {
    try { await fs.writeFile(HISTORY_FILE, JSON.stringify(powerHistory, null, 2)); }
    catch (e) { console.error("Error guardando historial:", e.message); }
}

// ── Telnet genérico: espera prompts reales ─────────────────────────────────
// Úsese para comandos de acción (reiniciar, autorizar, nombres, etc.)
function telnetCommand(host, user, password, commands, timeoutMs = 18000) {
    return new Promise((resolve, reject) => {
        const socket = net.createConnection({ host, port: 23 });
        let buf = "";
        const results = [];

        const globalTimer = setTimeout(() => {
            socket.destroy();
            reject(new Error(`Timeout global telnet ${host}`));
        }, timeoutMs);

        socket.on("data", (d) => {
            buf += d.toString();
            if (buf.includes("-- Enter Key To Continue --") || buf.includes("--More--"))
                socket.write(" ");
        });

        const waitFor = (pattern, ms = 6000) => new Promise((res, rej) => {
            const t = Date.now();
            const check = () => {
                if (pattern.test(buf)) { buf = ""; res(); }
                else if (Date.now() - t > ms) rej(new Error(`Timeout esperando ${pattern}`));
                else setTimeout(check, 100);
            };
            check();
        });

        const waitCapture = (pattern, ms = 6000) => new Promise((res) => {
            const t = Date.now();
            const check = () => {
                if (pattern.test(buf) || Date.now() - t > ms) { const out = buf; buf = ""; res(out); }
                else setTimeout(check, 100);
            };
            check();
        });

        socket.on("connect", async () => {
            try {
                await waitFor(/Username:/i, 7000);
                socket.write(`${user}\r\n`);
                await waitFor(/Password:/i, 5000);
                socket.write(`${password}\r\n`);

                // Esperar prompt > o #
                const loginOut = await waitCapture(/[>#]/, 5000);
                if (loginOut.includes(">")) {
                    socket.write("enable\r\n");
                    await waitCapture(/#/, 4000);
                }

                for (const cmd of commands) {
                    socket.write(`${cmd}\r\n`);
                    const out = await waitCapture(/#/, 4000);
                    results.push(out);
                }

                clearTimeout(globalTimer);
                try { socket.write("exit\r\n"); socket.end(); } catch (_) {}
                resolve(results);
            } catch (e) {
                clearTimeout(globalTimer);
                socket.destroy();
                reject(e);
            }
        });

        socket.on("error", (err) => { clearTimeout(globalTimer); reject(err); });
        socket.on("timeout", () => { clearTimeout(globalTimer); socket.destroy(); reject(new Error("TCP timeout")); });
    });
}

// ── Helper: detectar errores del firmware y parsear rango válido de PON ───
// Este firmware (HiOSO/VSOL EPON) SIEMPRE marca sus errores con "%" al
// inicio de línea, sin importar el idioma o el mensaje exacto. Usar esto
// es mucho más confiable que buscar palabras como "invalid"/"error" en
// inglés, que no siempre están presentes (ej: "% Epon if should be 1 ~ 4").
function isFirmwareError(text) {
    return /^%/m.test(text);
}
function parseEponRange(text) {
    const m = text.match(/Epon if should be\s+(\d+)\s*~\s*(\d+)/i);
    if (m) return { min: parseInt(m[1]), max: parseInt(m[2]) };
    return null;
}

// ── Detectar ONTs no autorizadas (HiOSO HA7104) ───────────────────────────
//
// Prompts reales de este firmware:
//   EPON>              → después del login (sin enable)
//   EPON#              → modo enable / configure terminal sale aquí también
//   EPON(config)#      → configure terminal
//   EPON(epon)#        → después de "epon"
//   EPON(epon_0/1)#    → dentro de "interface epon 0/1"
//
// Flujo por PON:
//   [desde EPON(epon)#]
//   interface epon 0/X   →  EPON(epon_0/X)#
//   show attempt onu     →  lista MACs pendientes
//   exit                 →  EPON(epon)#   ← vuelve al nodo padre
//
// IMPORTANTE: cuando el número de PON está fuera del rango soportado por
// esta OLT (ej. pedir PON 5 en una OLT de 4 puertos), el firmware responde
// "% Epon if should be 1 ~ 4" y te regresa a EPON(config)# — NO te deja en
// EPON(epon_0/X)# ni en EPON(epon)#. Si seguimos el loop como si nada,
// los siguientes comandos (show attempt onu, interface epon 0/X+1, etc.)
// se mandan en el contexto equivocado y todo se descarrila (por eso salían
// los "Unknown command" en PON 6, 7, 8). La solución es: en cuanto
// detectamos ese error de rango, paramos el loop ahí mismo — ya sabemos
// cuántos PON tiene la OLT, no hace falta seguir adivinando.
//
// ── Scraper HTTP para OLTs tipo "http" (onuAllPonOnuList.asp) ─────────────
// Campos reales del firmware (22 por ONT, verificados con data real):
//  [0]  "0/P:O"   [1]  nombre  [2]  MAC  [3]  Up/Down
//  [4-9] vendor/model/flags    [10] distancia en metros enteros
//  [11-15] potencia — siempre "--" en este FW (llega por Telnet)
//  [16] lastOnlineTime  [17] firstOnlineTime
//  [18] deregCount      [19] uptime en segundos  [20-21] contadores
function scrapeHTTP(host, user, password, timeoutMs = 6000) {
    return new Promise((resolve, reject) => {
        const auth = Buffer.from(`${user}:${password}`).toString("base64");
        const options = {
            hostname: host, port: 80,
            path: "/onuAllPonOnuList.asp", method: "GET",
            headers: { Authorization: `Basic ${auth}` },
            timeout: timeoutMs
        };
        const req = http.request(options, (res) => {
            let raw = "";
            res.on("data", d => raw += d.toString());
            res.on("end", () => {
                try { resolve(parseHTMLOntList(raw)); }
                catch (e) { reject(e); }
            });
        });
        req.on("timeout", () => { req.destroy(); reject(new Error(`HTTP timeout ${host}`)); });
        req.on("error", reject);
        req.end();
    });
}

function secsToUptime(s) {
    const n = parseInt(s);
    if (!n || n <= 0) return null;
    const d = Math.floor(n / 86400);
    const h = Math.floor((n % 86400) / 3600);
    const mi = Math.floor((n % 3600) / 60);
    return `${d}D ${h}H ${mi}M`;
}

function parseHTMLOntList(html) {
    const regex = /'([^']*)'/g;
    const vals = []; let m;
    while ((m = regex.exec(html)) !== null) vals.push(m[1]);

    // Layout del array por fila (confirmado con datos reales):
    // [0]  onuId      "0/1:8"
    // [1]  name       "1111188" o "NA"
    // [2]  mac        "E0:E8:E6:83:99:46"
    // [3]  status     "Up" / "Down"
    // [4]  vendorId
    // [5]  modelId
    // [6]  type
    // [7]  adminState
    // [8]  ?
    // [9]  ?
    // [10] distance   metros (enteros)
    // [11] temperature "38.00" o "--"
    // [12] voltage     "3.00"  o "--"
    // [13] txBias      "12.00" o "--"
    // [14] txPower     "1.72"  o "--"
    // [15] rxPower     "-22.52" o "--"
    // [16] lastUpTime  "2026-06-26 03:08:48"
    // [17] lastOffTime "2026-06-26 03:07:48"
    // [18] onlineFlag
    // [19] uptimeSegs
    // [20] ?
    // [21] ?
    // Total: 22 campos. Algunos firmwares usan 23 — autodetectar.

    // Autodetectar campos por fila probando 22 y 23
    let CAMPOS = 22;
    for (const c of [22, 23, 24]) {
        let ok = false;
        for (let i = 0; i + c <= vals.length; i += c) {
            if (/^0\/\d+:\d+$/.test(vals[i])) { ok = true; break; }
        }
        if (ok) { CAMPOS = c; break; }
    }

    const pNum = (s) => {
        if (!s || s === '--') return null;
        const n = parseFloat(s);
        return isNaN(n) ? null : n;
    };

    const NULL_DATE = "0000-00-00 00:00:00";
    const allOnts = [];

    for (let i = 0; i + CAMPOS <= vals.length; i += CAMPOS) {
        if (!/^0\/\d+:\d+$/.test(vals[i])) continue;
        const [ponPart, ontPart] = vals[i].split(":");
        const pon  = parseInt(ponPart.split("/")[1]);
        const ont  = parseInt(ontPart);
        const mac  = vals[i + 2].toUpperCase();
        const status = /^up$/i.test(vals[i + 3]) ? "working" : "offline";

        const distRaw = parseInt(vals[i + 10]);
        const distance = distRaw > 0 ? `${distRaw} m` : null;

        // Potencia y métricas ópticas — ya vienen en la lista principal
        const temperature = pNum(vals[i + 11]);
        const voltage     = pNum(vals[i + 12]);
        const txBias      = pNum(vals[i + 13]);
        const txPower     = pNum(vals[i + 14]);
        const rxPower     = pNum(vals[i + 15]);

        // Fechas
        const lastOnline  = vals[i + 16] && vals[i + 16] !== NULL_DATE ? vals[i + 16] : null;
        const lastOffline = vals[i + 17] && vals[i + 17] !== NULL_DATE ? vals[i + 17] : null;

        // Uptime en segundos → string legible
        const uptime = secsToUptime(vals[i + 19]);

        const nombre = (vals[i + 1] && vals[i + 1] !== "NA") ? vals[i + 1] : null;

        allOnts.push({
            pon, ont,
            macAddress: mac, serial: mac,
            status,
            rxPower, txPower, txBias, temperature, voltage,
            distance, uptime,
            firstOnlineTime: null,
            lastOnlineTime:  lastOnline,
            lastOfflineTime: lastOffline,
            type: "EPON", name: nombre
        });
    }

    // Calcular estadísticas por PON
    const activePons = [...new Set(allOnts.map(o => o.pon))].sort((a, b) => a - b);
    const pons = activePons.map(pId => {
        const pOnts    = allOnts.filter(o => o.pon === pId);
        const withPow  = pOnts.filter(o => o.rxPower !== null);
        const withTemp = pOnts.filter(o => o.temperature !== null);
        return {
            pon: pId,
            count:  pOnts.length,
            online: pOnts.filter(o => o.status === "working").length,
            avgPower:     withPow.length  ? parseFloat((withPow.reduce((s,o)=>s+o.rxPower,0)/withPow.length).toFixed(2)) : null,
            minPower:     withPow.length  ? parseFloat(Math.min(...withPow.map(o=>o.rxPower)).toFixed(2)) : null,
            maxPower:     withPow.length  ? parseFloat(Math.max(...withPow.map(o=>o.rxPower)).toFixed(2)) : null,
            criticalOnts: withPow.filter(o => o.rxPower < -28).length,
            avgTemp:      withTemp.length ? parseFloat((withTemp.reduce((s,o)=>s+o.temperature,0)/withTemp.length).toFixed(1)) : null,
            onts: pOnts
        };
    });

    const allWithPow  = allOnts.filter(o => o.rxPower !== null);
    const allWithTemp = allOnts.filter(o => o.temperature !== null);

    return {
        lastUpdate:     new Date().toISOString(),
        totalOnts:      allOnts.length,
        totalWorking:   allOnts.filter(o => o.status === "working").length,
        totalOffline:   allOnts.filter(o => o.status === "offline").length,
        totalCritical:  allWithPow.filter(o => o.rxPower < -28).length,
        avgTemperature: allWithTemp.length
            ? parseFloat((allWithTemp.reduce((s,o)=>s+o.temperature,0)/allWithTemp.length).toFixed(1))
            : null,
        pons,
        disconnectedOnts: allOnts.filter(o => o.status === "offline" && o.lastOnlineTime),
        partial: false, sourceType: "http"
    };
}

// Inyectar potencia óptica del caché Telnet anterior en datos HTTP frescos
function enrichWithCachedPower(httpData, prevCache) {
    if (!prevCache || !prevCache.pons) return httpData;
    const powerMap = new Map();
    prevCache.pons.forEach(pon => {
        pon.onts.forEach(ont => {
            if (ont.rxPower !== null)
                powerMap.set(`${pon.pon}:${ont.ont}`, {
                    rxPower: ont.rxPower, txPower: ont.txPower,
                    txBias: ont.txBias, temperature: ont.temperature, voltage: ont.voltage
                });
        });
    });
    httpData.pons.forEach(pon => {
        pon.onts.forEach(ont => {
            const key = `${pon.pon}:${ont.ont}`;
            if (powerMap.has(key)) Object.assign(ont, powerMap.get(key));
        });
        const withPow = pon.onts.filter(o => o.rxPower !== null);
        if (withPow.length > 0) {
            pon.avgPower     = parseFloat((withPow.reduce((s, o) => s + o.rxPower, 0) / withPow.length).toFixed(2));
            pon.minPower     = parseFloat(Math.min(...withPow.map(o => o.rxPower)).toFixed(2));
            pon.maxPower     = parseFloat(Math.max(...withPow.map(o => o.rxPower)).toFixed(2));
            pon.criticalOnts = withPow.filter(o => o.rxPower < -28).length;
        }
    });
    const allOnts = httpData.pons.flatMap(p => p.onts);
    const withTemp = allOnts.filter(o => o.temperature !== null);
    httpData.totalCritical  = allOnts.filter(o => o.rxPower !== null && o.rxPower < -28).length;
    httpData.avgTemperature = withTemp.length > 0
        ? parseFloat((withTemp.reduce((s, o) => s + o.temperature, 0) / withTemp.length).toFixed(1))
        : null;
    return httpData;
}

// Guardar historial de potencia desde datos HTTP (ya vienen completos en onuAllPonOnuList)
function saveOpticalHistory(oltId, data) {
    const ts = new Date().toISOString();
    if (!powerHistory[oltId]) powerHistory[oltId] = {};
    data.pons.forEach(pon => {
        pon.onts.forEach(ont => {
            if (ont.rxPower !== null) {
                const key = `${pon.pon}:${ont.ont}`;
                if (!powerHistory[oltId][key]) powerHistory[oltId][key] = [];
                powerHistory[oltId][key].push({ ts, rx: ont.rxPower, txPower: ont.txPower, temperature: ont.temperature });
                if (powerHistory[oltId][key].length > 288)
                    powerHistory[oltId][key] = powerHistory[oltId][key].slice(-288);
            }
        });
    });
    // NOTA: el guardado a disco (power_history.json) ya NO es automático.
    // Solo se persiste cuando el usuario lo pide explícitamente con el
    // botón "Guardar Local" (ver POST /api/save-local). Mientras tanto,
    // powerHistory vive solo en memoria.
}

// ── Función interna de scan ────────────────────────────────────────────────
// type "http"   → solo Telnet óptico background (lista viene de /api/onts directo)
// type "telnet" → Telnet completo original
async function runScan(oltId) {
    const olt = olts.find(o => o.id === oltId);
    if (!olt) throw new Error("OLT no encontrada");
    if (activeScans[oltId]) return;

    if (olt.type === "http") {
        // OLTs HTTP: los datos ópticos ya vienen en onuAllPonOnuList.asp — no necesita Telnet
        // El refresco real ocurre directo en /api/onts/:oltId
        const cached = ontCache[oltId];
        return cached ? cached.data : null;
    }

    activeScans[oltId] = true;
    broadcast("scan_started", { oltId, oltName: olt.name });

    try {
        const { scrapeOLT } = require("./generate_ultra_fast_cache.js");
        const result = await scrapeOLT(olt.host, olt.user, olt.password, SCAN_BUDGET_MS);

        ontCache[oltId] = { data: result, timestamp: Date.now() };
        // (sin escritura a disco aquí — ver nota de guardado manual más abajo)

        const ts = new Date().toISOString();
        if (!powerHistory[oltId]) powerHistory[oltId] = {};
        result.pons.forEach(pon => {
            pon.onts.forEach(ont => {
                if (ont.rxPower !== null) {
                    const key = `${pon.pon}:${ont.ont}`;
                    if (!powerHistory[oltId][key]) powerHistory[oltId][key] = [];
                    powerHistory[oltId][key].push({ ts, rx: ont.rxPower });
                    if (powerHistory[oltId][key].length > 288)
                        powerHistory[oltId][key] = powerHistory[oltId][key].slice(-288);
                }
            });
        });
        // NOTA: ontCache y powerHistory ahora viven solo en memoria durante
        // la operación normal. La escritura a ont_cache.json / power_history.json
        // es opcional y solo ocurre cuando el usuario presiona "💾 Guardar Local"
        // (POST /api/save-local), para no estar escribiendo a disco en cada scan.

        broadcast("scan_completed", { oltId, data: result });
        return result;
    } finally {
        delete activeScans[oltId];
    }
}

// ── API: OLTs CRUD ─────────────────────────────────────────────────────────
app.get("/api/olts", (req, res) => res.json({ success: true, olts }));

app.post("/api/olts", async (req, res) => {
    const { name, host, user, password } = req.body;
    if (!name || !host || !user || !password)
        return res.status(400).json({ success: false, message: "Faltan campos" });
    const newId = olts.length > 0 ? Math.max(...olts.map(o => o.id)) + 1 : 1;
    olts.push({ id: newId, name, host, user, password });
    await saveOLTs();
    res.json({ success: true, id: newId });
});

app.put("/api/olts/:id", async (req, res) => {
    const id = parseInt(req.params.id);
    const idx = olts.findIndex(o => o.id === id);
    if (idx === -1) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    olts[idx] = { ...olts[idx], ...req.body, id };
    await saveOLTs();
    res.json({ success: true });
});

app.delete("/api/olts/:id", async (req, res) => {
    const id = parseInt(req.params.id);
    olts = olts.filter(o => o.id !== id);
    delete ontCache[id];
    await saveOLTs();
    // ontCache ya no se persiste automáticamente; el borrado en memoria es
    // suficiente y se reflejará si el usuario hace "Guardar Local" después.
    res.json({ success: true });
});

// ── API: Scan ──────────────────────────────────────────────────────────────
app.get("/api/scan/:oltId", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    if (activeScans[oltId]) {
        const cached = ontCache[oltId];
        return res.json({ scanning: true, data: cached?.data || null });
    }

    try {
        const result = await runScan(oltId);
        res.json({ success: true, data: result });
    } catch (e) {
        console.error(`Error scan OLT ${oltId}:`, e.message);
        broadcast("scan_error", { oltId, error: e.message });
        res.status(500).json({ success: false, error: e.message });
    }
});

// ── API: ONTs ─────────────────────────────────────────────────────────────
// OLT HTTP  → scraping HTTP directo (<1s), potencia óptica del caché previo.
//             Telnet óptico se lanza en background → llega por WS scan_optical_ready
// OLT Telnet → comportamiento original con caché y scan background
app.get("/api/onts/:oltId", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    // ── OLT HTTP: respuesta instantánea siempre ────────────────────────────
    if (olt.type === "http") {
        try {
            const prevData = ontCache[oltId] ? ontCache[oltId].data : null;
            const httpData = await scrapeHTTP(olt.host, olt.user, olt.password);
            // Inyectar potencia óptica del caché anterior si existe
            const result = enrichWithCachedPower(httpData, prevData);
            // Guardar en caché EN MEMORIA para que /onu y el historial lo lean
            // (la escritura a disco es opcional, ver POST /api/save-local)
            ontCache[oltId] = { data: result, timestamp: Date.now() };
            // Guardar historial óptico (datos ya vienen en la lista HTTP) — en memoria
            saveOpticalHistory(oltId, result);
            return res.json(result);
        } catch (e) {
            console.error(`[/api/onts HTTP] OLT ${oltId}:`, e.message);
            // Si el HTTP falla, devolver caché si hay
            const cached = ontCache[oltId];
            if (cached && cached.data) return res.json({ ...cached.data, httpError: e.message });
            return res.status(500).json({ success: false, message: e.message });
        }
    }

    // ── OLT Telnet: comportamiento original ───────────────────────────────
    const cached = ontCache[oltId];
    const cacheValid = cached && (Date.now() - cached.timestamp < CACHE_TTL_MS);

    if (cacheValid) return res.json(cached.data);

    if (activeScans[oltId]) {
        return res.json(cached?.data || {
            lastUpdate: null, totalOnts: 0, totalWorking: 0,
            totalOffline: 0, totalCritical: 0, avgTemperature: null,
            pons: [], disconnectedOnts: [], scanning: true
        });
    }

    runScan(oltId).catch(e => {
        console.error(`Error scan background OLT ${oltId}:`, e.message);
        broadcast("scan_error", { oltId, error: e.message });
    });

    if (cached?.data) return res.json({ ...cached.data, scanning: true });

    res.json({
        lastUpdate: null, totalOnts: 0, totalWorking: 0,
        totalOffline: 0, totalCritical: 0, avgTemperature: null,
        pons: [], disconnectedOnts: [], scanning: true
    });
});

// ── API: Refresh forzado ───────────────────────────────────────────────────
app.post("/api/onts/:oltId/refresh", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    if (activeScans[oltId])
        return res.json({ success: true, message: "Escaneo ya en progreso" });

    if (ontCache[oltId]) ontCache[oltId].timestamp = 0;

    runScan(oltId).catch(e => {
        console.error(`Error refresh OLT ${oltId}:`, e.message);
        broadcast("scan_error", { oltId, error: e.message });
    });

    res.json({ success: true, message: "Escaneo iniciado" });
});

// ── API: Caché raw ─────────────────────────────────────────────────────────
app.get("/api/data/:oltId", (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const cached = ontCache[oltId];
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS)
        return res.json({ success: true, data: cached.data, cached: true });
    res.json({ success: false, message: "Cache expirado", data: cached?.data || null });
});

// ── API: Historial de potencia ─────────────────────────────────────────────
app.get("/api/history/:oltId/:pon/:ont", (req, res) => {
    const { oltId, pon, ont } = req.params;
    const key = `${pon}:${ont}`;
    const hist = (powerHistory[oltId] || {})[key] || [];
    res.json({ success: true, history: hist });
});

// ── API: Guardado manual en local (opcional) ───────────────────────────────
// ontCache y powerHistory viven solo en memoria mientras el servidor corre.
// Este endpoint es el ÚNICO lugar donde se escriben ont_cache.json y
// power_history.json a disco — se dispara cuando el usuario hace clic en
// el botón "💾 Guardar Local" del frontend, nunca automáticamente.
app.post("/api/save-local", async (req, res) => {
    try {
        await saveOntCache();
        await saveHistory();
        res.json({ success: true, message: "Datos guardados en local correctamente" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: ONTs no autorizadas (HiOSO HA7104) ───────────────────────────────
// Usa: enable > configure terminal > epon > interface epon 0/X > show attempt onu
app.get("/api/unauth/:oltId", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    try {
        console.log(`[unauth] Escaneando ONTs pendientes en ${olt.host}...`);
        const maxPon = parseInt(req.query.maxPon) || 8; // Obtener maxPon del query, por defecto 8
        const allUnauth = [];

        for (let ponNum = 1; ponNum <= maxPon; ponNum++) {
            try {
                const ponId = `0/${ponNum}`;
                const html  = await httpGetOLT(
                    olt.host, olt.user, olt.password,
                    `/onuAttemptOnuList.asp?oltponno=${encodeURIComponent(ponId)}`
                );
                const list = parseAttemptList(html);
                list.forEach(ont => allUnauth.push({ ...ont, pon: ponNum }));
            } catch (e) {
                console.warn(`[unauth] Error al escanear PON 0/${ponNum} en ${olt.host}: ${e.message}`);
            }
        }

        console.log(`[unauth] Encontradas: ${allUnauth.length}`);

        unauthOnts[oltId] = allUnauth;
        await saveUnauth();

        res.json({
            success: true,
            unauth: allUnauth,
            summary: {
                unauthorized: allUnauth.length,
                error: null
            }
        });
    } catch (e) {
        console.error(`[unauth] Error:`, e.message);
        res.json({
            success: false,
            unauth: unauthOnts[oltId] || [],
            error: e.message
        });
    }
});
app.post("/api/unauth/:oltId/authorize", async (req, res) => {
    try {
        const oltId = parseInt(req.params.oltId);
        const olt = olts.find(o => o.id === oltId);

        if (!olt)
            return res.status(404).json({ success: false, message: "OLT no encontrada" });

        const { onuId, onuMac } = req.body;

        if (!onuId || !onuMac)
            return res.status(400).json({
                success: false,
                message: "onuId y onuMac son obligatorios"
            });

  const result = await httpGetOLT(
    olt.host,
    olt.user,
    olt.password,
    "/goform/confirmAttemptOnu",
    8000,
    "POST",
    {
        onuId,
        onuMac: onuMac.toLowerCase()
    }
);

        res.json({
            success: true,
            message: "ONT autorizada correctamente",
            output: result
        });

    } catch (e) {
        res.status(500).json({
            success: false,
            message: e.message
        });
    }
});

// ── API: Reiniciar ONT (HTTP goform rebootOp) ─────────────────────────────
app.post("/api/restart/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        // Necesitamos el nombre actual de la ONT para el formulario
        const onuId  = `0/${pon}:${ont}`;
        const cached = ontCache[oltId]?.data?.pons
            ?.find(p => p.pon === parseInt(pon))?.onts
            ?.find(o => o.ont === parseInt(ont));
        const onuName = cached?.name || cached?.macAddress || "NA";
        await httpPostGoform(olt.host, olt.user, olt.password, onuId, onuName, "rebootOp");
        broadcast("onu_restarted", { pon, ont });
        res.json({ success: true, message: "ONT reiniciada" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Desactivar ONT (HTTP goform noactiveOp) ──────────────────────────
app.post("/api/deactivate/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const onuId  = `0/${pon}:${ont}`;
        const cached = ontCache[oltId]?.data?.pons
            ?.find(p => p.pon === parseInt(pon))?.onts
            ?.find(o => o.ont === parseInt(ont));
        const onuName = cached?.name || cached?.macAddress || "NA";
        await httpPostGoform(olt.host, olt.user, olt.password, onuId, onuName, "noactiveOp");
        broadcast("onu_deactivated", { pon, ont });
        res.json({ success: true, message: "ONT desactivada" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Activar ONT (HTTP goform activeOp) ────────────────────────────────
app.post("/api/activate/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const onuId  = `0/${pon}:${ont}`;
        const cached = ontCache[oltId]?.data?.pons
            ?.find(p => p.pon === parseInt(pon))?.onts
            ?.find(o => o.ont === parseInt(ont));
        const onuName = cached?.name || cached?.macAddress || "NA";
        await httpPostGoform(olt.host, olt.user, olt.password, onuId, onuName, "activeOp");
        broadcast("onu_activated", { pon, ont });
        res.json({ success: true, message: "ONT activada" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Limpiar flag (HTTP goform cleanLoopOp) ───────────────────────────
app.post("/api/cleanuopflag/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const onuId  = `0/${pon}:${ont}`;
        const cached = ontCache[oltId]?.data?.pons
            ?.find(p => p.pon === parseInt(pon))?.onts
            ?.find(o => o.ont === parseInt(ont));
        const onuName = cached?.name || cached?.macAddress || "NA";
        await httpPostGoform(olt.host, olt.user, olt.password, onuId, onuName, "cleanLoopOp");
        broadcast("onu_flag_cleared", { pon, ont });
        res.json({ success: true, message: "Flag limpiado" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Guardar nombre — solo vía HTTP goform (instantáneo, sin Telnet) ───
app.post("/api/ont/name", async (req, res) => {
    const { oltId, pon, ont, name } = req.body;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const onuId   = `0/${pon}:${ont}`;
        const postData = new URLSearchParams({
            onuId:        onuId,
            onuName:      name,
            onuOperation: "nonOp"
        }).toString();
        const auth = Buffer.from(`${olt.user}:${olt.password}`).toString("base64");

        await new Promise((resolve, reject) => {
            const options = {
                hostname: olt.host, port: 80,
                path:    "/goform/setOnu",
                method:  "POST",
                headers: {
                    "Content-Type":   "application/x-www-form-urlencoded",
                    "Content-Length": Buffer.byteLength(postData),
                    "Authorization":  `Basic ${auth}`,
                    "Referer":        `http://${olt.host}/onuConfig.asp`
                },
                timeout: 8000
            };
            const req2 = http.request(options, (r) => {
                let body = "";
                r.on("data", d => body += d);
                r.on("end", () => {
                    console.log(`[setOnu] ${olt.host} onuId=${onuId} HTTP ${r.statusCode}`);
                    resolve({ status: r.statusCode, body });
                });
            });
            req2.on("error", reject);
            req2.on("timeout", () => { req2.destroy(); reject(new Error("Timeout HTTP goform")); });
            req2.write(postData);
            req2.end();
        });

        // Actualizar nombre en caché local sin invalidar todo
        if (ontCache[oltId]?.data?.pons) {
            for (const p of ontCache[oltId].data.pons) {
                const o = p.onts?.find(o => o.pon === parseInt(pon) && o.ont === parseInt(ont));
                if (o) { o.name = name; break; }
            }
        }
        res.json({ success: true, message: "Nombre guardado" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── Helper: HTTP POST a /goform/setOnu ────────────────────────────────────
function httpPostGoform(host, user, password, onuId, onuName, onuOperation, timeoutMs = 8000) {
    return new Promise((resolve, reject) => {
        const postData = new URLSearchParams({ onuId, onuName, onuOperation }).toString();
        const auth = Buffer.from(`${user}:${password}`).toString("base64");
        const options = {
            hostname: host, port: 80,
            path: "/goform/setOnu",
            method: "POST",
            headers: {
                "Content-Type":   "application/x-www-form-urlencoded",
                "Content-Length": Buffer.byteLength(postData),
                "Authorization":  `Basic ${auth}`,
                "Referer":        `http://${host}/onuConfig.asp`
            },
            timeout: timeoutMs
        };
        const req = http.request(options, (res) => {
            let body = "";
            res.on("data", d => body += d);
            res.on("end", () => {
                console.log(`[goform] ${host} onuId=${onuId} op=${onuOperation} HTTP ${res.statusCode}`);
                resolve({ status: res.statusCode, body });
            });
        });
        req.on("error", reject);
        req.on("timeout", () => { req.destroy(); reject(new Error("Timeout HTTP goform")); });
        req.write(postData);
        req.end();
    });
}

// ── Helper: HTTP autenticado a la OLT (GET / POST) ─────────────────────────
function httpGetOLT(host, user, password, path, timeoutMs = 8000, method = "GET", postData = null) {
    return new Promise((resolve, reject) => {

        const auth = Buffer.from(`${user}:${password}`).toString("base64");

        const body = postData
            ? new URLSearchParams(postData).toString()
            : "";

        const options = {
            hostname: host,
            port: 80,
            path,
            method,
            headers: {
                "Authorization": `Basic ${auth}`,
                "Referer": `http://${host}/`
            },
            timeout: timeoutMs
        };

        if (method === "POST") {
            options.headers["Content-Type"] = "application/x-www-form-urlencoded";
            options.headers["Content-Length"] = Buffer.byteLength(body);
        }

        const req = http.request(options, (res) => {
            let raw = "";

            res.on("data", d => raw += d.toString());

            res.on("end", () => resolve(raw));
        });

        req.on("timeout", () => {
            req.destroy();
            reject(new Error(`HTTP timeout ${host}${path}`));
        });

        req.on("error", reject);

        if (method === "POST" && body)
            req.write(body);

        req.end();
    });
}

// ── Helper: parsear onuConfig.asp ─────────────────────────────────────────
// Página: http://<host>/onuConfig.asp?onuno=0/<pon>:<ont>&oltponno=0/<pon>
// Extrae: Mac Address, Name, Online Status, Activate Status, Operation,
//         First/Last UpTime, Last OffTime
// Métricas ópticas: Optical module Temperature, Voltage, Bias Current,
//                   Tx Power, Rx Power
function parseOnuConfig(html) {
    // Los datos vienen en arrays JS dentro del HTML, NO en las celdas de la tabla.
    // El firmware los inyecta con document.write() en onLoad.
    //
    // var onuinfo = new Array(
    //   [0] onuId,  [1] name,  [2] mac,  [3] status(Up/Down),
    //   [4] ?,      [5] ?,     [6] ?,
    //   [7] firstUpTime,  [8] lastUpTime,  [9] lastOffTime,
    //   [10..] otros
    // );
    // var onuOpmInfo = new Array(
    //   [0] onuId, [1] temperature, [2] voltage, [3] txBias, [4] txPower, [5] rxPower
    // );

    const parseNum = (s) => {
        if (!s) return null;
        const m = s.match(/(-?[\d]+(?:\.[\d]+)?)/);
        return m ? parseFloat(m[1]) : null;
    };

    // Extraer array JS: busca var NAME=new Array(\n'val1','val2',...\n);
    const extractArray = (varName) => {
        const re = new RegExp(
            'var\\s+' + varName + '\\s*=\\s*new\\s+Array\\s*\\(\\s*([\\s\\S]*?)\\s*\\)\\s*;',
            'i'
        );
        const m = html.match(re);
        if (!m) return null;
        // Extraer todos los strings entre comillas simples
        const vals = [];
        const strRe = /'([^']*)'/g;
        let sm;
        while ((sm = strRe.exec(m[1])) !== null) vals.push(sm[1]);
        return vals.length > 0 ? vals : null;
    };

    const info    = extractArray('onuinfo');
    const opmInfo = extractArray('onuOpmInfo');

    // Determinar activate status desde el HTML (lo pone con document.write basado en info[12])
    let activateStatus = null;
    if (info && info[12] !== undefined) {
        activateStatus = info[12] === '2' ? 'Deactivate' : 'Activate';
    }

    return {
        macAddress:     info ? info[2] || null : null,
        name:           info ? (info[1] || null) : null,
        onlineStatus:   info ? info[3] || null : null,   // 'Up' o 'Down'
        activateStatus,
        operation:      null,  // siempre '---' por defecto en la página
        firstUpTime:    info ? info[7] || null : null,
        lastUpTime:     info ? info[8] || null : null,
        lastOffTime:    info ? info[9] || null : null,
        temperature:    opmInfo ? parseNum(opmInfo[1]) : null,
        voltage:        opmInfo ? parseNum(opmInfo[2]) : null,
        txBias:         opmInfo ? parseNum(opmInfo[3]) : null,
        txPower:        opmInfo ? parseNum(opmInfo[4]) : null,
        rxPower:        opmInfo ? parseNum(opmInfo[5]) : null,
    };
}

// ── API: DEBUG — ver HTML crudo de onuConfig.asp ─────────────────────────
// GET /api/debug/ont/:oltId/:pon/:ont  → devuelve el HTML crudo para diagnosticar el parser
app.get("/api/debug/ont/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const onuId = `0/${pon}:${ont}`;
        const ponId = `0/${pon}`;
        const html = await httpGetOLT(
            olt.host, olt.user, olt.password,
            `/onuConfig.asp?onuno=${encodeURIComponent(onuId)}&oltponno=${encodeURIComponent(ponId)}`
        );
        res.set('Content-Type', 'text/plain; charset=utf-8');
        res.send(`=== URL: /onuConfig.asp?onuno=${onuId}&oltponno=${ponId} ===

${html}`);
    } catch (e) {
        res.status(500).send("Error: " + e.message);
    }
});

// ── API: Detalle instantáneo de ONT via HTTP (onuConfig.asp) ──────────────
// Reemplaza el Telnet óptico individual — respuesta en <1s
app.get("/api/optical/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const onuId  = `0/${pon}:${ont}`;
        const ponId  = `0/${pon}`;
        const html = await httpGetOLT(
            olt.host, olt.user, olt.password,
            `/onuConfig.asp?onuno=${encodeURIComponent(onuId)}&oltponno=${encodeURIComponent(ponId)}`
        );
        const parsed = parseOnuConfig(html);
        res.json({
            success: true,
            rxPower:     parsed.rxPower,
            txPower:     parsed.txPower,
            txBias:      parsed.txBias,
            temperature: parsed.temperature,
            voltage:     parsed.voltage,
            // campos extra del detalle
            macAddress:     parsed.macAddress,
            name:           parsed.name,
            onlineStatus:   parsed.onlineStatus,
            activateStatus: parsed.activateStatus,
            operation:      parsed.operation,
            firstUpTime:    parsed.firstUpTime,
            lastUpTime:     parsed.lastUpTime,
            lastOffTime:    parsed.lastOffTime,
        });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── Refresco masivo de potencia óptica (recorre ONTs Online una por una) ──
// Hace exactamente lo mismo que el botón "Refrescar potencia" de una sola
// ONT (onuConfig.asp), pero repetido para todas las ONTs Online de la OLT,
// en tandas pequeñas para no saturar el equipo. Pensado para OLTs cuyo
// listado masivo (onuAllPonOnuList.asp) nunca trae potencia (ej. Buena Vista).
const OPTICAL_REFRESH_CONCURRENCY = 3;
const OPTICAL_REFRESH_DELAY_MS = 150;

async function refreshAllOptical(oltId) {
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return;
    const cached = ontCache[oltId];
    if (!cached || !cached.data || !cached.data.pons) return;

    // Solo tiene sentido refrescar ONTs Online — las offline no reportan óptica
    const targets = [];
    cached.data.pons.forEach(pon => {
        pon.onts.forEach(ont => {
            if (ont.status === "working") targets.push(ont);
        });
    });

    activeOpticalRefresh[oltId] = true;
    broadcast("optical_refresh_started", { oltId, total: targets.length });

    let done = 0, updated = 0;
    try {
        for (let i = 0; i < targets.length; i += OPTICAL_REFRESH_CONCURRENCY) {
            const batch = targets.slice(i, i + OPTICAL_REFRESH_CONCURRENCY);
            await Promise.all(batch.map(async (ont) => {
                try {
                    const onuId = `0/${ont.pon}:${ont.ont}`;
                    const ponId = `0/${ont.pon}`;
                    const html = await httpGetOLT(
                        olt.host, olt.user, olt.password,
                        `/onuConfig.asp?onuno=${encodeURIComponent(onuId)}&oltponno=${encodeURIComponent(ponId)}`
                    );
                    const parsed = parseOnuConfig(html);
                    if (parsed.rxPower !== null) {
                        ont.rxPower     = parsed.rxPower;
                        ont.txPower     = parsed.txPower;
                        ont.txBias      = parsed.txBias;
                        ont.temperature = parsed.temperature;
                        ont.voltage     = parsed.voltage;
                        updated++;
                    }
                } catch (e) {
                    // Si una ONT puntual falla (timeout, etc.), seguimos con las demás
                }
                done++;
            }));
            broadcast("optical_refresh_progress", { oltId, done, total: targets.length });
            if (i + OPTICAL_REFRESH_CONCURRENCY < targets.length)
                await new Promise(r => setTimeout(r, OPTICAL_REFRESH_DELAY_MS));
        }

        // Recalcular estadísticas por PON y totales con los valores nuevos
        cached.data.pons.forEach(pon => {
            const withPow  = pon.onts.filter(o => o.rxPower !== null);
            const withTemp = pon.onts.filter(o => o.temperature !== null);
            pon.avgPower     = withPow.length  ? parseFloat((withPow.reduce((s,o)=>s+o.rxPower,0)/withPow.length).toFixed(2)) : null;
            pon.minPower     = withPow.length  ? parseFloat(Math.min(...withPow.map(o=>o.rxPower)).toFixed(2)) : null;
            pon.maxPower     = withPow.length  ? parseFloat(Math.max(...withPow.map(o=>o.rxPower)).toFixed(2)) : null;
            pon.criticalOnts = withPow.filter(o => o.rxPower < -28).length;
            pon.avgTemp      = withTemp.length ? parseFloat((withTemp.reduce((s,o)=>s+o.temperature,0)/withTemp.length).toFixed(1)) : null;
        });
        const allOnts     = cached.data.pons.flatMap(p => p.onts);
        const allWithPow  = allOnts.filter(o => o.rxPower !== null);
        const allWithTemp = allOnts.filter(o => o.temperature !== null);
        cached.data.totalCritical  = allWithPow.filter(o => o.rxPower < -28).length;
        cached.data.avgTemperature = allWithTemp.length
            ? parseFloat((allWithTemp.reduce((s,o)=>s+o.temperature,0)/allWithTemp.length).toFixed(1))
            : null;
        cached.data.lastUpdate = new Date().toISOString();
        cached.timestamp = Date.now();

        // Historial en memoria (el guardado a disco sigue siendo manual, ver /api/save-local)
        saveOpticalHistory(oltId, cached.data);

        broadcast("optical_refresh_completed", { oltId, updated, total: targets.length, data: cached.data });
    } finally {
        delete activeOpticalRefresh[oltId];
    }
}

// POST /api/onts/:oltId/refresh-optical → dispara el refresco masivo en background
app.post("/api/onts/:oltId/refresh-optical", (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    if (activeOpticalRefresh[oltId])
        return res.json({ success: true, message: "Ya hay una actualización de potencia en curso" });
    if (!ontCache[oltId] || !ontCache[oltId].data)
        return res.status(400).json({ success: false, message: "Primero carga los datos de la OLT" });

    refreshAllOptical(oltId).catch(e => {
        delete activeOpticalRefresh[oltId];
        console.error(`Error refrescando óptica OLT ${oltId}:`, e.message);
        broadcast("optical_refresh_error", { oltId, error: e.message });
    });

    res.json({ success: true, message: "Actualización de potencia iniciada" });
});

// ── API: Lista de ONTs autorizadas por PON (onuMacAuthedOnuList.asp) ───────
// Guarda en authed_onts.json para historial/consulta
// GET /api/authed/:oltId/:pon  → consulta un PON específico
// GET /api/authed/:oltId       → consulta todos los PONs (1-8)
const AUTHED_FILE = path.join(__dirname, "authed_onts.json");
let authedOnts = {};

async function loadAuthed() {
    try { const d = await fs.readFile(AUTHED_FILE, "utf8"); authedOnts = JSON.parse(d); }
    catch { authedOnts = {}; }
}
async function saveAuthed() {
    try { await fs.writeFile(AUTHED_FILE, JSON.stringify(authedOnts, null, 2)); }
    catch (e) { console.error("Error guardando authed_onts:", e.message); }
}
function parseAuthedList(html) {
    const list = [];

    const match = html.match(/var\s+ponOnuTable\s*=\s*new\s+Array\s*\(([\s\S]*?)\);/i);
    if (!match) return list;

    const data = match[1];

    const re = /'([^']+)'\s*,\s*'([0-9A-Fa-f:]{17})'/g;
    let m;

    while ((m = re.exec(data)) !== null) {
        list.push({
            onuId: m[1].trim(),
            onuMac: m[2].toUpperCase()
        });
    }

    return list;
}

// Attempt usa exactamente el mismo formato
function parseAttemptList(html) {
    return parseAuthedList(html);
}

app.get("/api/authed/:oltId/:pon", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const pon = req.params.pon;

    const olt = olts.find(o => o.id === oltId);

    if (!olt) {
        return res.status(404).json({
            success: false,
            message: "OLT no encontrada"
        });
    }

    try {

        const ponId = `0/${pon}`;

        const html = await httpGetOLT(
            olt.host,
            olt.user,
            olt.password,
            `/onuMacAuthedOnuList.asp?oltponno=${encodeURIComponent(ponId)}`
        );

        const list = parseAuthedList(html);

        if (!authedOnts[oltId])
            authedOnts[oltId] = {};

        authedOnts[oltId][pon] = {
            list,
            updatedAt: new Date().toISOString()
        };

        saveAuthed().catch(console.error);

        return res.json({
            success: true,
            pon: ponId,
            total: list.length,
            list
        });

    } catch (e) {

        const cached = authedOnts[oltId]?.[pon];

        if (cached) {
            return res.json({
                success: true,
                cached: true,
                pon: `0/${pon}`,
                total: cached.list.length,
                list: cached.list
            });
        }

        return res.status(500).json({
            success: false,
            message: e.message
        });
    }
});

// GET /api/authed/:oltId  → consulta los PONs reales de la OLT (maxPon del query param)
app.get("/api/authed/:oltId", async (req, res) => {
    const oltId  = parseInt(req.params.oltId);
    const olt    = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    // El frontend pasa maxPon = mayor PON conocido del caché (ej. 8 para una OLT de 8 puertos)
    const maxPon = Math.min(parseInt(req.query.maxPon) || 8, 16);

    const results = {};
    for (let ponNum = 1; ponNum <= maxPon; ponNum++) {
        try {
            const ponId = `0/${ponNum}`;
            const html  = await httpGetOLT(
                olt.host, olt.user, olt.password,
                `/onuMacAuthedOnuList.asp?oltponno=${encodeURIComponent(ponId)}`
            );
            const list = parseAuthedList(html);
            results[ponNum] = { list, total: list.length };
            if (!authedOnts[oltId]) authedOnts[oltId] = {};
            authedOnts[oltId][ponNum] = { list, updatedAt: new Date().toISOString() };
        } catch (e) {
            const cached = (authedOnts[oltId] || {})[ponNum];
            results[ponNum] = cached
                ? { list: cached.list, total: cached.list.length, cached: true }
                : { list: [], total: 0, error: e.message };
            if (ponNum === 1 && !cached) break;
        }
    }
    saveAuthed().catch(() => {});
    res.json({ success: true, pons: results });
});

// ── API: ONTs intentando (onuAttemptOnuList.asp) ──────────────────────────
// GET /api/attempt/:oltId  → recorre los PONs reales de la OLT (maxPon del query)
app.get("/api/attempt/:oltId", async (req, res) => {
    const oltId  = parseInt(req.params.oltId);
    const olt    = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    const maxPon = Math.min(parseInt(req.query.maxPon) || 8, 16);

    const results = {};
    for (let ponNum = 1; ponNum <= maxPon; ponNum++) {
        try {
            const ponId = `0/${ponNum}`;
            const html  = await httpGetOLT(
                olt.host, olt.user, olt.password,
                `/onuAttemptOnuList.asp?oltponno=${encodeURIComponent(ponId)}`
            );
            const list = parseAttemptList(html);
            results[ponNum] = { list, total: list.length };
        } catch (e) {
            results[ponNum] = { list: [], total: 0, error: e.message };
            if (ponNum === 1) break;
        }
    }
    res.json({ success: true, pons: results });
});

// ── API: Eliminar ONT autorizada (goform/deletePonMacAuthedOnu) ────────────
// DELETE /api/authed/:oltId  body: { onuId, onuMac }
app.delete("/api/authed/:oltId", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt   = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    const { onuId, onuMac } = req.body;
    if (!onuId || !onuMac)
        return res.status(400).json({ success: false, message: "Faltan parámetros: onuId y onuMac son obligatorios" });

    try {
        const postData = new URLSearchParams({ onuId, onuMac }).toString();
        const auth     = Buffer.from(`${olt.user}:${olt.password}`).toString("base64");
        await new Promise((resolve, reject) => {
            const options = {
                hostname: olt.host, port: 80,
                path:    "/goform/deletePonMacAuthedOnu",
                method:  "POST",
                headers: {
                    "Content-Type":   "application/x-www-form-urlencoded",
                    "Content-Length": Buffer.byteLength(postData),
                    "Authorization":  `Basic ${auth}`,
                    "Referer":        `http://${olt.host}/onuMacAuthedOnuList.asp`
                },
                timeout: 8000
            };
            const req2 = http.request(options, (r) => {
                let body = "";
                r.on("data", d => body += d);
                r.on("end", () => {
                    console.log(`[deleteAuthed] ${olt.host} onuId=${onuId} onuMac=${onuMac} HTTP ${r.statusCode}`);
                    resolve({ status: r.statusCode, body });
                });
            });
            req2.on("error", reject);
            req2.on("timeout", () => { req2.destroy(); reject(new Error("Timeout HTTP goform")); });
            req2.write(postData);
            req2.end();
        });

        // Quitar del caché local de authedOnts
        if (authedOnts[oltId]) {
            for (const pon of Object.keys(authedOnts[oltId])) {
                const entry = authedOnts[oltId][pon];
                if (entry && entry.list) {
                    entry.list = entry.list.filter(o => o.onuId !== onuId && o.mac !== onuMac.toUpperCase());
                }
            }
            saveAuthed().catch(() => {});
        }

        broadcast("ont_authed_deleted", { oltId, onuId, onuMac });
        res.json({ success: true, message: `ONT ${onuId} (${onuMac}) eliminada de autorizadas` });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── Inicialización ─────────────────────────────────────────────────────────
Promise.all([loadOLTs(), loadHistory(), loadUnauth(), loadOntCache(), loadAuthed()]).then(() => {
    server.listen(PORT, () => {
        console.log(`✅ Servidor en http://localhost:${PORT}`);
        console.log(`🔌 WebSocket en ws://localhost:${PORT}/ws`);
        console.log(`📊 OLTs cargadas: ${olts.length}`);
    });
});

module.exports = app;