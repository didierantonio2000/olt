const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs").promises;
const path = require("path");
const http = require("http");
const https = require("https");
const WebSocket = require("ws");
const net = require("net");

const app = express();
const PORT = 3000;
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
async function detectUnauthEPON(host, user, password, maxPons = 8) {
    return new Promise((resolve) => {
        const socket = net.createConnection({ host, port: 23 });
        let buf = "";
        let debugLog = "";
        let done = false;

        const log = (s) => { debugLog += s + "\n"; };

        const finish = (result) => {
            if (done) return;
            done = true;
            clearTimeout(globalTimer);
            try { socket.write("exit\r\n"); socket.end(); } catch (_) {}
            resolve({ ...result, debugLog });
        };

        // 45s total: 8 PONs × ~4s c/u + auth ~6s
        const globalTimer = setTimeout(() => {
            log("[TIMEOUT GLOBAL — devolviendo lo que se obtuvo]");
            finish({ unauthList: [], error: "Timeout global" });
        }, 45000);

        socket.on("data", (d) => {
            buf += d.toString();
            // Responder paginación automáticamente
            if (buf.includes("-- Enter Key To Continue --") || buf.includes("--More--"))
                socket.write(" ");
        });

        // waitBuf: espera el patrón OR timeout — nunca rechaza
        const waitBuf = (pattern, ms = 6000) => new Promise((res) => {
            const t = Date.now();
            const tick = () => {
                if (pattern.test(buf) || Date.now() - t > ms) {
                    const out = buf; buf = ""; res(out);
                } else setTimeout(tick, 80);
            };
            tick();
        });

        socket.on("connect", async () => {
            try {
                // ── Auth ────────────────────────────────────────────────────
                let out = await waitBuf(/Username:/i, 8000);
                log("LOGIN PROMPT:\n" + out.trim());
                socket.write(`${user}\r\n`);

                out = await waitBuf(/Password:/i, 6000);
                socket.write(`${password}\r\n`);

                // Post-login: puede ser EPON> o EPON#
                out = await waitBuf(/EPON[>#]/, 6000);
                log("POST-LOGIN:\n" + out.trim());

                if (/EPON>/.test(out)) {
                    socket.write("enable\r\n");
                    out = await waitBuf(/EPON#/, 5000);
                    log("POST-ENABLE:\n" + out.trim());
                }

                // ── configure terminal → epon ───────────────────────────────
                socket.write("configure terminal\r\n");
                out = await waitBuf(/EPON[^>]*#/, 4000);
                log("configure terminal:\n" + out.trim());

                socket.write("epon\r\n");
                out = await waitBuf(/EPON\(epon\)#/, 5000);
                log("epon:\n" + out.trim());

                // ── Loop por PONs ────────────────────────────────────────────
                const unauthList = [];

                for (let p = 1; p <= maxPons; p++) {
                    // Entramos a interface epon 0/P desde EPON(epon)#
                    socket.write(`interface epon 0/${p}\r\n`);
                    out = await waitBuf(/EPON[^>]*#/, 4000);
                    log(`\ninterface epon 0/${p}:\n${out.trim()}`);

                    if (isFirmwareError(out)) {
                        const range = parseEponRange(out);
                        if (range) {
                            log(`  → Rango válido de PON en esta OLT: ${range.min}~${range.max}. Deteniendo escaneo (no hay más puertos físicos).`);
                            break; // ya sabemos el límite real, no seguir
                        }
                        log(`  → PON ${p} no disponible, continuando: ${out.trim().split("\n").find(l => l.startsWith("%")) || out.trim()}`);
                        continue;
                    }

                    // Estamos dentro de EPON(epon_0/P)# — pedir ONTs pendientes
                    socket.write("show attempt onu\r\n");
                    out = await waitBuf(/EPON\(epon_0\/\d+\)#/, 5000);
                    log(`show attempt onu PON ${p}:\n${out}`);

                    // Extraer MACs de la salida
                    const macRegex = /([0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5})/gi;
                    let m;
                    while ((m = macRegex.exec(out)) !== null) {
                        const mac = m[1].toUpperCase();
                        if (mac === "FF:FF:FF:FF:FF:FF") continue;
                        if (mac.startsWith("01:") || mac.startsWith("33:")) continue;
                        if (!unauthList.find(u => u.mac === mac)) {
                            unauthList.push({ mac, pon: p, detectedAt: new Date().toISOString() });
                            log(`  ✓ NO AUTORIZADA en PON ${p}: ${mac}`);
                        }
                    }

                    // Salir de EPON(epon_0/P)# → volver a EPON(epon)#
                    socket.write("exit\r\n");
                    out = await waitBuf(/EPON\(epon\)#/, 4000);
                    log(`exit → EPON(epon)#:\n${out.trim()}`);
                }

                log(`\n=== TOTAL no autorizadas: ${unauthList.length} ===`);
                finish({ unauthList, error: null });

            } catch (e) {
                log("EXCEPCION: " + e.message);
                finish({ unauthList: [], error: e.message });
            }
        });

        socket.on("error", (e) => {
            log("SOCKET ERROR: " + e.message);
            finish({ unauthList: [], error: e.message });
        });
        socket.on("timeout", () => {
            log("SOCKET TIMEOUT");
            finish({ unauthList: [], error: "TCP timeout" });
        });
    });
}


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

    // Autodetectar si son 22 o 23 campos por fila
    let CAMPOS = 22;
    for (const c of [22, 23]) {
        for (let i = 0; i + c <= vals.length; i += c) {
            if (/^0\/\d+:\d+$/.test(vals[i])) { CAMPOS = c; break; }
        }
        if (CAMPOS === c) break;
    }

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
        const uptime = secsToUptime(vals[i + 19]);
        const NULL_DATE = "0000-00-00 00:00:00";
        const lastOnline  = vals[i + 16] && vals[i + 16] !== NULL_DATE ? vals[i + 16] : null;
        const firstOnline = vals[i + 17] && vals[i + 17] !== NULL_DATE ? vals[i + 17] : null;
        const nombre = (vals[i + 1] && vals[i + 1] !== "NA") ? vals[i + 1] : null;
        allOnts.push({
            pon, ont,
            macAddress: mac, serial: mac,
            status,
            rxPower: null, txPower: null, txBias: null,
            temperature: null, voltage: null,
            distance, uptime,
            firstOnlineTime: firstOnline,
            lastOnlineTime:  lastOnline,
            lastOfflineTime: null,
            type: "EPON", name: nombre
        });
    }

    const activePons = [...new Set(allOnts.map(o => o.pon))].sort((a, b) => a - b);
    const pons = activePons.map(pId => {
        const pOnts = allOnts.filter(o => o.pon === pId);
        return {
            pon: pId, count: pOnts.length,
            online: pOnts.filter(o => o.status === "working").length,
            avgPower: null, minPower: null, maxPower: null,
            criticalOnts: 0, avgTemp: null, onts: pOnts
        };
    });

    return {
        lastUpdate: new Date().toISOString(),
        totalOnts: allOnts.length,
        totalWorking: allOnts.filter(o => o.status === "working").length,
        totalOffline: allOnts.filter(o => o.status === "offline").length,
        totalCritical: 0, avgTemperature: null,
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

// Lanzar Telnet óptico en background para OLT HTTP y actualizar caché + WS
function launchOpticalBackground(oltId, olt, baseResult) {
    const onlineCount = baseResult.pons.reduce((s, p) => s + p.online, 0);
    if (onlineCount === 0) return;
    setImmediate(async () => {
        try {
            const { scrapeOLT } = require("./generate_ultra_fast_cache.js");
            const telnetResult = await scrapeOLT(olt.host, olt.user, olt.password, SCAN_BUDGET_MS);
            const merged = enrichWithCachedPower(baseResult, telnetResult);
            merged.lastUpdate = new Date().toISOString();
            // Guardar potencia en historial
            const ts = new Date().toISOString();
            if (!powerHistory[oltId]) powerHistory[oltId] = {};
            telnetResult.pons.forEach(pon => {
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
            await saveHistory();
            // Actualizar caché con potencia real
            ontCache[oltId] = { data: merged, timestamp: Date.now() };
            await saveOntCache();
            broadcast("scan_optical_ready", { oltId, data: merged });
        } catch (e) {
            console.error(`[optical bg] OLT ${oltId}:`, e.message);
        }
    });
}

// ── Función interna de scan ────────────────────────────────────────────────
// type "http"   → solo Telnet óptico background (lista viene de /api/onts directo)
// type "telnet" → Telnet completo original
async function runScan(oltId) {
    const olt = olts.find(o => o.id === oltId);
    if (!olt) throw new Error("OLT no encontrada");
    if (activeScans[oltId]) return;

    if (olt.type === "http") {
        // Para HTTP el scan solo sirve para refrescar potencia óptica vía Telnet
        // La lista siempre se obtiene directo en /api/onts sin pasar por aquí
        const cached = ontCache[oltId];
        if (cached && cached.data) launchOpticalBackground(oltId, olt, cached.data);
        return cached ? cached.data : null;
    }

    activeScans[oltId] = true;
    broadcast("scan_started", { oltId, oltName: olt.name });

    try {
        const { scrapeOLT } = require("./generate_ultra_fast_cache.js");
        const result = await scrapeOLT(olt.host, olt.user, olt.password, SCAN_BUDGET_MS);

        ontCache[oltId] = { data: result, timestamp: Date.now() };
        await saveOntCache();

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
        await saveHistory();

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
    await saveOntCache();
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
            // Guardar en caché para que /onu y el historial lo lean
            ontCache[oltId] = { data: result, timestamp: Date.now() };
            saveOntCache().catch(() => {});
            // Lanzar Telnet óptico en background (actualiza WS cuando termina)
            launchOpticalBackground(oltId, olt, result);
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

// ── API: ONTs no autorizadas (HiOSO HA7104) ───────────────────────────────
// Usa: enable > configure terminal > epon > interface epon 0/X > show attempt onu
app.get("/api/unauth/:oltId", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    try {
        console.log(`[unauth] Escaneando ONTs pendientes en ${olt.host}...`);
        const { unauthList, debugLog, error } = await detectUnauthEPON(
            olt.host, olt.user, olt.password, 8
        );

        console.log(`[unauth] Encontradas: ${unauthList.length}`);

        unauthOnts[oltId] = unauthList;
        await saveUnauth();

        res.json({
            success: true,
            unauth: unauthList,
            rawOutput: debugLog,
            summary: {
                unauthorized: unauthList.length,
                error: error || null
            }
        });
    } catch (e) {
        console.error(`[unauth] Error:`, e.message);
        res.json({
            success: true,
            unauth: unauthOnts[oltId] || [],
            rawOutput: e.message,
            error: e.message
        });
    }
});

// ── API: Autorizar ONT (HiOSO HA7104) ────────────────────────────────────
// Flujo actual (NO confirmado todavía con la consola real del OLT):
//   enable → configure terminal → epon → interface epon 0/<pon>
//   whitelist add <mac>
// El prompt dentro del PON es EPON(epon_0/X)#
//
// ⚠️ Pendiente: confirmar con "interface epon 0/X" + "?" en la consola
// real cuál es el comando exacto de autorización en este firmware.
// Mientras tanto se deja "whitelist add <mac>" tal como estaba.
app.post("/api/unauth/:oltId/authorize", async (req, res) => {
    const oltId = parseInt(req.params.oltId);
    const olt = olts.find(o => o.id === oltId);
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });

    const { pon, mac } = req.body;
    if (!pon || !mac)
        return res.status(400).json({ success: false, message: "Faltan parámetros: pon y mac son obligatorios" });

    const macClean = mac.trim().toLowerCase();

    return new Promise((resolveReq) => {
        const socket = net.createConnection({ host: olt.host, port: 23 });
        let buf = "";
        let debugLog = "";
        let responded = false; // evita mandar la respuesta HTTP dos veces (timeout + éxito casi simultáneo)

        const log = (s) => { debugLog += s + "\n"; console.log(`[authorize] ${s}`); };

        const respond = (statusCode, body, success) => {
            if (responded) return;
            responded = true;
            clearTimeout(globalTimer);
            try { socket.write("exit\r\n"); socket.end(); } catch (_) {}
            if (success) {
                if (unauthOnts[oltId])
                    unauthOnts[oltId] = unauthOnts[oltId].filter(u => u.mac !== mac.toUpperCase());
                saveUnauth().catch(() => {});
                if (ontCache[oltId]) ontCache[oltId].timestamp = 0;
                broadcast("ont_authorized", { oltId, pon, mac: macClean });
            }
            resolveReq(res.status(statusCode).json(body));
        };

        const globalTimer = setTimeout(() => {
            respond(500, { success: false, message: "Timeout al autorizar", output: debugLog }, false);
        }, 30000);

        socket.on("data", (d) => {
            buf += d.toString();
            if (buf.includes("-- Enter Key To Continue --") || buf.includes("--More--"))
                socket.write(" ");
        });

        const waitBuf = (pattern, ms = 6000) => new Promise((res2) => {
            const t = Date.now();
            const tick = () => {
                if (pattern.test(buf) || Date.now() - t > ms) {
                    const out = buf; buf = ""; res2(out);
                } else setTimeout(tick, 80);
            };
            tick();
        });

        socket.on("connect", async () => {
            try {
                let out;

                // Auth
                out = await waitBuf(/Username:/i, 8000);
                log("USERNAME: " + out.trim());
                socket.write(`${olt.user}\r\n`);

                out = await waitBuf(/Password:/i, 6000);
                socket.write(`${olt.password}\r\n`);

                out = await waitBuf(/EPON[>#]/, 6000);
                log("POST-LOGIN: " + out.trim());

                if (/EPON>/.test(out)) {
                    socket.write("enable\r\n");
                    out = await waitBuf(/EPON#/, 5000);
                    log("POST-ENABLE: " + out.trim());
                }

                // configure terminal
                socket.write("configure terminal\r\n");
                out = await waitBuf(/EPON[^>]*#/, 4000);
                log("configure terminal: " + out.trim());

                // epon
                socket.write("epon\r\n");
                out = await waitBuf(/EPON\(epon\)#/, 5000);
                log("epon: " + out.trim());

                // interface epon 0/<pon>
                socket.write(`interface epon 0/${pon}\r\n`);
                out = await waitBuf(/EPON[^>]*#/, 5000);
                log(`interface epon 0/${pon}: ${out.trim()}`);

                if (isFirmwareError(out)) {
                    return respond(200, {
                        success: false,
                        message: `La OLT rechazó el PON ${pon}: ${out.trim().split("\n").find(l => l.startsWith("%")) || out.trim()}`,
                        output: debugLog
                    }, false);
                }

                // whitelist add <mac>
                socket.write(`whitelist add ${macClean}\r\n`);
                out = await waitBuf(/EPON\(epon_0\/\d+\)#/, 5000);
                log(`whitelist add ${macClean}: ${out.trim()}`);

                if (isFirmwareError(out)) {
                    return respond(200, {
                        success: false,
                        message: `La OLT rechazó el comando whitelist add: ${out.trim().split("\n").find(l => l.startsWith("%")) || out.trim()}`,
                        output: debugLog
                    }, false);
                }

                // Salir hasta EPON# para poder hacer write
                socket.write("exit\r\n");  // sale de epon_0/X → epon
                out = await waitBuf(/EPON\(epon\)#/, 3000);
                log(`exit (epon): ${out.trim()}`);

                socket.write("exit\r\n");  // sale de epon → config
                out = await waitBuf(/EPON[^>]*#/, 3000);
                log(`exit (config): ${out.trim()}`);

                socket.write("exit\r\n");  // sale de config → enable
                out = await waitBuf(/EPON#/, 3000);
                log(`exit (enable): ${out.trim()}`);

                // Guardar configuración en flash
                socket.write("write\r\n");
                out = await waitBuf(/EPON#/, 6000);
                log(`write: ${out.trim()}`);

                respond(200, {
                    success: true,
                    message: `ONT ${macClean} autorizada en PON ${pon}`,
                    output: debugLog
                }, true);

            } catch (e) {
                log("EXCEPCION: " + e.message);
                respond(500, { success: false, message: e.message, output: debugLog }, false);
            }
        });

        socket.on("error", (e) => {
            log("SOCKET ERROR: " + e.message);
            respond(500, { success: false, message: e.message, output: debugLog }, false);
        });
    });
});

// ── API: Reiniciar ONT ─────────────────────────────────────────────────────
app.post("/api/restart/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        await telnetCommand(olt.host, olt.user, olt.password, [
            `interface epon 0/${pon}`,
            `onu ${ont} admin-state down`,
            `onu ${ont} admin-state up`,
            "exit"
        ]);
        broadcast("onu_restarted", { pon, ont });
        res.json({ success: true, message: "ONT reiniciada" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Desactivar ONT ────────────────────────────────────────────────────
app.post("/api/deactivate/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        await telnetCommand(olt.host, olt.user, olt.password, [
            `interface epon 0/${pon}`,
            `onu ${ont} admin-state down`,
            "exit"
        ]);
        broadcast("onu_deactivated", { pon, ont });
        res.json({ success: true, message: "ONT desactivada" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Limpiar flag ──────────────────────────────────────────────────────
app.post("/api/cleanuopflag/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        await telnetCommand(olt.host, olt.user, olt.password, [
            `interface epon 0/${pon}`,
            `onu ${ont} cleanuopflag`,
            "exit"
        ]);
        broadcast("onu_flag_cleared", { pon, ont });
        res.json({ success: true, message: "Flag limpiado" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Guardar nombre ────────────────────────────────────────────────────
app.post("/api/ont/name", async (req, res) => {
    const { oltId, pon, ont, name } = req.body;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        // 1) Guardar nombre vía Telnet (CLI)
        await telnetCommand(olt.host, olt.user, olt.password, [
            `interface epon 0/${pon}`,
            `onu ${ont} name ${name}`,
            "exit"
        ]);

        // 2) Aplicar la operación vía formulario web de la OLT
        //    El campo onuId usa formato "0/<pon>:<ont>" según la interfaz web
        const onuId = `0/${pon}:${ont}`;
        const formBody = new URLSearchParams({
            onuId:        onuId,
            onuName:      name,
            onuOperation: "nonOp"   // "nonOp" confirma el nombre sin cambiar el estado operativo
        });

        await new Promise((resolve, reject) => {
            const postData = formBody.toString();
            const auth = Buffer.from(`${olt.user}:${olt.password}`).toString("base64");
            const options = {
                hostname: olt.host,
                port:     80,
                path:     "/goform/setOnu",
                method:   "POST",
                headers: {
                    "Content-Type":   "application/x-www-form-urlencoded",
                    "Content-Length": Buffer.byteLength(postData),
                    "Authorization":  `Basic ${auth}`,
                    "Referer":        `http://${olt.host}/onu_detail.asp`
                },
                timeout: 8000
            };
            const req2 = http.request(options, (r) => {
                let body = "";
                r.on("data", d => body += d);
                r.on("end", () => {
                    console.log(`[setOnu] ${olt.host} onuId=${onuId} status=${r.statusCode}`);
                    resolve({ status: r.statusCode, body });
                });
            });
            req2.on("error", (e) => {
                console.warn(`[setOnu] Advertencia HTTP goform: ${e.message} — nombre ya guardado vía Telnet`);
                resolve({ status: 0, error: e.message }); // no bloquear si el HTTP falla
            });
            req2.on("timeout", () => {
                req2.destroy();
                console.warn(`[setOnu] Timeout HTTP goform — nombre ya guardado vía Telnet`);
                resolve({ status: 0, error: "timeout" }); // idem
            });
            req2.write(postData);
            req2.end();
        });

        if (ontCache[oltId]) ontCache[oltId].timestamp = 0;
        res.json({ success: true, message: "Nombre guardado y aplicado en OLT" });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── API: Potencia óptica individual ───────────────────────────────────────
app.get("/api/optical/:oltId/:pon/:ont", async (req, res) => {
    const { oltId, pon, ont } = req.params;
    const olt = olts.find(o => o.id === parseInt(oltId));
    if (!olt) return res.status(404).json({ success: false, message: "OLT no encontrada" });
    try {
        const results = await telnetCommand(olt.host, olt.user, olt.password, [
            `show onu optical-ddm epon 0/${pon} ${ont}`
        ]);
        const out = results.join("\n");
        const rxM = out.match(/RxPower\s*[:\-]?\s*(-?[\d.]+)\s*dBm/i);
        const txM = out.match(/TxPower\s*[:\-]?\s*(-?[\d.]+)\s*dBm/i);
        const biasM = out.match(/TxBias\s*[:\-]?\s*([\d.]+)\s*mA/i);
        const tempM = out.match(/Temperature\s*[:\-]?\s*([\d.]+)\s*C/i);
        const voltM = out.match(/Voltage\s*[:\-]?\s*([\d.]+)\s*V/i);
        res.json({
            success: true,
            rxPower: rxM ? parseFloat(rxM[1]) : null,
            txPower: txM ? parseFloat(txM[1]) : null,
            txBias: biasM ? parseFloat(biasM[1]) : null,
            temperature: tempM ? parseFloat(tempM[1]) : null,
            voltage: voltM ? parseFloat(voltM[1]) : null,
            raw: out
        });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ── Inicialización ─────────────────────────────────────────────────────────
Promise.all([loadOLTs(), loadHistory(), loadUnauth(), loadOntCache()]).then(() => {
    server.listen(PORT, () => {
        console.log(`✅ Servidor en http://localhost:${PORT}`);
        console.log(`🔌 WebSocket en ws://localhost:${PORT}/ws`);
        console.log(`📊 OLTs cargadas: ${olts.length}`);
    });
});

module.exports = app;