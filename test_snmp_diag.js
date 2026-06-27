const snmp = require('net-snmp');
const fs = require('fs');

console.log("Iniciando prueba de diagnóstico SNMP...");
console.log("Target: 10.251.5.11 (Buena Vista)");

const session = snmp.createSession("10.251.5.11", "public", {
    version: snmp.Version2c,
    timeout: 5000,
    retries: 2
});

// OID común para system.sysDescr (para verificar conectividad)
const oids = ["1.3.6.1.2.1.1.1.0"];

session.get(oids, function (error, varbinds) {
    if (error) {
        console.error("Error de conectividad SNMP:", error.toString());
        console.log("Asegúrese de que el servicio SNMP esté habilitado en la OLT y la comunidad sea 'public'.");
        session.close();
        return;
    }

    for (let i = 0; i < varbinds.length; i++) {
        if (snmp.isVarbindError(varbinds[i])) {
            console.error(snmp.varbindError(varbinds[i]));
        } else {
            console.log("Conectividad SNMP Exitosa!");
            console.log(varbinds[i].oid + " = " + varbinds[i].value.toString());
            
            // Si hay conexión, intentar un snmpwalk en el árbol enterprise de VSOL/EPON
            console.log("\nIniciando exploración de OIDs (Walk)...");
            // 1.3.6.1.4.1.37950 es el enterprise OID típico para VSOL
            const baseOid = "1.3.6.1.4.1.37950"; 
            
            session.walk(baseOid, 10, function (err, vbs) {
                if (err) {
                    console.error("Error en el walk:", err.toString());
                } else {
                    console.log(`Se encontraron ${vbs.length} valores en el árbol ${baseOid}`);
                    // Guardar los primeros resultados en un archivo para análisis
                    const results = vbs.slice(0, 20).map(vb => `${vb.oid} = ${vb.value}`).join('\n');
                    fs.writeFileSync('snmp_diag_results.txt', results);
                    console.log("Resultados preliminares guardados en snmp_diag_results.txt");
                }
                session.close();
            });
            return; // Evitar cerrar la sesión antes del walk
        }
    }
    session.close();
});
