const net = require('net');
const fs = require('fs');

const OLT_CONFIG = {
  host: '10.251.5.11',
  port: 23,
  username: 'lite',
  password: 'lite',
  enablePass: 'lite'
};

let logOutput = '';

function log(msg) {
  console.log(msg);
  logOutput += msg + '\n';
}

class TelnetClient {
  constructor(config) {
    this.config = config;
    this.socket = null;
    this.buffer = '';
    this.responsePromise = null;
    this.responseResolve = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.config.port, this.config.host);
      
      this.socket.on('data', (data) => {
        this.buffer += data.toString();
        if (this.responseResolve && (this.buffer.includes('>') || this.buffer.includes('#'))) {
          const resolve = this.responseResolve;
          this.responseResolve = null;
          resolve(this.buffer);
        }
      });

      this.socket.on('error', reject);
      this.socket.on('connect', () => {
        setTimeout(() => resolve(), 1000);
      });
    });
  }

  send(command) {
    return new Promise((resolve) => {
      this.buffer = '';
      this.responseResolve = resolve;
      this.socket.write(command + '\n');
      
      setTimeout(() => {
        if (this.responseResolve) {
          const resolve = this.responseResolve;
          this.responseResolve = null;
          resolve(this.buffer);
        }
      }, 3000);
    });
  }

  close() {
    if (this.socket) this.socket.end();
  }
}

async function detectUnauthorizedOnts() {
  const client = new TelnetClient(OLT_CONFIG);
  
  try {
    log('🔌 Conectando a OLT 10.251.5.11...');
    await client.connect();
    
    log('✅ Conectado. Enviando credenciales...');
    
    // Login
    let output = await client.send('lite');
    log('Usuario enviado');
    
    output = await client.send('lite');
    log('Contraseña enviada');
    
    // Enable
    output = await client.send('enable');
    log('Enable enviado');
    
    output = await client.send('lite');
    log('Enable password enviado');
    
    // Terminal config
    output = await client.send('terminal length 0');
    log('Terminal configurado');
    
    // Config mode
    output = await client.send('configure terminal');
    log('Modo configuración activado');
    
    const unauthorizedOnts = [];
    
    for (let pon = 1; pon <= 8; pon++) {
      log(`\n🔍 Escaneando PON ${pon}...`);
      
      output = await client.send(`interface gpon 0/${pon}`);
      
      // Buscar ONTs no autorizadas
      output = await client.send('show onu auto-find');
      log(`Resultado PON ${pon}:\n${output}\n`);
      
      // Parsear líneas
      const lines = output.split('\n');
      lines.forEach(line => {
        if (line.includes('GPON') && line.includes(':')) {
          const parts = line.trim().split(/\s+/);
          if (parts.length >= 2) {
            unauthorizedOnts.push({
              pon,
              line: line.trim()
            });
          }
        }
      });
      
      output = await client.send('exit');
    }
    
    log('\n═══════════════════════════════════════════════════════════');
    log('📊 RESULTADOS FINALES');
    log('═══════════════════════════════════════════════════════════\n');
    
    if (unauthorizedOnts.length === 0) {
      log('✅ No hay ONTs pendientes de autorización');
    } else {
      log(`⚠️  Se encontraron ${unauthorizedOnts.length} ONTs por autorizar:\n`);
      unauthorizedOnts.forEach((o, i) => {
        log(`${i+1}. ${o.line}`);
      });
    }
    
    log('\n═══════════════════════════════════════════════════════════\n');
    
    client.close();
    
    // Guardar log
    fs.writeFileSync('C:\\Users\\wason\\Desktop\\olt\\unauthorized-onts.txt', logOutput);
    log('📁 Resultado guardado en: unauthorized-onts.txt');
    
  } catch (error) {
    log('❌ Error: ' + error.message);
    client.close();
    fs.writeFileSync('C:\\Users\\wason\\Desktop\\olt\\unauthorized-onts.txt', logOutput);
  }
}

detectUnauthorizedOnts();
