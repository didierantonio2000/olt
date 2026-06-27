const net = require('net');

const OLT_CONFIG = {
  host: '10.251.5.11',
  port: 23,
  username: 'lite',
  password: 'lite',
  enablePass: 'lite'
};

class TelnetClient {
  constructor(config) {
    this.config = config;
    this.socket = null;
    this.buffer = '';
    this.waitingFor = null;
    this.timeout = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.config.port, this.config.host);
      
      this.socket.on('data', (data) => {
        this.buffer += data.toString();
        if (this.waitingFor && this.buffer.includes(this.waitingFor)) {
          clearTimeout(this.timeout);
          this.processWait();
        }
      });

      this.socket.on('error', reject);
      this.socket.on('connect', () => {
        this.waitingFor = 'Username:';
        this.timeout = setTimeout(() => resolve(), 3000);
      });
    });
  }

  processWait() {
    if (this.waitingFor === 'Username:') {
      this.socket.write(this.config.username + '\n');
      this.waitingFor = 'Password:';
      this.buffer = '';
    } else if (this.waitingFor === 'Password:') {
      this.socket.write(this.config.password + '\n');
      this.waitingFor = '>';
      this.buffer = '';
    } else if (this.waitingFor === 'enable') {
      this.socket.write('enable\n');
      this.waitingFor = 'Password:';
      this.buffer = '';
    } else if (this.waitingFor === 'enablePass') {
      this.socket.write(this.config.enablePass + '\n');
      this.waitingFor = '#';
      this.buffer = '';
    }
  }

  send(command) {
    return new Promise((resolve, reject) => {
      this.socket.write(command + '\n');
      this.waitingFor = '#';
      this.buffer = '';
      
      this.timeout = setTimeout(() => {
        resolve(this.buffer);
      }, 2000);

      this.socket.once('data', (data) => {
        clearTimeout(this.timeout);
        this.buffer += data.toString();
        resolve(this.buffer);
      });
    });
  }

  close() {
    if (this.socket) this.socket.end();
  }
}

async function detectUnauthorizedOnts() {
  const client = new TelnetClient(OLT_CONFIG);
  
  try {
    console.log('🔌 Conectando a OLT 10.251.5.11...');
    await client.connect();
    
    console.log('✅ Conectado. Autenticando...');
    await new Promise(r => setTimeout(r, 1000));
    
    console.log('📋 Ejecutando comandos de detección...\n');
    
    // Entrar a modo enable
    let output = await client.send('enable');
    console.log('Enable output:', output.substring(0, 200));
    
    // Desactivar paginación
    output = await client.send('terminal length 0');
    console.log('Terminal config:', output.substring(0, 200));
    
    // Entrar a config
    output = await client.send('configure terminal');
    console.log('Config mode:', output.substring(0, 200));
    
    // Recorrer cada PON y buscar ONTs no autorizadas
    const unauthorizedOnts = [];
    
    for (let pon = 1; pon <= 8; pon++) {
      console.log(`\n🔍 Escaneando PON ${pon}...`);
      
      // Entrar a interfaz PON
      output = await client.send(`interface gpon 0/${pon}`);
      console.log(`Interfaz PON 0/${pon}:`, output.substring(0, 150));
      
      // Ejecutar auto-find para detectar equipos nuevos
      output = await client.send('show onu auto-find');
      console.log(`Auto-find PON ${pon}:`, output);
      
      // Parsear resultados
      const lines = output.split('\n');
      lines.forEach(line => {
        if (line.includes('GPON') || line.includes('Serial') || line.includes('auto-find')) {
          console.log(`  → ${line.trim()}`);
          
          // Intentar extraer serial y ONT ID
          const match = line.match(/GPON0\/\d+:(\d+)\s+(\w+)/);
          if (match) {
            unauthorizedOnts.push({
              pon,
              ont: match[1],
              serial: match[2]
            });
          }
        }
      });
      
      // Salir de interfaz
      await client.send('exit');
    }
    
    console.log('\n\n═══════════════════════════════════════════════════════════');
    console.log('📊 RESULTADOS DE DETECCIÓN');
    console.log('═══════════════════════════════════════════════════════════\n');
    
    if (unauthorizedOnts.length === 0) {
      console.log('✅ No hay ONTs pendientes de autorización');
    } else {
      console.log(`⚠️  Se encontraron ${unauthorizedOnts.length} ONTs por autorizar:\n`);
      unauthorizedOnts.forEach((o, i) => {
        console.log(`${i+1}. PON ${o.pon}:${o.ont} - Serial: ${o.serial}`);
      });
    }
    
    console.log('\n═══════════════════════════════════════════════════════════\n');
    
    client.close();
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    client.close();
  }
}

detectUnauthorizedOnts();
