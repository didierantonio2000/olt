const net = require("net");

/**
 * Clase para manejar conexiones Telnet de forma robusta
 * Compatible con Windows, Linux y macOS
 */
class TelnetConnection {
    constructor(host, port = 23, timeout = 30000) {
        this.host = host;
        this.port = parseInt(port);
        this.timeout = parseInt(timeout);
        this.socket = null;
        this.buffer = "";
        this.isConnected = false;
    }

    /**
     * Conectar a la OLT
     */
    connect() {
        return new Promise((resolve, reject) => {
            try {
                this.socket = net.createConnection({
                    host: this.host,
                    port: this.port,
                    timeout: this.timeout
                });

                const connectionTimeout = setTimeout(() => {
                    this.socket.destroy();
                    reject(new Error(`Timeout conectando a ${this.host}:${this.port}`));
                }, this.timeout);

                this.socket.on("connect", () => {
                    clearTimeout(connectionTimeout);
                    this.isConnected = true;
                    console.log(`Conectado a ${this.host}:${this.port}`);
                    resolve();
                });

                this.socket.on("data", (data) => {
                    this.buffer += data.toString();
                    // Manejar paginación automáticamente
                    if (this.buffer.includes("-- Enter Key To Continue --") || 
                        this.buffer.includes("--More--") ||
                        this.buffer.includes("[K")) {
                        this.socket.write("\r\n");
                    }
                });

                this.socket.on("error", (error) => {
                    clearTimeout(connectionTimeout);
                    reject(error);
                });

                this.socket.on("close", () => {
                    this.isConnected = false;
                    console.log(`Desconectado de ${this.host}:${this.port}`);
                });
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Enviar comando y esperar respuesta
     */
    sendCommand(command, waitFor = /#/, timeout = 10000) {
        return new Promise((resolve, reject) => {
            if (!this.isConnected) {
                reject(new Error("No conectado a Telnet"));
                return;
            }

            const startTime = Date.now();
            const commandTimeout = setTimeout(() => {
                reject(new Error(`Timeout esperando respuesta para comando: ${command}`));
            }, timeout);

            const checkResponse = () => {
                if (waitFor.test(this.buffer)) {
                    clearTimeout(commandTimeout);
                    const response = this.buffer;
                    this.buffer = "";
                    resolve(response);
                } else if (Date.now() - startTime > timeout) {
                    clearTimeout(commandTimeout);
                    reject(new Error(`Timeout esperando por: ${waitFor}`));
                } else {
                    setTimeout(checkResponse, 100);
                }
            };

            // Enviar comando
            this.socket.write(`${command}\r\n`);
            
            // Esperar respuesta
            checkResponse();
        });
    }

    /**
     * Autenticarse en la OLT
     */
    async authenticate(username, password) {
        try {
            // Esperar prompt de usuario
            await this.waitForPattern(/Username:/i, 5000);
            this.socket.write(`${username}\r\n`);

            // Esperar prompt de contraseña
            await this.waitForPattern(/Password:/i, 5000);
            this.socket.write(`${password}\r\n`);

            // Esperar prompt de login exitoso
            const loginResponse = await this.waitForPattern(/[>#]/, 5000);
            
            // Si es prompt de usuario (>), enviar enable
            if (loginResponse.includes(">")) {
                this.socket.write("enable\r\n");
                await this.waitForPattern(/#/, 5000);
            }

            console.log("Autenticación exitosa");
            return true;
        } catch (error) {
            throw new Error(`Error en autenticación: ${error.message}`);
        }
    }

    /**
     * Esperar por un patrón en el buffer
     */
    waitForPattern(pattern, timeout = 10000) {
        return new Promise((resolve, reject) => {
            const startTime = Date.now();
            const check = () => {
                if (pattern.test(this.buffer)) {
                    const response = this.buffer;
                    this.buffer = "";
                    resolve(response);
                } else if (Date.now() - startTime > timeout) {
                    reject(new Error(`Timeout esperando patrón: ${pattern}`));
                } else {
                    setTimeout(check, 100);
                }
            };
            check();
        });
    }

    /**
     * Desconectar
     */
    disconnect() {
        return new Promise((resolve) => {
            if (this.socket) {
                this.socket.end();
                this.socket.on("close", () => {
                    this.isConnected = false;
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }

    /**
     * Verificar si está conectado
     */
    connected() {
        return this.isConnected && this.socket && !this.socket.destroyed;
    }
}

module.exports = TelnetConnection;
