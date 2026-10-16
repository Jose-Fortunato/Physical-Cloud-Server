// @ts-check

// Importa a biblioteca de WebSocket para comunicação em tempo real
const WebSocket = require('ws');
// Importa o módulo HTTP nativo do Node.js para criar o servidor base
const http = require('http');
// Gerador de números aleatórios criptograficamente seguro
const { randomInt } = require('crypto');

// Servidor HTTP simples (base do WebSocket). Responde "Server Ping OK" em requisições normais
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Server Ping OK');
});

const PORT = process.env.PORT || 3000;

// Cria o servidor WebSocket em cima do servidor HTTP
const wss = new WebSocket.Server({ server: server });

// Mapa: código (string de 6 dígitos) → conexão WebSocket do dispositivo servidor
const clientes = new Map();

// Máximo de tentativas de código inválido por conexão (proteção contra força bruta)
const MAX_TENTATIVAS = 10;

// ─── Segurança contra quedas do processo ─────────────────────────────────────
// Se algo escapar dos try/catch, loga o erro em vez de derrubar o servidor
// (derrubar = perder o mapa "clientes" = todos os códigos viram inválidos)
process.on('uncaughtException', (err) => console.error('uncaughtException:', err));
process.on('unhandledRejection', (err) => console.error('unhandledRejection:', err));

// ─── Heartbeat: detecta e remove conexões mortas ─────────────────────────────
// O navegador responde ping automaticamente com pong
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    const sock = /** @type {any} */ (ws);
    if (sock.isAlive === false) {
      ws.terminate(); // dispara o evento 'close' e limpa o código do mapa
      return;
    }
    sock.isAlive = false;
    ws.ping();
  });
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

// Envia JSON com segurança (só se o socket estiver aberto)
/** @param {WebSocket} ws @param {object} obj */
function enviar(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

// Evento disparado toda vez que um novo cliente se conecta via WebSocket
wss.on('connection', (ws) => {
  // Cast para permitir propriedades próprias (evita erro do @ts-check)
  const sock = /** @type {any} */ (ws);
  sock.isAlive = true;
  sock.tentativas = 0;
  console.log('Client connected');

  ws.on('pong', () => { sock.isAlive = true; });
  ws.on('error', (err) => console.error('WS error:', err));

  ws.on('message', (message) => {
    sock.isAlive = true; // qualquer mensagem prova que o cliente está vivo

    // No ws v8+ a mensagem chega como Buffer; toString() converte para UTF-8
    const texto = message.toString('utf8');

    // JSON.parse protegido: mensagem inválida não derruba mais o servidor
    let dados;
    try {
      dados = JSON.parse(texto);
    } catch {
      enviar(ws, { type: "erro", sucesso: false, mensagem: "Mensagem inválida" });
      return;
    }
    if (!dados || typeof dados !== "object") return;
    if (dados.type === "ping") return; // ignora o ping criado no front

    // ─── Tipo: "tornar-servidor" ─────────────────────────────────────────────
    if (dados.type === "tornar-servidor") {

      // Remove o código anterior deste cliente, se existir
      if (sock.codigoTornarServidor !== undefined &&
          clientes.get(sock.codigoTornarServidor) === ws) {
        clientes.delete(sock.codigoTornarServidor);
      }

      // Gera um código de 6 dígitos que ainda não esteja em uso
      let codigo;
      do {
        codigo = String(randomInt(100000, 1000000)); // 100000 a 999999
      } while (clientes.has(codigo));

      clientes.set(codigo, ws);
      sock.codigoTornarServidor = codigo;

      console.log(`Código ${codigo} criado. Códigos ativos:`, [...clientes.keys()]);
      enviar(ws, { type: "codigo-gerado", codigo: codigo });

    // ─── Tipo: "conectar" ────────────────────────────────────────────────────
    } else if (dados.type === "conectar") {
      const codigoInserido = String(dados.codigo).trim();

      // Limite de tentativas inválidas por conexão
      if (sock.tentativas >= MAX_TENTATIVAS) {
        enviar(ws, { type: "erro", sucesso: false, mensagem: "Muitas tentativas. Recarregue a página." });
        return;
      }

      // Impede conectar ao próprio código
      if (sock.codigoTornarServidor !== undefined && codigoInserido === sock.codigoTornarServidor) {
        enviar(ws, { type: "erro", sucesso: false, mensagem: "Você não pode conectar ao próprio dispositivo servidor." });
        return;
      }

      const clienteServidor = clientes.get(codigoInserido);

      // Log de diagnóstico: mostra o que foi procurado e o que existe no mapa
      console.log(`Procurando: ${codigoInserido} | Códigos ativos:`, [...clientes.keys()]);

      enviar(ws, { type: "codigo-recebido", codigo: dados.codigo });

      if (clienteServidor && clienteServidor.readyState === WebSocket.OPEN) {
        sock.tentativas = 0;
        enviar(ws, { type: "conectado", sucesso: true, mensagem: "Conexão estabelecida com sucesso!" });
        enviar(clienteServidor, { type: "dispositivo-conectou" });
      } else {
        sock.tentativas++;
        // Mensagem diferenciada para facilitar o diagnóstico
        const mensagem = clienteServidor
          ? "Dispositivo servidor está offline"
          : "Código inválido ou expirado";
        enviar(ws, { type: "erro", sucesso: false, mensagem });
      }
    }
  });

  // Quando o cliente se desconecta, remove o código dele do mapa
  ws.on('close', () => {
    if (sock.codigoTornarServidor !== undefined && clientes.get(sock.codigoTornarServidor) === ws) {
      clientes.delete(sock.codigoTornarServidor);
      console.log(`Código servidor ${sock.codigoTornarServidor} limpo do mapa.`);
    }
  });
});

server.listen(PORT, () => {
  console.log(`WebSocket Server rodando na porta ${PORT}`);
});