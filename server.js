// @ts-check

// Importa a biblioteca de WebSocket para comunicação em tempo real
const WebSocket = require('ws');
// Importa o módulo HTTP nativo do Node.js para criar o servidor base
const http = require('http');

// Cria um servidor HTTP simples (usado como base para o WebSocket)
// Responde com "Server Ping OK" em qualquer requisição HTTP normal
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Server Ping OK');
});

// Porta onde o servidor vai rodar (usa variável de ambiente ou 3000 como padrão)
const PORT = process.env.PORT || 3000;

// Cria o servidor WebSocket em cima do servidor HTTP
const wss = new WebSocket.Server({ server: server });

// Mapa que associa código numérico → conexão WebSocket do dispositivo servidor
// Ex: { 123456: ws_do_dispositivo_A }
const clientes = new Map();


// Evento disparado toda vez que um novo cliente se conecta via WebSocket
wss.on('connection', (ws) => {
  console.log('Client connected');

  // Evento disparado quando uma mensagem chega desse cliente
  ws.on('message', (message) => {

    // Converte a mensagem recebida para string UTF-8.
    // A mensagem pode chegar como string, Buffer, ArrayBuffer ou array de Buffers
    // dependendo da versão do ws e do cliente, por isso os três casos.
    const texto = typeof message === 'string'
      ? message
      : Array.isArray(message)
        ? Buffer.concat(message).toString('utf8')
        : Buffer.from(message instanceof ArrayBuffer ? new Uint8Array(message) : message).toString('utf8');

    // Converte o texto JSON para objeto JavaScript
    const dados = JSON.parse(texto);

    // ─── Tipo: "tornar-servidor" ───────────────────────────────────────────────
    // O cliente quer se registrar como dispositivo servidor (quem vai receber conexões)
    if (dados.type === "tornar-servidor") {

      // Se este cliente já tinha um código anterior, remove do mapa antes de gerar um novo
      if (ws.codigoTornarServidor !== undefined &&
          clientes.get(ws.codigoTornarServidor) === ws) {
        clientes.delete(ws.codigoTornarServidor);
      }

      // Gera um código aleatório de 6 dígitos que ainda não esteja em uso
      let de = 100000;
      let ate = 999999;
      let codigo;
      do {
        codigo = Math.floor(Math.random() * (ate - de + 1)) + de;
       
      } while (clientes.has(codigo));
 String(codigo);
      // Registra o cliente no mapa com o código gerado
    clientes.set(String(codigo), ws);


      // Guarda o código do botão Se tornar dispositivo servidor que é uma string na própria conexão WebSocket para referência futura
      // (útil para remover do mapa quando o cliente desconectar ou trocar de código)
      ws.codigoTornarServidor = String(codigo);

      // Envia o código gerado em forma de String de volta para o cliente
      ws.send(JSON.stringify({ type: "codigo-gerado", codigo: String(codigo) }));

    // ─── Tipo: "conectar" ─────────────────────────────────────────────────────
    // O cliente quer se conectar a um dispositivo servidor usando um código
    } else if (dados.type === "conectar") {

      // Impede que o cliente se conecte ao próprio código
      if (ws.codigoTornarServidor !== undefined &&
        String(dados.codigo) === ws.codigoTornarServidor
      ) {
        ws.send(JSON.stringify({ type: "erro", sucesso: false, mensagem: "Você não pode conectar ao próprio dispositivo servidor." }));
        return;
      }

      // Busca no mapa a conexão WebSocket do dispositivo servidor com aquele código
      const clienteServidor = clientes.get(String(dados.codigo));

      // Confirma para o cliente que o código foi recebido (independente de ser válido ou não)
      ws.send(JSON.stringify({ type: "codigo-recebido", codigo: String(dados.codigo) }));

      // Verifica se o dispositivo servidor existe e está com a conexão aberta
      if (clienteServidor && clienteServidor.readyState === WebSocket.OPEN) {
        // Avisa o cliente conectante que a conexão foi bem-sucedida
        ws.send(JSON.stringify({ type: "conectado", sucesso: true, mensagem: "Conexão estabelecida com sucesso!" }));
        // Avisa o dispositivo servidor que alguém acabou de se conectar a ele
        clienteServidor.send(JSON.stringify({ type: "dispositivo-conectou" }));
      } else {
        // Código não encontrado ou dispositivo servidor offline
        ws.send(JSON.stringify({ type: "erro", sucesso: false, mensagem: "Código inválido" }));
      }
    }

    console.log(`Received message: ${message}`);
  });

  // Evento disparado quando o cliente se desconecta
  ws.on('close', () => {
    // Se esse cliente era um dispositivo servidor, remove seu código do mapa
    // para que o código não fique "ocupado" indefinidamente
    if (ws.codigoTornarServidor !== undefined &&
      clientes.get(ws.codigoTornarServidor) === ws
    ) {
      clientes.delete(ws.codigoTornarServidor);
    }
  });
});

console.log('WebSocket server is running on ws://localhost:3000');

// Inicia o servidor HTTP (e junto com ele o WebSocket) na porta configurada
server.listen(PORT, () => {
  console.log(`WebSocket Server rodando na porta ${PORT}`);
});