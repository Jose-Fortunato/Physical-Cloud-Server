
const WebSocket = require('ws');
const http = require('http');
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Server Ping OK');
});
const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ server: server });
const clientes = new Map();


wss.on('connection', (ws) => {
  console.log('Client connected');

  ws.on('message', (message) => {
    const dados = JSON.parse(message);
    if (dados.type === "tornar-servidor") {
      if (ws.codigoTornarServidor !== undefined &&
          clientes.get(ws.codigoTornarServidor) === ws) {
        clientes.delete(ws.codigoTornarServidor);
      }

      let de = 100000;
      let ate = 999999;
      let codigo;
      do {
        codigo = Math.floor(Math.random() * (ate - de + 1)) + de;
      } while (clientes.has(codigo));
      clientes.set(codigo, ws);
      ws.codigoTornarServidor = codigo;
      ws.send(JSON.stringify({type: "codigo-gerado", codigo: codigo }));

    } else if (dados.type === "conectar") {
      if ( ws.codigoTornarServidor !== undefined &&
        Number(dados.codigo) === ws.codigoTornarServidor
      ) {
        ws.send(JSON.stringify({type: "erro", sucesso: false, mensagem: "Você não pode conectar ao próprio dispositivo servidor." }));
        return;
      }

      const clienteServidor = clientes.get(Number(dados.codigo));

      ws.send(JSON.stringify({type: "codigo-recebido", codigo: dados.codigo}));

      if (clienteServidor && clienteServidor.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({type: "conectado", sucesso: true, mensagem: "Conexão estabelecida com sucesso!" }));
        clienteServidor.send(JSON.stringify({ type: "dispositivo-conectou"  }));

      } else {
        ws.send(JSON.stringify({type: "erro", sucesso: false, mensagem: "Código inválido"}));
      }
    }

    console.log(`Received message: ${message}`);
  });

 
  ws.on('close', () => {
    if (ws.codigoTornarServidor !== undefined &&
      clientes.get(ws.codigoTornarServidor) === ws
    ) {
      clientes.delete(ws.codigoTornarServidor);
    }
  });
});

console.log('WebSocket server is running on ws://localhost:3000');

server.listen(PORT, () => {
  console.log(`WebSocket Server rodando na porta ${PORT}`);
});
