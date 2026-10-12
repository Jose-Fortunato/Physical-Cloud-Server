const WebSocket = require('ws');
const http = require('http');
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Server Ping OK');
});
const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ server: server });
const clientes = new Map(); 

function gerarCodigoUnico() {
    let codigo;

    do {
        codigo = String(
            Math.floor(Math.random() * 900000) + 100000
        );
    } while (clientes.has(codigo));

    return codigo;
}

wss.on('connection', (ws) => {
  console.log('Client connected');
  
  ws.on('message', (message) => {
   const dados = JSON.parse(message);
    if (dados.type === "tornar-servidor") {
    removerCodigoDoCliente(ws);

    
    const codigo = gerarCodigoUnico();

   
    clientes.set(codigo, ws);
    ws.codigoTornarServidor = codigo;

   
    ws.send(JSON.stringify({
        type: "codigo-gerado",
        codigo: codigo
    }));
}
    else if (dados.type === "conectar") {
      const codigoRecebido = String(dados.codigo ?? "").trim();
const clienteServidor = clientes.get(codigoRecebido);
    
    ws.send(JSON.stringify({ type: "codigo-recebido", codigo: dados.codigo }));
    if (clienteServidor === ws) {
  enviarErro(
    ws,
    'AUTO_CONEXAO',
    'Esse é o código do seu próprio dispositivo servidor.'
  );
  return;
}
      else if (clienteServidor) {
        ws.send(JSON.stringify({ type: "conectado" ,sucesso: true, mensagem: "Conexão estabelecida com sucesso!" }));
         clienteServidor.send(JSON.stringify({ type: "dispositivo-conectou" }));
      }
       else if(clienteServidor === undefined){
          ws.send(JSON.stringify({ type: "erro" , sucesso: false, mensagem: "Código inválido" }));
        }
    }
    console.log(`Received message: ${message}`);
  });
});
console.log('WebSocket server is running on ws://localhost:3000');
server.listen(PORT, () => {
  console.log(`WebSocket Server rodando na porta ${PORT}`);
});