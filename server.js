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
 if (ws.codigoTornarServidor) {
        clientes.delete(ws.codigoTornarServidor);
    }
   let de = 100000;
    let ate = 999999;
      let codigo = Math.floor(Math.random() * (ate - de + 1)) + de;
       clientes.set(codigo, ws);
       ws.codigoTornarServidor = codigo;
      ws.send(JSON.stringify({ type: "codigo-gerado", codigo: codigo }));   
      
    }else if (dados.type === "conectar") {
    const clienteServidor = clientes.get(dados.codigo);
    ws.send(JSON.stringify({ type: "codigo-recebido", codigo: dados.codigo }));
    if (clienteServidor === ws.codigoTornarServidor) {
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
        if(clienteServidor === undefined){
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