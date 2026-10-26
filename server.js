// @ts-check

const WebSocket = require('ws');
const http = require('http');
const { randomInt, randomUUID } = require('crypto');

const VERSAO = "2026-10-26-v11-fixes";

const COMMIT = process.env.RENDER_GIT_COMMIT
  ? process.env.RENDER_GIT_COMMIT.slice(0, 7)
  : "local";
const BRANCH = process.env.RENDER_GIT_BRANCH || "local";
const INICIO = new Date().toISOString();
const PORT = process.env.PORT || 3000;

const NA_RENDER = Boolean(process.env.RENDER || process.env.RENDER_GIT_COMMIT);

const ORIGENS_PERMITIDAS = new Set([
  "https://josefortunato-coder.github.io",
  ...(process.env.ORIGENS_EXTRAS || "").split(",").map((s) => s.trim()).filter(Boolean),
]);

const TTL_CODIGO_MS = 5 * 60 * 1000;
const TTL_PEDIDO_MS = 60 * 1000;
const MAX_CLIENTES_POR_SERVIDOR = 3;
const MAX_SERVIDORES_POR_CLIENTE = 5;
const MAX_FALHAS_POR_CONEXAO = 10;
const MAX_FALHAS_POR_IP = 20;
const JANELA_FALHAS_IP_MS = 10 * 60 * 1000;
const MAX_CONEXOES_POR_IP = 10;
const MAX_MENSAGENS_POR_JANELA = 200;
const JANELA_MENSAGENS_MS = 10 * 1000;
const MAX_PAYLOAD_BYTES = 64 * 1024;

/** código → { ws, expiraEm } */
const codigos = new Map();
/** sessaoId → sessão entre um servidor e um cliente */
const sessoes = new Map();
/** ip → quantas conexões abertas */
const conexoesPorIp = new Map();
/** ip → { n, inicio } falhas de código */
const falhasPorIp = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`OK`);
});

const wss = new WebSocket.Server({ server: server, maxPayload: MAX_PAYLOAD_BYTES });

process.on('uncaughtException', (err) => console.error('uncaughtException:', err));
process.on('unhandledRejection', (err) => console.error('unhandledRejection:', err));

/**
 * @param {WebSocket} ws
 * @param {{ type: string; sucesso?: boolean; mensagem?: any; sessaoId?: any; motivo?: any; codigo?: string; expiraEm?: number; validadeSegundos?: number; nome?: any; permissao?: any; dados?: any; }} obj
 */
function enviar(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

/**
 * @param {WebSocket} ws
 * @param {string} mensagem
 */
function erro(ws, mensagem) {
  enviar(ws, { type: "erro", sucesso: false, mensagem });
}

/**
 * @param {string | undefined} origem
 */
function origemPermitida(origem) {
  if (!origem) return false;
  if (ORIGENS_PERMITIDAS.has(origem)) return true;
  if (!NA_RENDER && /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origem)) return true;
  return false;
}

/**
 * @param {http.IncomingMessage} req
 */
function obterIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (typeof xff === 'string') {
    const primeiro = xff.split(',')[0].trim();
    if (primeiro) return primeiro;
  }
  return (req.socket && req.socket.remoteAddress) || 'desconhecido';
}

/**
 * @param {string} v
 */
function validarCodigo(v) { return typeof v === 'string' && /^\d{6}$/.test(v.trim()) ? v.trim() : null; }
/**
 * @param {string} v
 */
function validarDeviceId(v) { return typeof v === 'string' && /^[A-Za-z0-9_-]{8,64}$/.test(v) ? v : null; }
/**
 * @param {string} v
 */
function validarSessaoId(v) { return typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v) ? v : null; }
/**
 * @param {string} v
 */
function limparNome(v) {
  if (typeof v !== 'string') return "Dispositivo";
  const limpo = v.replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 40);
  return limpo || "Dispositivo";
}

/**
 * @param {any} ip
 */
function ipBloqueado(ip) {
  const f = falhasPorIp.get(ip);
  if (!f) return false;
  if (Date.now() - f.inicio > JANELA_FALHAS_IP_MS) { falhasPorIp.delete(ip); return false; }
  return f.n >= MAX_FALHAS_POR_IP;
}

/**
 * @param {any} ip
 */
function registrarFalhaIp(ip) {
  const f = falhasPorIp.get(ip);
  if (!f || Date.now() - f.inicio > JANELA_FALHAS_IP_MS) {
    falhasPorIp.set(ip, { n: 1, inicio: Date.now() });
  } else {
    f.n++;
  }
}

/**
 * @param {WebSocket} ws
 */
function removerCodigo(ws) {
  const sock = /** @type {any} */ (ws);
  if (sock.codigo !== undefined) {
    const entrada = codigos.get(sock.codigo);
    if (entrada && entrada.ws === ws) codigos.delete(sock.codigo);
    sock.codigo = undefined;
  }
}

/**
 * @param {WebSocket} ws
 * @param {string} papel
 */
function contarSessoes(ws, papel) {
  const sock = /** @type {any} */ (ws);
  let n = 0;
  for (const id of sock.sessoes) {
    const s = sessoes.get(id);
    if (s && s[papel] === ws && s.estado === 'ativa') n++;
  }
  return n;
}

/**
 * @param {string} sessaoId
 * @param {string} motivo
 * @param {WebSocket | undefined} [quemEncerrou]
 */
function encerrarSessao(sessaoId, motivo, quemEncerrou) {
  const s = sessoes.get(sessaoId);
  if (!s) return;
  if (s.timer) clearTimeout(s.timer);
  sessoes.delete(sessaoId);
  /** @type {any} */ (s.servidor).sessoes.delete(sessaoId);
  /** @type {any} */ (s.cliente).sessoes.delete(sessaoId);
  for (const alvo of [s.servidor, s.cliente]) {
    if (alvo !== quemEncerrou) enviar(alvo, { type: "sessao-encerrada", sessaoId, motivo });
  }
}

const rotina = setInterval(() => {
  wss.clients.forEach((ws) => {
    const sock = /** @type {any} */ (ws);
    if (sock.isAlive === false) { ws.terminate(); return; }
    sock.isAlive = false;
    ws.ping();
  });

  const agora = Date.now();
  for (const [codigo, entrada] of codigos) {
    if (entrada.expiraEm <= agora) {
      codigos.delete(codigo);
      /** @type {any} */ (entrada.ws).codigo = undefined;
      enviar(entrada.ws, { type: "codigo-expirado" });
    }
  }

  for (const [ip, f] of falhasPorIp) {
    if (agora - f.inicio > JANELA_FALHAS_IP_MS) falhasPorIp.delete(ip);
  }
}, 30000);
wss.on('close', () => clearInterval(rotina));

wss.on('connection', (ws, req) => {
  const sock = /** @type {any} */ (ws);

  ws.on('error', (err) => console.error('WS error:', err.message));

  if (!origemPermitida(req.headers.origin)) {
    console.warn('Origem bloqueada:', req.headers.origin);
    ws.close(1008, 'Origem não permitida');
    return;
  }

  const ip = obterIp(req);
  const abertas = conexoesPorIp.get(ip) || 0;
  if (abertas >= MAX_CONEXOES_POR_IP) {
    ws.close(1008, 'Conexões demais');
    return;
  }
  conexoesPorIp.set(ip, abertas + 1);

  sock.ip = ip;
  sock.isAlive = true;
  sock.falhas = 0;
  sock.sessoes = new Set();
  sock.deviceId = undefined;
  sock.codigo = undefined;
  sock.janelaInicio = Date.now();
  sock.msgs = 0;

  ws.on('pong', () => { sock.isAlive = true; });

  ws.on('message', (message, isBinary) => {
    sock.isAlive = true;
    if (isBinary) return;

    const agora = Date.now();
    if (agora - sock.janelaInicio > JANELA_MENSAGENS_MS) {
      sock.janelaInicio = agora;
      sock.msgs = 0;
    }
    if (++sock.msgs > MAX_MENSAGENS_POR_JANELA) {
      ws.close(1008, 'Mensagens demais');
      return;
    }

    let dados;
    try {
      dados = JSON.parse(message.toString('utf8'));
    } catch {
      erro(ws, "Mensagem inválida");
      return;
    }
    if (!dados || typeof dados !== "object" || Array.isArray(dados)) return;

    switch (dados.type) {

      case "ping":
        return;

      case "tornar-servidor": {
        const deviceId = validarDeviceId(dados.deviceId);
        if (!deviceId) return erro(ws, "Identificador do dispositivo inválido");
        sock.deviceId = deviceId;

        removerCodigo(ws);

        let codigo;
        do {
          codigo = String(randomInt(100000, 1000000));
        } while (codigos.has(codigo));

        const expiraEm = Date.now() + TTL_CODIGO_MS;
        codigos.set(codigo, { ws, expiraEm });
        sock.codigo = codigo;

        enviar(ws, { type: "codigo-gerado", codigo, expiraEm, validadeSegundos: TTL_CODIGO_MS / 1000 });
        return;
      }

      // [Fix.1] Nova mensagem: cliente solicita parar de ser servidor.
      // Remove o código do Map global e confirma. Encerra também sessões pendentes
      // (ainda no estado 'pendente') associadas a este servidor.
      case "parar-servidor": {
        removerCodigo(ws);

        // Encerra sessões pendentes (pedidos não respondidos) deste servidor.
        // Sessões 'ativa' já são encerradas pelo cliente via "encerrar" antes
        // de enviar "parar-servidor", mas por segurança cobrimos as pendentes.
        for (const id of [...sock.sessoes]) {
          const s = sessoes.get(id);
          if (s && s.servidor === ws && s.estado === 'pendente') {
            encerrarSessao(id, 'pedido-expirou', ws);
          }
        }

        enviar(ws, { type: "servidor-parado" });
        return;
      }

      case "conectar": {
        const codigo = validarCodigo(dados.codigo);
        const deviceId = validarDeviceId(dados.deviceId);
        if (!codigo) return erro(ws, "O código deve ter 6 números");
        if (!deviceId) return erro(ws, "Identificador do dispositivo inválido");
        sock.deviceId = deviceId;
        const nome = limparNome(dados.nome);

        if (sock.falhas >= MAX_FALHAS_POR_CONEXAO || ipBloqueado(sock.ip)) {
          return erro(ws, "Muitas tentativas. Aguarde alguns minutos.");
        }

        // [Fix.4/Item6] Distinguir código inexistente (conta falha) de expirado
        // ou de servidor desconectado (não conta falha).
        let entrada = codigos.get(codigo);
        /** @type {"nunca-existiu"|"expirado"|"desconectado"|null} */
        let razaoInvalido = entrada ? null : "nunca-existiu";

        if (entrada && entrada.expiraEm <= Date.now()) {
          codigos.delete(codigo);
          /** @type {any} */ (entrada.ws).codigo = undefined;
          entrada = undefined;
          razaoInvalido = "expirado";
        } else if (entrada && entrada.ws.readyState !== WebSocket.OPEN) {
          codigos.delete(codigo);
          entrada = undefined;
          razaoInvalido = "desconectado";
        }

        if (!entrada) {
          // Só conta como falha se o código nunca existiu (tentativa de adivinhação).
          // Código expirado ou servidor que caiu não penalizam o cliente legítimo.
          if (razaoInvalido === "nunca-existiu") {
            sock.falhas++;
            registrarFalhaIp(sock.ip);
          }
          return erro(ws, "Código inválido ou expirado");
        }

        const servidorWs = entrada.ws;
        const servidorSock = /** @type {any} */ (servidorWs);

        if (servidorWs === ws || servidorSock.deviceId === deviceId) {
          return erro(ws, "Você não pode conectar ao próprio dispositivo servidor.");
        }

        for (const id of sock.sessoes) {
          const s = sessoes.get(id);
          if (s && s.servidor === servidorWs && s.cliente === ws) {
            return erro(ws, "Você já está conectado (ou aguardando aprovação) neste servidor.");
          }
        }

        if (contarSessoes(servidorWs, 'servidor') >= MAX_CLIENTES_POR_SERVIDOR) {
          return erro(ws, "Este servidor já atingiu o limite de dispositivos conectados.");
        }
        if (contarSessoes(ws, 'cliente') >= MAX_SERVIDORES_POR_CLIENTE) {
          return erro(ws, "Você já atingiu o limite de servidores conectados.");
        }

        sock.falhas = 0;
        const sessaoId = randomUUID();
        const sessao = {
          id: sessaoId,
          servidor: servidorWs,
          cliente: ws,
          estado: 'pendente',
          permissao: 'leitura',
          nome,
          timer: /** @type {any} */ (null),
        };
        sessao.timer = setTimeout(() => {
          if (sessao.estado === 'pendente') encerrarSessao(sessaoId, 'pedido-expirou');
        }, TTL_PEDIDO_MS);

        sessoes.set(sessaoId, sessao);
        servidorSock.sessoes.add(sessaoId);
        sock.sessoes.add(sessaoId);

        enviar(ws, { type: "aguardando-aprovacao", sessaoId });
        enviar(servidorWs, { type: "pedido-conexao", sessaoId, nome });
        return;
      }

      case "responder-pedido": {
        const sessaoId = validarSessaoId(dados.sessaoId);
        const s = sessaoId ? sessoes.get(sessaoId) : undefined;
        if (!sessaoId || !s || s.servidor !== ws || s.estado !== 'pendente') {
          return erro(ws, "Pedido não encontrado");
        }

        if (dados.aceitar !== true) {
          encerrarSessao(sessaoId, 'recusado', ws);
          return;
        }

        clearTimeout(s.timer);
        s.estado = 'ativa';
        s.permissao = dados.permissao === 'leitura-escrita' ? 'leitura-escrita' : 'leitura';

        removerCodigo(ws);

        enviar(s.cliente, {
          type: "conectado", sucesso: true, sessaoId, permissao: s.permissao,
          mensagem: "Conexão estabelecida com sucesso!",
        });
        enviar(ws, { type: "dispositivo-conectou", sessaoId, nome: s.nome, permissao: s.permissao });
        return;
      }

      case "sinal": {
        const sessaoId = validarSessaoId(dados.sessaoId);
        const s = sessaoId ? sessoes.get(sessaoId) : undefined;
        if (!sessaoId || !s || s.estado !== 'ativa') return erro(ws, "Sessão não encontrada");
        if (s.servidor !== ws && s.cliente !== ws) return erro(ws, "Sessão não encontrada");
        if (!dados.dados || typeof dados.dados !== "object" || Array.isArray(dados.dados)) return;

        const destino = s.servidor === ws ? s.cliente : s.servidor;
        enviar(destino, { type: "sinal", sessaoId, dados: dados.dados });
        return;
      }

      case "encerrar": {
        const sessaoId = validarSessaoId(dados.sessaoId);
        const s = sessaoId ? sessoes.get(sessaoId) : undefined;
        if (!sessaoId || !s || (s.servidor !== ws && s.cliente !== ws)) return;
        encerrarSessao(sessaoId, 'encerrado-pelo-dispositivo', ws);
        return;
      }

      default:
        return erro(ws, "Tipo de mensagem desconhecido");
    }
  });

  ws.on('close', () => {
    removerCodigo(ws);
    for (const id of [...sock.sessoes]) encerrarSessao(id, 'dispositivo-desconectou', ws);

    const n = (conexoesPorIp.get(sock.ip) || 1) - 1;
    if (n <= 0) conexoesPorIp.delete(sock.ip);
    else conexoesPorIp.set(sock.ip, n);
  });
});

server.listen(PORT, () => {
  const inicioLegivel = INICIO.slice(0, 19).replace("T", " ");
  console.log(`=== SERVIDOR INICIADO | versao ${VERSAO} | commit ${COMMIT} | branch ${BRANCH} | porta ${PORT} | inicio ${inicioLegivel} ===`);
});