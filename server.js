// @ts-check

// ═══════════════════════════════════════════════════════════════════════════
// Physical Cloud - servidor de sinalização
// Este servidor NÃO guarda nem toca em arquivos. Ele só:
//   1) cria códigos para dispositivos servidores,
//   2) controla quem pode conectar em quem (com aprovação),
//   3) repassa as mensagens de ligação (WebRTC) entre os dois aparelhos.
// Depois da ligação, os arquivos viajam direto de um aparelho para o outro.
// ═══════════════════════════════════════════════════════════════════════════

const WebSocket = require('ws');
const http = require('http');
const { randomInt, randomUUID } = require('crypto');

// Troque este texto a cada commit para saber qual versão está no ar
const VERSAO = "2026-10-19-v4-sessoes";

const COMMIT = process.env.RENDER_GIT_COMMIT
  ? process.env.RENDER_GIT_COMMIT.slice(0, 7)
  : "local";
const BRANCH = process.env.RENDER_GIT_BRANCH || "local";
const INICIO = new Date().toISOString();
const PORT = process.env.PORT || 3000;

// ─── Configurações ───────────────────────────────────────────────────────────
const NA_RENDER = Boolean(process.env.RENDER || process.env.RENDER_GIT_COMMIT);

// Sites autorizados a usar este servidor. Para adicionar outros sem mexer no código,
// use a variável de ambiente ORIGENS_EXTRAS (separadas por vírgula).
const ORIGENS_PERMITIDAS = new Set([
  "https://josefortunato-coder.github.io",
  ...(process.env.ORIGENS_EXTRAS || "").split(",").map((s) => s.trim()).filter(Boolean),
]);

const TTL_CODIGO_MS = 5 * 60 * 1000;      // código vale 5 minutos
const TTL_PEDIDO_MS = 60 * 1000;          // o servidor tem 60s para aprovar um pedido
const MAX_CLIENTES_POR_SERVIDOR = 3;      // quantos aparelhos podem usar o mesmo servidor
const MAX_SERVIDORES_POR_CLIENTE = 5;     // a quantos servidores um aparelho pode se ligar
const MAX_FALHAS_POR_CONEXAO = 10;        // códigos errados por conexão
const MAX_FALHAS_POR_IP = 20;             // códigos errados por IP (a cada 10 min)
const JANELA_FALHAS_IP_MS = 10 * 60 * 1000;
const MAX_CONEXOES_POR_IP = 10;           // conexões abertas ao mesmo tempo por IP
const MAX_MENSAGENS_POR_JANELA = 200;     // mensagens a cada 10s por conexão
const JANELA_MENSAGENS_MS = 10 * 1000;
const MAX_PAYLOAD_BYTES = 64 * 1024;      // mensagens são só texto pequeno

// ─── Estado (só em memória: se o servidor reiniciar, tudo recomeça) ──────────
/** código → { ws, expiraEm } */
const codigos = new Map();
/** sessaoId → sessão entre um servidor e um cliente */
const sessoes = new Map();
/** ip → quantas conexões abertas */
const conexoesPorIp = new Map();
/** ip → { n, inicio } falhas de código */
const falhasPorIp = new Map();

// ─── Servidor HTTP e WebSocket ───────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Server Ping OK\nversao: ${VERSAO}\ncommit: ${COMMIT}\nbranch: ${BRANCH}\niniciado em: ${INICIO}`);
});

const wss = new WebSocket.Server({ server: server, maxPayload: MAX_PAYLOAD_BYTES });

// Se algo escapar dos try/catch, loga em vez de derrubar o processo
process.on('uncaughtException', (err) => console.error('uncaughtException:', err));
process.on('unhandledRejection', (err) => console.error('unhandledRejection:', err));

// ─── Funções auxiliares ──────────────────────────────────────────────────────

/**
 * Envia JSON só se o socket estiver aberto
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
  // Em testes no seu computador (fora do Render), aceita localhost
  if (!NA_RENDER && /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origem)) return true;
  return false;
}

/**
 * IP do visitante. Atrás do proxy do Render, vem no cabeçalho x-forwarded-for
 * @param {http.IncomingMessage} req
 */
function obterIp(req) {
  const xff = req.headers['x-forwarded-for'];
  const primeiro = typeof xff === 'string' ? xff.split(',')[0].trim() : '';
  return primeiro || (req.socket && req.socket.remoteAddress) || 'desconhecido';
}

// Validações dos campos recebidos
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
function validarSessaoId(v) { return typeof v === 'string' && /^[0-9a-f-]{36}$/.test(v) ? v : null; }
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
 * Remove o código de um dispositivo servidor (se ele tiver um)
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
 * Conta sessões de um aparelho como "servidor" ou como "cliente"
 * @param {WebSocket} ws
 * @param {string} papel
 */
function contarSessoes(ws, papel) {
  const sock = /** @type {any} */ (ws);
  let n = 0;
  for (const id of sock.sessoes) {
    const s = sessoes.get(id);
    if (s && s[papel] === ws) n++;
  }
  return n;
}

/**
 * Encerra uma sessão e avisa o outro lado (quem encerrou não é avisado)
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

// ─── Rotina a cada 30s: heartbeat, códigos expirados e limpeza ───────────────
const rotina = setInterval(() => {
  // Heartbeat: derruba conexões que não respondem (o navegador responde ping sozinho)
  wss.clients.forEach((ws) => {
    const sock = /** @type {any} */ (ws);
    if (sock.isAlive === false) { ws.terminate(); return; }
    sock.isAlive = false;
    ws.ping();
  });

  // Códigos vencidos
  const agora = Date.now();
  for (const [codigo, entrada] of codigos) {
    if (entrada.expiraEm <= agora) {
      codigos.delete(codigo);
      /** @type {any} */ (entrada.ws).codigo = undefined;
      enviar(entrada.ws, { type: "codigo-expirado" });
    }
  }

  // Limpa contadores antigos de falhas por IP
  for (const [ip, f] of falhasPorIp) {
    if (agora - f.inicio > JANELA_FALHAS_IP_MS) falhasPorIp.delete(ip);
  }
}, 30000);
wss.on('close', () => clearInterval(rotina));

// ─── Conexões ────────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const sock = /** @type {any} */ (ws);

  // O handler de erro vem primeiro: sem ele, um erro de socket pode derrubar o processo
  ws.on('error', (err) => console.error('WS error:', err.message));

  // 1) Só aceita conexões vindas do seu site
  if (!origemPermitida(req.headers.origin)) {
    console.warn('Origem bloqueada:', req.headers.origin);
    ws.close(1008, 'Origem não permitida');
    return;
  }

  // 2) Limite de conexões simultâneas por IP
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
    if (isBinary) return; // só aceitamos texto

    // Limite de mensagens por conexão (evita flood)
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

      // ── Este aparelho quer ser servidor: gera um código ───────────────────
      case "tornar-servidor": {
        const deviceId = validarDeviceId(dados.deviceId);
        if (!deviceId) return erro(ws, "Identificador do dispositivo inválido");
        sock.deviceId = deviceId;

        removerCodigo(ws); // troca o código antigo por um novo

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

      // ── Este aparelho quer conectar num servidor usando um código ─────────
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

        // Procura o código (e descarta se já venceu)
        let entrada = codigos.get(codigo);
        if (entrada && entrada.expiraEm <= Date.now()) {
          codigos.delete(codigo);
          /** @type {any} */ (entrada.ws).codigo = undefined;
          entrada = undefined;
        }

        if (!entrada || entrada.ws.readyState !== WebSocket.OPEN) {
          sock.falhas++;
          registrarFalhaIp(sock.ip);
          return erro(ws, "Código inválido ou expirado");
        }

        const servidorWs = entrada.ws;
        const servidorSock = /** @type {any} */ (servidorWs);

        // Não pode conectar no próprio aparelho (mesma conexão ou mesmo deviceId)
        if (servidorWs === ws || servidorSock.deviceId === deviceId) {
          return erro(ws, "Você não pode conectar ao próprio dispositivo servidor.");
        }

        // Já existe pedido ou conexão com esse servidor?
        for (const id of sock.sessoes) {
          const s = sessoes.get(id);
          if (s && s.servidor === servidorWs && s.cliente === ws) {
            return erro(ws, "Você já está conectado (ou aguardando aprovação) neste servidor.");
          }
        }

        // Limites de quantidade
        if (contarSessoes(servidorWs, 'servidor') >= MAX_CLIENTES_POR_SERVIDOR) {
          return erro(ws, "Este servidor já atingiu o limite de dispositivos conectados.");
        }
        if (contarSessoes(ws, 'cliente') >= MAX_SERVIDORES_POR_CLIENTE) {
          return erro(ws, "Você já atingiu o limite de servidores conectados.");
        }

        // Cria o pedido. A conexão só vale depois que o servidor aprovar.
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

      // ── O servidor aprova ou recusa um pedido ─────────────────────────────
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

        enviar(s.cliente, {
          type: "conectado", sucesso: true, sessaoId, permissao: s.permissao,
          mensagem: "Conexão estabelecida com sucesso!",
        });
        enviar(ws, { type: "dispositivo-conectou", sessaoId, nome: s.nome, permissao: s.permissao });
        return;
      }

      // ── Repasse das mensagens de ligação (WebRTC) entre os dois aparelhos ─
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

      // ── Qualquer um dos dois lados encerra a sessão ───────────────────────
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

  // Quando o aparelho desconecta: apaga o código dele e avisa quem estava ligado a ele
  ws.on('close', () => {
    removerCodigo(ws);
    for (const id of [...sock.sessoes]) encerrarSessao(id, 'dispositivo-desconectou', ws);

    const n = (conexoesPorIp.get(sock.ip) || 1) - 1;
    if (n <= 0) conexoesPorIp.delete(sock.ip);
    else conexoesPorIp.set(sock.ip, n);
  });
});

server.listen(PORT, () => {
  console.log(`=== SERVIDOR INICIADO | versao ${VERSAO} | commit ${COMMIT} | branch ${BRANCH} | porta ${PORT} ===`);
});