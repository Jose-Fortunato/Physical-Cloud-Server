// server.js — Physical Cloud Signaling Server
// Requer Node.js 18+ (Render: runtime Node 18 ou superior).
// @ts-check
"use strict";

const { WebSocketServer, WebSocket } = require("ws");
const { createClient } = require("@supabase/supabase-js");
const { createHash, createHmac, randomBytes, timingSafeEqual } = require("node:crypto");
const { isIP } = require("node:net");

const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios.");
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// @ts-ignore
function inteiroConfig(nome, padrao, minimo, maximo) {
    const valor = Number.parseInt(process.env[nome] || String(padrao), 10);
    return Number.isSafeInteger(valor) ? Math.min(maximo, Math.max(minimo, valor)) : padrao;
}

const MAX_PAYLOAD_BYTES = inteiroConfig("PC_WS_MAX_PAYLOAD_BYTES", 1024 * 1024, 64 * 1024, 4 * 1024 * 1024);
const MAX_CONEXOES_GLOBAIS = inteiroConfig("PC_MAX_CONEXOES_GLOBAIS", 1500, 50, 10000);
const MAX_CONEXOES_POR_IP = inteiroConfig("PC_MAX_CONEXOES_POR_IP", 400, 5, 2000);
const MAX_NAO_AUTENTICADAS = inteiroConfig("PC_MAX_NAO_AUTENTICADAS", 400, 10, 5000);
const MAX_TENTATIVAS_AUTH_IP_MIN = inteiroConfig("PC_MAX_TENTATIVAS_AUTH_IP_MIN", 450, 3, 3000);
const MAX_TENTATIVAS_AUTH_GLOBAL_MIN = inteiroConfig("PC_MAX_TENTATIVAS_AUTH_GLOBAL_MIN", 3000, 100, 30000);
const MAX_CADASTROS_IP_HORA = inteiroConfig("PC_MAX_CADASTROS_IP_HORA", 400, 1, 1000);
const MAX_CADASTROS_GLOBAIS_HORA = inteiroConfig("PC_MAX_CADASTROS_GLOBAIS_HORA", 2000, 10, 20000);
const TIMEOUT_IDENTIFICACAO_MS = inteiroConfig("PC_TIMEOUT_IDENTIFICACAO_MS", 15000, 3000, 120000);
const MAX_MENSAGENS_SOCKET_MIN = inteiroConfig("PC_MAX_MENSAGENS_SOCKET_MIN", 600, 60, 6000);
const TEMPO_PEDIDO_MS = inteiroConfig("PC_TEMPO_PEDIDO_MS", 60000, 10000, 180000);
const MAX_PEDIDOS_POR_DISPOSITIVO_MIN = inteiroConfig("PC_MAX_PEDIDOS_POR_DISPOSITIVO_MIN", 12, 2, 120);
const MAX_PEDIDOS_PENDENTES_ORIGEM = inteiroConfig("PC_MAX_PEDIDOS_PENDENTES_ORIGEM", 5, 1, 30);
const MAX_PEDIDOS_PENDENTES_ALVO = inteiroConfig("PC_MAX_PEDIDOS_PENDENTES_ALVO", 20, 1, 100);
const MAX_PEDIDOS_RECEBIDOS_ALVO_MIN = inteiroConfig("PC_MAX_PEDIDOS_RECEBIDOS_ALVO_MIN", 60, 5, 300);
const MAX_CONSULTAS_AUTO_GLOBAL_MIN = inteiroConfig("PC_MAX_CONSULTAS_AUTO_GLOBAL_MIN", 1200, 60, 12000);
const MAX_OPERACOES_AUTO_DISPOSITIVO_MIN = inteiroConfig("PC_MAX_OPERACOES_AUTO_DISPOSITIVO_MIN", 10, 2, 60);
const MAX_OPERACOES_AUTO_GLOBAL_MIN = inteiroConfig("PC_MAX_OPERACOES_AUTO_GLOBAL_MIN", 1200, 60, 12000);
// Render encaminha o tráfego pelo Cloudflare. Para rate limiting, prefira
// CF-Connecting-IP: Cloudflare o define na borda como o IP que se conectou.
// X-Forwarded-For pode conter valores fornecidos pelo próprio cliente à esquerda;
// portanto, nunca o habilitamos automaticamente no Render.
const NO_RENDER = process.env.RENDER === "true";
const CONFIAR_X_FORWARDED_FOR = process.env.PC_TRUST_X_FORWARDED_FOR === "true" && !NO_RENDER;
const TURN_URLS = String(process.env.PC_TURN_URLS || "").split(/[\s,]+/).map(v => v.trim())
    .filter(v => /^turns?:[^\s]+$/i.test(v)).slice(0, 8);
const TURN_SHARED_SECRET = process.env.PC_TURN_SHARED_SECRET || "";
const TURN_CREDENTIAL_TTL_SECONDS = inteiroConfig("PC_TURN_CREDENTIAL_TTL_SECONDS", 3600, 300, 86400);

// Mensagens de sinalização são pequenas. Não permitir o limite padrão alto do ws.
const wss = new WebSocketServer({
    // @ts-ignore
    port: PORT,
    maxPayload: MAX_PAYLOAD_BYTES,
    perMessageDeflate: false,
    clientTracking: true,
});
console.log(`Servidor WebSocket rodando na porta ${PORT} (maxPayload=${MAX_PAYLOAD_BYTES} bytes)`);

// ─── Estado em memória ───────────────────────────────────────────────────────

/** @type {Map<string, { ws: WebSocket, nome: string }>} */
const dispositivos = new Map(); // deviceId → { ws, nome }

/** @type {Map<string, string>} */
const wsPorDeviceId = new Map(); // ws._id → deviceId

/** @type {Map<string, string>} */
const codigos = new Map(); // codigo6digitos → deviceId (servidor)

/** @type {Map<string, ReturnType<typeof setTimeout>>} */
const timersCodigo = new Map(); // deviceId → timer de expiração

/** @type {Map<string, { requerenteId: string, alvoId: string, via: string }>} */
const sessoesPendentes = new Map(); // sessaoId → pedido aguardando resposta

/** @type {Map<string, { id1: string, id2: string }>} */
const sessoesAtivas = new Map(); // sessaoId → { id1, id2 } (deviceIds dos dois lados)

/** @type {Map<string, string>} */
const autoHashParaDeviceId = new Map(); // idHash → deviceId (modo automático em memória)
const filasOperacoesAutomaticas = new Map(); // serializa registrar/desregistrar por deviceId

// @ts-ignore
function enfileirarOperacaoAutomatica(deviceId, operacao) {
    const anterior = filasOperacoesAutomaticas.get(deviceId) || Promise.resolve();
    const atual = anterior.catch(() => {}).then(operacao);
    filasOperacoesAutomaticas.set(deviceId, atual);
    atual.then(
        () => { if (filasOperacoesAutomaticas.get(deviceId) === atual) filasOperacoesAutomaticas.delete(deviceId); },
        () => { if (filasOperacoesAutomaticas.get(deviceId) === atual) filasOperacoesAutomaticas.delete(deviceId); }
    );
    return atual;
}

// Limites em memória. Em escala horizontal, migrar estes contadores para Redis.

const limitesPorAcao = new Map();
const consultasAutoGlobais = new Map();
const operacoesAutoPorDispositivo = new Map();

// @ts-ignore
function permitirAcaoDispositivo(deviceId, acao, limite = MAX_PEDIDOS_POR_DISPOSITIVO_MIN) {
    return consumirJanela(limitesPorAcao, `${deviceId}:${acao}`, limite, 60 * 1000);
}

// @ts-ignore
function permitirPedidoRecebido(alvoId) {
    return permitirAcaoDispositivo(alvoId, "pedidos-recebidos", MAX_PEDIDOS_RECEBIDOS_ALVO_MIN);
}

// @ts-ignore
function permitirOperacaoAutomatica(deviceId, acao) {
    return consumirJanela(
        operacoesAutoPorDispositivo,
        `${deviceId}:${acao}`,
        MAX_OPERACOES_AUTO_DISPOSITIVO_MIN,
        60 * 1000
    ) && consumirJanela(
        consultasAutoGlobais,
        "operacoes-global",
        MAX_OPERACOES_AUTO_GLOBAL_MIN,
        60 * 1000
    );
}

// @ts-ignore
function contarPedidosPendentes(campo, valor) {
    let total = 0;
    // @ts-ignore
    for (const pedido of sessoesPendentes.values()) if (pedido[campo] === valor) total++;
    return total;
}

// @ts-ignore
function removerPedidoPendente(sessaoId) {
    const pedido = sessoesPendentes.get(sessaoId);
    if (!pedido) return null;
    // @ts-ignore
    if (pedido.timer) clearTimeout(pedido.timer);
    sessoesPendentes.delete(sessaoId);
    return pedido;
}

// @ts-ignore
function encerrarPedidoPendente(sessaoId, motivo = "pedido-expirou") {
    const pedido = removerPedidoPendente(sessaoId);
    if (!pedido) return false;
    const payload = { type: "sessao-encerrada", sessaoId, motivo };
    const wsRequerente = wsDeDevice(pedido.requerenteId);
    const wsAlvo = wsDeDevice(pedido.alvoId);
    if (wsRequerente) enviar(wsRequerente, payload);
    if (wsAlvo && wsAlvo !== wsRequerente) enviar(wsAlvo, payload);
    return true;
}

// @ts-ignore
function criarPedidoPendente(sessaoId, requerenteId, alvoId, via) {
    if (contarPedidosPendentes("requerenteId", requerenteId) >= MAX_PEDIDOS_PENDENTES_ORIGEM ||
        contarPedidosPendentes("alvoId", alvoId) >= MAX_PEDIDOS_PENDENTES_ALVO) return null;
    const expiraEm = Date.now() + TEMPO_PEDIDO_MS;
    const pedido = { requerenteId, alvoId, via, criadoEm: Date.now(), expiraEm, timer: null };
    // @ts-ignore
    pedido.timer = setTimeout(() => {
        if (sessoesPendentes.get(sessaoId) === pedido) encerrarPedidoPendente(sessaoId, "pedido-expirou");
    }, TEMPO_PEDIDO_MS);
    // @ts-ignore
    pedido.timer.unref?.();
    sessoesPendentes.set(sessaoId, pedido);
    return pedido;
}

// @ts-ignore
function gerarCredenciaisTurn(deviceId) {
    if (!TURN_URLS.length || !TURN_SHARED_SECRET) return [];
    const expiracao = Math.floor(Date.now() / 1000) + TURN_CREDENTIAL_TTL_SECONDS;
    const username = `${expiracao}:${deviceId}`;
    const credential = createHmac("sha1", TURN_SHARED_SECRET).update(username).digest("base64");
    return [{ urls: TURN_URLS, username, credential }];
}

if (TURN_URLS.length && !TURN_SHARED_SECRET) {
    console.error("[TURN] PC_TURN_URLS foi configurado, mas PC_TURN_SHARED_SECRET está ausente. TURN ficará desabilitado.");
} else if (!TURN_URLS.length) {
    console.warn("[TURN] Nenhum servidor TURN configurado; conexões em NAT/firewalls restritivos podem falhar.");
}

let conexoesAbertas = 0;
let conexoesNaoAutenticadas = 0;
const conexoesPorIp = new Map();
const tentativasAuthPorIp = new Map();
let tentativasAuthGlobais = { inicio: Date.now(), quantidade: 0 };
const cadastrosPorIp = new Map();
let cadastrosGlobaisJanela = { inicio: Date.now(), quantidade: 0 };

// @ts-ignore
function normalizarIp(candidato) {
    if (typeof candidato !== "string") return null;
    const valor = candidato.trim().replace(/^\[|\]$/g, "");
    if (!valor || valor.length > 64 || !isIP(valor)) return null;
    return valor.toLowerCase().startsWith("::ffff:") ? valor.slice(7) : valor;
}

// @ts-ignore
function ipDaRequisicao(req) {
    if (NO_RENDER) {
        // Cloudflare é a borda pública do Render; CF-Connecting-IP é o header
        // específico de origem, mais seguro que escolher o primeiro XFF.
        const cfIp = normalizarIp(req?.headers?.["cf-connecting-ip"]);
        if (cfIp) return cfIp;
    } else if (CONFIAR_X_FORWARDED_FOR) {
        // Somente para proxy próprio explicitamente confiável fora do Render.
        const encaminhado = req?.headers?.["x-forwarded-for"];
        if (typeof encaminhado === "string") {
            const candidato = normalizarIp(encaminhado.split(",", 1)[0]);
            if (candidato) return candidato;
        }
    }
    // Fallback fail-closed: atrás do proxy, pode agrupar usuários, mas evita
    // rate-limit bypass por XFF forjado quando o header confiável não existe.
    return normalizarIp(req?.socket?.remoteAddress) || "ip-desconhecido";
}

// @ts-ignore
function consumirJanela(map, chave, limite, duracaoMs, agora = Date.now()) {
    let item = map.get(chave);
    if (!item || agora - item.inicio >= duracaoMs || agora < item.inicio) {
        item = { inicio: agora, quantidade: 0 };
        map.set(chave, item);
    }
    if (item.quantidade >= limite) return false;
    item.quantidade++;
    return true;
}

function consumirTentativaAuthGlobal(agora = Date.now()) {
    if (agora - tentativasAuthGlobais.inicio >= 60 * 1000 || agora < tentativasAuthGlobais.inicio) {
        tentativasAuthGlobais = { inicio: agora, quantidade: 0 };
    }
    if (tentativasAuthGlobais.quantidade >= MAX_TENTATIVAS_AUTH_GLOBAL_MIN) return false;
    tentativasAuthGlobais.quantidade++;
    return true;
}

// @ts-ignore
function permitirNovoCadastro(ip) {
    const agora = Date.now();
    if (agora - cadastrosGlobaisJanela.inicio >= 60 * 60 * 1000 || agora < cadastrosGlobaisJanela.inicio) {
        cadastrosGlobaisJanela = { inicio: agora, quantidade: 0 };
    }
    let porIp = cadastrosPorIp.get(ip);
    if (!porIp || agora - porIp.inicio >= 60 * 60 * 1000 || agora < porIp.inicio) {
        porIp = { inicio: agora, quantidade: 0 };
        cadastrosPorIp.set(ip, porIp);
    }
    if (cadastrosGlobaisJanela.quantidade >= MAX_CADASTROS_GLOBAIS_HORA || porIp.quantidade >= MAX_CADASTROS_IP_HORA) return false;
    cadastrosGlobaisJanela.quantidade++;
    porIp.quantidade++;
    return true;
}

const limpezaRateLimit = setInterval(() => {
    const agora = Date.now();
    for (const [ip, item] of tentativasAuthPorIp) if (agora - item.inicio > 2 * 60 * 1000) tentativasAuthPorIp.delete(ip);
    for (const [ip, item] of cadastrosPorIp) if (agora - item.inicio > 2 * 60 * 60 * 1000) cadastrosPorIp.delete(ip);
    for (const [chave, item] of limitesPorAcao) if (agora - item.inicio > 2 * 60 * 1000) limitesPorAcao.delete(chave);
    for (const [chave, item] of consultasAutoGlobais) if (agora - item.inicio > 2 * 60 * 1000) consultasAutoGlobais.delete(chave);
    for (const [chave, item] of operacoesAutoPorDispositivo) if (agora - item.inicio > 2 * 60 * 1000) operacoesAutoPorDispositivo.delete(chave);
}, 60 * 1000);
limpezaRateLimit.unref?.();

// Autenticação por dispositivo: deviceId é um identificador local, não a credencial.
// A posse é provada pelo token aleatório; o ID não deve ser tratado como senha.
// O token em claro nunca é persistido no Supabase, somente seu SHA-256.
const TABELA_AUTENTICACAO = "dispositivos_autenticacao";
const TOKEN_AUTENTICACAO_RE = /^[A-Za-z0-9_-]{43}$/;

// @ts-ignore
function hashTokenAutenticacao(token) {
    return createHash("sha256").update(token, "utf8").digest("hex");
}

// @ts-ignore
function hashesIguais(a, b) {
    if (typeof a !== "string" || typeof b !== "string" || !/^[0-9a-f]{64}$/i.test(a) || !/^[0-9a-f]{64}$/i.test(b)) return false;
    const ba = Buffer.from(a, "hex");
    const bb = Buffer.from(b, "hex");
    return ba.length === bb.length && timingSafeEqual(ba, bb);
}

/**
 * Valida a credencial existente ou registra uma credencial na primeira conexão.
 * O INSERT com chave única fecha a janela de corrida entre dois primeiros logins.
 * @param {string} deviceId
 * @param {unknown} tokenRecebido
 * @returns {Promise<{ok: boolean, novoToken?: string}>}
 */
// @ts-ignore
async function autenticarOuRegistrarDispositivo(deviceId, tokenRecebido, ipOrigem) {
    const tokenValido = typeof tokenRecebido === "string" && TOKEN_AUTENTICACAO_RE.test(tokenRecebido)
        ? tokenRecebido : null;
    if (tokenRecebido != null && !tokenValido) return { ok: false };

    const { data: registro, error: erroBusca } = await supabase
        .from(TABELA_AUTENTICACAO)
        .select("device_id, token_hash")
        .eq("device_id", deviceId)
        .maybeSingle();
    if (erroBusca) throw erroBusca;

    if (registro) {
        if (!tokenValido || !hashesIguais(hashTokenAutenticacao(tokenValido), registro.token_hash)) {
            return { ok: false };
        }
        // A falha apenas de telemetria não deve derrubar uma credencial válida.
        supabase.from(TABELA_AUTENTICACAO)
            .update({ ultima_autenticacao: new Date().toISOString() })
            .eq("device_id", deviceId)
            .then(({ error }) => { if (error) console.error("[Auth] Falha ao atualizar atividade:", error.message); })
            // @ts-ignore
            .catch((error) => console.error("[Auth] Falha ao atualizar atividade:", error));
        return { ok: true };
    }

    // Primeiro cadastro ou recuperação após remoção explícita da credencial.
    // deviceId é um UUID local e aleatório; o token bearer prova posse do registro.
    // Limites por origem e globais contêm automação de cadastros em massa.
    // @ts-ignore
    if (!permitirNovoCadastro(ipOrigem)) return { ok: false, limite: true };
    const tokenNovo = tokenValido || randomBytes(32).toString("base64url");
    const agora = new Date().toISOString();
    const { error: erroInsert } = await supabase.from(TABELA_AUTENTICACAO).insert({
        device_id: deviceId,
        token_hash: hashTokenAutenticacao(tokenNovo),
        criado_em: agora,
        ultima_autenticacao: agora,
    });

    if (!erroInsert) return { ok: true, ...(tokenValido ? {} : { novoToken: tokenNovo }) };

    // Outra conexão pode ter cadastrado o mesmo ID primeiro. Só aceita se o
    // solicitante já possuía exatamente o token que venceu a corrida.
    const { data: concorrente, error: erroReleitura } = await supabase
        .from(TABELA_AUTENTICACAO)
        .select("device_id, token_hash")
        .eq("device_id", deviceId)
        .maybeSingle();
    if (erroReleitura) throw erroReleitura;
    if (concorrente && tokenValido && hashesIguais(hashTokenAutenticacao(tokenValido), concorrente.token_hash)) {
        return { ok: true };
    }
    if (concorrente) return { ok: false };
    throw erroInsert;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

let _wsCounter = 0;
/**
 * @param {import("ws")} ws
 */
function wsId(ws) {
    // @ts-ignore
    if (!ws._pcId) ws._pcId = String(++_wsCounter);
    // @ts-ignore
    return ws._pcId;
}

function gerarCodigo() {
    return String(Math.floor(100000 + Math.random() * 900000));
}

function gerarSessaoId() {
    if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

/** @param {WebSocket} ws @param {object} obj */
function enviar(ws, obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(JSON.stringify(obj)); } catch (e) { console.error("Erro ao enviar:", e); }
    }
}

/** @param {string} deviceId */
function wsDeDevice(deviceId) {
    return dispositivos.get(deviceId)?.ws || null;
}

/** Confirma que este WebSocket ainda é o proprietário do deviceId vinculado. */
// @ts-ignore
function socketAtual(ws, deviceId) {
    return typeof deviceId === "string" && !!deviceId && dispositivos.get(deviceId)?.ws === ws;
}

/** Atualiza a última atividade dos registros automáticos sem bloquear o WebSocket. */
// @ts-ignore
function atualizarAtividadeAuto(deviceId, idHash = null, nomeAtual = null) {
    if (typeof deviceId !== "string" || !deviceId) return;
    const atualizacao = { ultima_atividade: new Date().toISOString() };
    // @ts-ignore
    if (typeof nomeAtual === "string") atualizacao.nome_atual = nomeAtual.slice(0, 60);
    let query = supabase
        .from("dispositivos_automaticos")
        .update(atualizacao)
        .eq("device_id", deviceId);
    if (idHash) query = query.eq("id_hash", idHash);
    query.then(({ error }) => {
        if (error) console.error("[AutoMode] Falha ao atualizar última atividade:", error.message);
    // @ts-ignore
    }).catch((error) => {
        console.error("[AutoMode] Falha ao atualizar última atividade:", error);
    });
}

/** @param {string} deviceId */
function nomeDeDevice(deviceId) {
    return dispositivos.get(deviceId)?.nome || "Dispositivo";
}

/** @param {string} deviceId encerra todas as sessões e remove o device */
function desconectarDevice(deviceId) {
    // Encerra sessões pendentes onde este device é parte
    for (const [sessaoId, p] of sessoesPendentes) {
        if (p.requerenteId === deviceId || p.alvoId === deviceId) {
            const outroParte = p.requerenteId === deviceId ? p.alvoId : p.requerenteId;
            const wsOutro = wsDeDevice(outroParte);
            if (wsOutro) enviar(wsOutro, { type: "sessao-encerrada", sessaoId, motivo: "dispositivo-desconectou" });
            removerPedidoPendente(sessaoId);
        }
    }
    // Encerra sessões ativas onde este device é parte
    for (const [sessaoId, s] of sessoesAtivas) {
        if (s.id1 === deviceId || s.id2 === deviceId) {
            const outroId = s.id1 === deviceId ? s.id2 : s.id1;
            const wsOutro = wsDeDevice(outroId);
            if (wsOutro) enviar(wsOutro, { type: "sessao-encerrada", sessaoId, motivo: "dispositivo-desconectou" });
            sessoesAtivas.delete(sessaoId);
        }
    }
    // Remove código se for servidor
    for (const [codigo, devId] of codigos) {
        if (devId === deviceId) {
            codigos.delete(codigo);
            const timer = timersCodigo.get(deviceId);
            if (timer) clearTimeout(timer);
            timersCodigo.delete(deviceId);
        }
    }
    // Remove os hashes automáticos da memória e registra a última atividade.
    // O registro permanece ativo no banco: ativo representa a preferência do
    // usuário, enquanto a presença em memória indica que está online agora.
    let tinhaRegistroAutomatico = false;
    for (const [hash, devId] of autoHashParaDeviceId) {
        if (devId === deviceId) {
            autoHashParaDeviceId.delete(hash);
            tinhaRegistroAutomatico = true;
        }
    }
    if (tinhaRegistroAutomatico) atualizarAtividadeAuto(deviceId);
    // Remove do mapa de dispositivos
    dispositivos.delete(deviceId);
}

// ─── Lógica de código de 6 dígitos ──────────────────────────────────────────

const VALIDADE_CODIGO_MS = 5 * 60 * 1000; // 5 minutos

/** @param {string} deviceId @param {string} nome */
// @ts-ignore
// @ts-ignore
function criarCodigo(deviceId, nome) {
    // Remove código anterior
    for (const [c, dId] of codigos) {
        if (dId === deviceId) codigos.delete(c);
    }
    const timer = timersCodigo.get(deviceId);
    if (timer) clearTimeout(timer);

    let codigo;
    do { codigo = gerarCodigo(); } while (codigos.has(codigo));
    codigos.set(codigo, deviceId);

    const expirar = setTimeout(() => {
        if (codigos.get(codigo) === deviceId) {
            codigos.delete(codigo);
            timersCodigo.delete(deviceId);
            const ws = wsDeDevice(deviceId);
            if (ws) enviar(ws, { type: "codigo-expirado" });
        }
    }, VALIDADE_CODIGO_MS);
    timersCodigo.set(deviceId, expirar);

    const ws = wsDeDevice(deviceId);
    if (ws) enviar(ws, { type: "codigo-gerado", codigo, validadeSegundos: VALIDADE_CODIGO_MS / 1000 });
}

// ─── Conexão WebSocket ───────────────────────────────────────────────────────

wss.on("connection", (ws, req) => {
    wsId(ws); // garante _pcId
    const ipOrigem = ipDaRequisicao(req);
    const conexoesDesteIp = conexoesPorIp.get(ipOrigem) || 0;
    if (conexoesAbertas >= MAX_CONEXOES_GLOBAIS || conexoesDesteIp >= MAX_CONEXOES_POR_IP || conexoesNaoAutenticadas >= MAX_NAO_AUTENTICADAS) {
        ws.close(1013, "Servidor temporariamente ocupado. Tente novamente.");
        return;
    }
    conexoesAbertas++;
    conexoesNaoAutenticadas++;
    conexoesPorIp.set(ipOrigem, conexoesDesteIp + 1);

    /** @type {string | null} */
    let deviceId = null;
    let autenticando = false;
    let contabilizadaComoAutenticada = false;
    let mensagensNaJanela = 0;
    let inicioJanelaMensagens = Date.now();
    const timeoutIdentificacao = setTimeout(() => {
        if (!deviceId && ws.readyState === WebSocket.OPEN) {
            enviar(ws, { type: "identidade-indisponivel", mensagem: "Tempo esgotado para identificar o dispositivo. Tente novamente." });
            ws.close(4008, "Tempo de identificação esgotado");
        }
    }, TIMEOUT_IDENTIFICACAO_MS);
    timeoutIdentificacao.unref?.();

    ws.on("message", async (raw) => {
        const agoraMensagem = Date.now();
        if (agoraMensagem - inicioJanelaMensagens >= 60 * 1000 || agoraMensagem < inicioJanelaMensagens) {
            inicioJanelaMensagens = agoraMensagem;
            mensagensNaJanela = 0;
        }
        mensagensNaJanela++;
        if (mensagensNaJanela > MAX_MENSAGENS_SOCKET_MIN) {
            ws.close(1008, "Limite de mensagens excedido");
            return;
        }
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }
        if (!msg || typeof msg.type !== "string") return;
        if (msg.type !== "identificar" && !socketAtual(ws, deviceId)) return;

        switch (msg.type) {

            // ── Identificação ───────────────────────────────────────────────
            case "identificar": {
                if (!consumirTentativaAuthGlobal() || !consumirJanela(tentativasAuthPorIp, ipOrigem, MAX_TENTATIVAS_AUTH_IP_MIN, 60 * 1000)) {
                    enviar(ws, { type: "identidade-indisponivel", mensagem: "Muitas tentativas de identificação. Aguarde um minuto e tente novamente." });
                    ws.close(1013, "Limite de tentativas de identificação");
                    break;
                }
                const id = String(msg.deviceId || "");
                const nome = String(msg.nome || "Dispositivo").slice(0, 60);
                if (!/^[A-Za-z0-9_-]{8,128}$/.test(id)) {
                    enviar(ws, { type: "identidade-recusada", mensagem: "Identificador de dispositivo inválido." });
                    ws.close(4003, "Identificador inválido");
                    break;
                }
                if (msg.authVersion !== 1) {
                    const mensagem = "Atualize o Physical Cloud para a versão com autenticação de dispositivo antes de conectar.";
                    enviar(ws, { type: "erro", mensagem }); // Compatível com clientes antigos: exibe o aviso de atualização.
                    enviar(ws, { type: "identidade-recusada", mensagem });
                    ws.close(4003, "Cliente desatualizado");
                    break;
                }
                if (deviceId) {
                    enviar(ws, { type: "identidade-recusada", mensagem: "A identidade desta conexão já foi definida." });
                    ws.close(4003, "Identidade já definida");
                    break;
                }
                if (autenticando) break;
                autenticando = true;
                try {
                    const autenticacao = await autenticarOuRegistrarDispositivo(id, msg.authToken, ipOrigem);
                    if (ws.readyState !== WebSocket.OPEN) break;
                    // @ts-ignore
                    if (autenticacao.limite) {
                        enviar(ws, { type: "identidade-indisponivel", mensagem: "O limite temporário de novos dispositivos foi atingido. Aguarde e tente novamente." });
                        ws.close(1013, "Limite de novos dispositivos");
                        break;
                    }
                    if (!autenticacao.ok) {
                        enviar(ws, {
                            type: "identidade-recusada",
                            mensagem: "Este deviceId já está vinculado a outra credencial. Não foi permitido assumir a identidade. Se você reinstalou o app ou apagou os dados do site, siga a recuperação descrita no LEIA-ME do pacote."
                        });
                        ws.close(4003, "Credencial de dispositivo inválida");
                        break;
                    }

                    // Só depois da prova de posse a conexão pode substituir uma sessão antiga.
                    const existente = dispositivos.get(id);
                    deviceId = id;
                    clearTimeout(timeoutIdentificacao);
                    if (!contabilizadaComoAutenticada) {
                        contabilizadaComoAutenticada = true;
                        conexoesNaoAutenticadas = Math.max(0, conexoesNaoAutenticadas - 1);
                    }
                    dispositivos.set(deviceId, { ws, nome });
                    wsPorDeviceId.set(wsId(ws), deviceId);
                    if (existente && existente.ws !== ws) existente.ws.terminate();

                    enviar(ws, {
                        type: "identidade-autenticada",
                        turnServers: gerarCredenciaisTurn(deviceId),
                        ...(autenticacao.novoToken ? { authToken: autenticacao.novoToken } : {}),
                    });
                } catch (error) {
                    console.error("[Auth] Não foi possível autenticar o dispositivo:", error);
                    enviar(ws, {
                        type: "identidade-indisponivel",
                        mensagem: "O servidor não conseguiu validar a identidade. Verifique se a tabela dispositivos_autenticacao foi criada no Supabase e tente novamente."
                    });
                    ws.close(1011, "Falha ao verificar identidade");
                } finally {
                    autenticando = false;
                }
                break;
            }

            // ── Tornar-se servidor (gerar código) ───────────────────────────
            case "tornar-servidor": {
                if (!socketAtual(ws, deviceId)) break;
                // @ts-ignore
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;
                // @ts-ignore
                criarCodigo(deviceId, nome);
                break;
            }

            // ── Parar de ser servidor ────────────────────────────────────────
            case "parar-servidor": {
                if (!socketAtual(ws, deviceId)) break;
                for (const [c, dId] of codigos) {
                    if (dId === deviceId) codigos.delete(c);
                }
                // @ts-ignore
                const timer = timersCodigo.get(deviceId);
                if (timer) clearTimeout(timer);
                // @ts-ignore
                timersCodigo.delete(deviceId);
                enviar(ws, { type: "servidor-parado" });
                break;
            }

            // ── Conectar por código ─────────────────────────────────────────
            case "conectar": {
                if (!socketAtual(ws, deviceId)) break;
                const codigo = String(msg.codigo || "");
                // @ts-ignore
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;

                const servidorId = codigos.get(codigo);
                if (!servidorId) {
                    enviar(ws, { type: "erro", mensagem: "Código inválido ou expirado." });
                    break;
                }
                if (servidorId === deviceId) {
                    enviar(ws, { type: "erro", mensagem: "Você não pode conectar ao próprio dispositivo." });
                    break;
                }
                const wsServidor = wsDeDevice(servidorId);
                if (!wsServidor) {
                    enviar(ws, { type: "erro", mensagem: "Dispositivo servidor está offline." });
                    break;
                }

                if (!permitirAcaoDispositivo(deviceId, "conectar") || !permitirPedidoRecebido(servidorId)) {
                    enviar(ws, { type: "erro", mensagem: "Muitos pedidos de conexão. Aguarde um minuto." });
                    break;
                }
                const sessaoId = gerarSessaoId();
                const pedido = criarPedidoPendente(sessaoId, deviceId, servidorId, "codigo");
                if (!pedido) {
                    enviar(ws, { type: "erro", mensagem: "Há muitos pedidos pendentes. Aguarde uma resposta antes de tentar novamente." });
                    break;
                }

                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "codigo", expiraEm: pedido.expiraEm });
                enviar(wsServidor, {
                    type: "pedido-conexao",
                    sessaoId,
                    nome,
                    deviceId,
                    via: "codigo",
                    expiraEm: pedido.expiraEm,
                    validadeMs: TEMPO_PEDIDO_MS,
                });
                break;
            }

            // ── Responder pedido (aceitar/recusar) ──────────────────────────
            case "responder-pedido": {
                if (!socketAtual(ws, deviceId)) break;
                const sessaoId = String(msg.sessaoId || "");
                const pedido = sessoesPendentes.get(sessaoId);
                if (!pedido) break;
                // @ts-ignore
                if (pedido.expiraEm && Date.now() >= pedido.expiraEm) {
                    encerrarPedidoPendente(sessaoId, "pedido-expirou");
                    break;
                }
                // Só o dispositivo alvo (servidor que recebeu o pedido) pode
                // aceitar ou recusar. O solicitante não pode aprovar a própria sessão.
                if (pedido.alvoId !== deviceId) {
                    enviar(ws, { type: "erro", mensagem: "Somente o dispositivo destinatário pode responder a este pedido." });
                    break;
                }
                removerPedidoPendente(sessaoId);

                const wsRequerente = wsDeDevice(pedido.requerenteId);
                const wsAlvo = wsDeDevice(pedido.alvoId);
                if (wsAlvo !== ws) break;

                if (!msg.aceitar) {
                    if (wsRequerente) enviar(wsRequerente, { type: "sessao-encerrada", sessaoId, motivo: "recusado" });
                    break;
                }

                const permissao = "leitura-escrita"; // único nível de permissão implementado pelos clientes atuais.
                sessoesAtivas.set(sessaoId, { id1: pedido.requerenteId, id2: pedido.alvoId });

                // Requerente é o "cliente" (quem pediu), alvo é o "servidor" (quem aceitou)
                if (wsRequerente) enviar(wsRequerente, {
                    type: "conectado",
                    sessaoId,
                    servidorNome: nomeDeDevice(pedido.alvoId),
                    servidorDeviceId: pedido.alvoId,
                    permissao,
                    mensagem: "Conexão estabelecida!",
                });
                if (wsAlvo) enviar(wsAlvo, {
                    type: "dispositivo-conectou",
                    sessaoId,
                    nome: nomeDeDevice(pedido.requerenteId),
                    clienteDeviceId: pedido.requerenteId,
                    permissao,
                });
                break;
            }

            // ── Encerrar sessão ─────────────────────────────────────────────
            case "encerrar": {
                if (!socketAtual(ws, deviceId)) break;
                const sessaoId = String(msg.sessaoId || "");
                const sessao = sessoesAtivas.get(sessaoId);
                if (!sessao || (sessao.id1 !== deviceId && sessao.id2 !== deviceId)) break;
                sessoesAtivas.delete(sessaoId);
                const outroId = sessao.id1 === deviceId ? sessao.id2 : sessao.id1;
                const wsOutro = wsDeDevice(outroId);
                if (wsOutro) enviar(wsOutro, { type: "sessao-encerrada", sessaoId, motivo: "encerrado-pelo-dispositivo" });
                break;
            }

            // ── Sinal WebRTC ────────────────────────────────────────────────
            case "sinal": {
                if (!socketAtual(ws, deviceId)) break;
                const sessaoId = String(msg.sessaoId || "");
                const sessao = sessoesAtivas.get(sessaoId);
                if (!sessao || (sessao.id1 !== deviceId && sessao.id2 !== deviceId)) break;
                const outroId = sessao.id1 === deviceId ? sessao.id2 : sessao.id1;
                const wsOutro = wsDeDevice(outroId);
                if (wsOutro) enviar(wsOutro, { type: "sinal", sessaoId, dados: msg.dados });
                break;
            }

            // ── Atualizar nome ──────────────────────────────────────────────
            case "atualizar-nome": {
                if (!socketAtual(ws, deviceId)) break;
                const nome = String(msg.nome || "").slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;
                // @ts-ignore
                atualizarAtividadeAuto(deviceId, null, nome);
                // Propaga para sessões ativas
                for (const [sessaoId, s] of sessoesAtivas) {
                    if (s.id1 === deviceId || s.id2 === deviceId) {
                        const outroId = s.id1 === deviceId ? s.id2 : s.id1;
                        const wsOutro = wsDeDevice(outroId);
                        if (wsOutro) enviar(wsOutro, { type: "sessao-nome-atualizado", sessaoId, nome });
                    }
                }
                break;
            }

            // ── Reconectar (por deviceId, com caixa de aprovação) ──────────
            case "reconectar": {
                if (!socketAtual(ws, deviceId)) break;
                const alvoId = String(msg.deviceIdAlvo || "");
                if (!alvoId || alvoId === deviceId) {
                    enviar(ws, { type: "erro", mensagem: "Você não pode reconectar ao próprio dispositivo." });
                    break;
                }
                // @ts-ignore
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;

                const wsAlvo = wsDeDevice(alvoId);
                if (!wsAlvo) {
                    enviar(ws, { type: "erro", mensagem: "Dispositivo não encontrado ou offline." });
                    break;
                }

                if (!permitirAcaoDispositivo(deviceId, "reconectar") || !permitirPedidoRecebido(alvoId)) {
                    enviar(ws, { type: "erro", mensagem: "Muitas tentativas de reconexão. Aguarde um minuto." });
                    break;
                }
                const sessaoId = gerarSessaoId();
                const pedido = criarPedidoPendente(sessaoId, deviceId, alvoId, "reconexao");
                if (!pedido) {
                    enviar(ws, { type: "erro", mensagem: "Há muitos pedidos pendentes para este dispositivo." });
                    break;
                }
                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "reconexao", expiraEm: pedido.expiraEm });
                enviar(wsAlvo, { type: "pedido-conexao", sessaoId, nome, deviceId, via: "reconexao", expiraEm: pedido.expiraEm, validadeMs: TEMPO_PEDIDO_MS });
                break;
            }

            // ── Reconectar silencioso (sem caixa, direto para o alvo decidir) ─
            case "reconectar-silencioso": {
                if (!socketAtual(ws, deviceId)) break;
                const alvoId = String(msg.deviceIdAlvo || "");
                if (!alvoId || alvoId === deviceId) break;
                // @ts-ignore
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;

                const wsAlvo = wsDeDevice(alvoId);
                if (!wsAlvo) break; // silencioso, não reporta erro

                if (!permitirAcaoDispositivo(deviceId, "reconectar-silencioso") || !permitirPedidoRecebido(alvoId)) break;
                const sessaoId = gerarSessaoId();
                const pedido = criarPedidoPendente(sessaoId, deviceId, alvoId, "reconexao");
                if (!pedido) break;
                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "reconexao", expiraEm: pedido.expiraEm });
                enviar(wsAlvo, { type: "pedido-conexao", sessaoId, nome, deviceId, via: "reconexao", expiraEm: pedido.expiraEm, validadeMs: TEMPO_PEDIDO_MS });
                break;
            }

            // ── Ping ────────────────────────────────────────────────────────
            case "ping":
                enviar(ws, { type: "pong" });
                break;

            // ════════════════════════════════════════════════════════════════
            // MODO AUTOMÁTICO
            // ════════════════════════════════════════════════════════════════

            // ── Registrar ID automático (app Android) ───────────────────────
            // O app envia o hash SHA-256 do seu PC-XXXXXXXX.
            // Nunca recebemos o ID em texto puro — apenas o hash.
            case "registrar-id-automatico": {
                if (!socketAtual(ws, deviceId)) break;
                const requestId = typeof msg.requestId === "string" && /^[a-f0-9]{36}$/i.test(msg.requestId)
                    ? msg.requestId : null;
                // @ts-ignore
                const responderRegistro = payload => enviar(ws, requestId ? { ...payload, requestId } : payload);
                if (!permitirOperacaoAutomatica(deviceId, "registrar")) {
                    responderRegistro({ type: "erro", mensagem: "Muitos registros de ID automático. Aguarde um minuto." });
                    break;
                }
                await enfileirarOperacaoAutomatica(deviceId, async () => {
                    if (!socketAtual(ws, deviceId)) return;
                    const idHash = String(msg.idHash || "").toLowerCase();
                    if (!/^[0-9a-f]{64}$/.test(idHash)) {
                        responderRegistro({ type: "erro", mensagem: "Hash de dispositivo inválido." });
                        return;
                    }
                    const donoEmMemoria = autoHashParaDeviceId.get(idHash);
                    if (donoEmMemoria && donoEmMemoria !== deviceId) {
                        responderRegistro({ type: "erro", mensagem: "Este ID automático já está associado a outro dispositivo." });
                        return;
                    }

                    // @ts-ignore
                    const nomeAtual = String(msg.nomeAtual || nomeDeDevice(deviceId) || "Dispositivo").slice(0, 60);
                    const agora = new Date().toISOString();
                    try {
                        // Não usamos upsert cego: isso poderia transferir a propriedade
                        // de um hash existente para outro deviceId em uma corrida.
                        const { data: existente, error: erroBusca } = await supabase
                            .from("dispositivos_automaticos")
                            .select("device_id")
                            .eq("id_hash", idHash)
                            .maybeSingle();
                        if (erroBusca) throw erroBusca;
                        if (existente?.device_id && existente.device_id !== deviceId) {
                            responderRegistro({ type: "erro", mensagem: "Este ID automático já pertence a outro dispositivo." });
                            return;
                        }

                        let resultado;
                        if (existente) {
                            resultado = await supabase
                                .from("dispositivos_automaticos")
                                .update({ nome_atual: nomeAtual, ativo: true, ultima_atividade: agora })
                                .eq("id_hash", idHash)
                                .eq("device_id", deviceId)
                                .select("id_hash")
                                .maybeSingle();
                        } else {
                            resultado = await supabase
                                .from("dispositivos_automaticos")
                                .insert({
                                    id_hash: idHash,
                                    device_id: deviceId,
                                    nome_atual: nomeAtual,
                                    ativo: true,
                                    criado_em: agora,
                                    ultima_atividade: agora,
                                })
                                .select("id_hash")
                                .maybeSingle();
                        }
                        if (resultado.error) throw resultado.error;
                        if (!resultado.data) {
                            responderRegistro({ type: "erro", mensagem: "Não foi possível confirmar a propriedade do ID automático." });
                            return;
                        }
                        if (!socketAtual(ws, deviceId)) return;
                        // @ts-ignore
                        autoHashParaDeviceId.set(idHash, deviceId);
                    } catch (e) {
                        console.error("[AutoMode] Supabase registro/validação falhou:", e);
                        responderRegistro({ type: "erro", mensagem: "Não foi possível validar o proprietário do ID automático." });
                        return;
                    }

                    responderRegistro({ type: "registrar-id-automatico-ok", idHash });
                    console.log("[AutoMode] Dispositivo registrado com hash.");
                });
                break;
            }

            // ── Conectar por ID (web / qualquer cliente) ────────────────────
            // O cliente envia o hash SHA-256 do ID que ele quer conectar.
            // O servidor localiza o dispositivo dono desse hash e envia
            // um "pedido-automatico" para ele. O app Android auto-aceita.
            case "conectar-por-id": {
                if (!socketAtual(ws, deviceId)) break;
                const idHash = String(msg.idHash || "").toLowerCase();
                // @ts-ignore
                const clienteNome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                if (!permitirAcaoDispositivo(deviceId, "conectar-por-id", 20) ||
                    !consumirJanela(consultasAutoGlobais, "global", MAX_CONSULTAS_AUTO_GLOBAL_MIN, 60 * 1000)) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Muitas consultas de ID automático. Aguarde um minuto." });
                    break;
                }
                if (!/^[0-9a-f]{64}$/.test(idHash)) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Hash inválido." });
                    break;
                }

                // Recusa explicitamente o próprio ID antes da consulta ao Supabase.
                // Assim, uma falha/atraso de registro não mascara o motivo como "ID não encontrado".
                if (autoHashParaDeviceId.get(idHash) === deviceId) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Você não pode conectar ao próprio dispositivo." });
                    break;
                }

                let registro;
                try {
                    const { data, error } = await supabase
                        .from("dispositivos_automaticos")
                        .select("device_id, ativo")
                        .eq("id_hash", idHash)
                        .maybeSingle();
                    if (error) throw error;
                    registro = data;
                } catch (e) {
                    console.error("[AutoMode] Falha ao validar o ID automático:", e);
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Não foi possível validar o dispositivo." });
                    break;
                }
                // Não cria sessão para um solicitante cujo socket foi substituído
                // enquanto a consulta ao Supabase estava em andamento.
                if (!socketAtual(ws, deviceId)) break;
                if (!registro || registro.ativo !== true || typeof registro.device_id !== "string") {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "ID não encontrado ou modo automático inativo." });
                    break;
                }

                const alvoId = registro.device_id;
                // Não há bloqueio por o solicitante também estar em modo servidor:
                // apenas a tentativa de conectar ao próprio deviceId é recusada.
                if (alvoId === deviceId) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Você não pode conectar ao próprio dispositivo." });
                    break;
                }
                if (autoHashParaDeviceId.get(idHash) !== alvoId) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "O dispositivo está offline ou não registrou o modo automático nesta conexão." });
                    break;
                }
                const wsAlvo = wsDeDevice(alvoId);
                if (!wsAlvo || wsAlvo.readyState !== WebSocket.OPEN || !socketAtual(wsAlvo, alvoId)) {
                    if (autoHashParaDeviceId.get(idHash) === alvoId) autoHashParaDeviceId.delete(idHash);
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "O dispositivo está offline no momento." });
                    break;
                }

                if (!socketAtual(ws, deviceId) || !socketAtual(wsAlvo, alvoId)) break;
                if (!permitirPedidoRecebido(alvoId)) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "O dispositivo de destino está recebendo muitos pedidos. Tente novamente em um minuto." });
                    break;
                }
                const sessaoId = gerarSessaoId();
                const pedido = criarPedidoPendente(sessaoId, deviceId, alvoId, "id-automatico");
                if (!pedido) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "O dispositivo de destino está com muitos pedidos pendentes." });
                    break;
                }
                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "id-automatico", expiraEm: pedido.expiraEm });
                enviar(wsAlvo, {
                    type: "pedido-automatico",
                    sessaoId,
                    clienteNome,
                    clienteDeviceId: deviceId,
                    expiraEm: pedido.expiraEm,
                    validadeMs: TEMPO_PEDIDO_MS,
                });
                // @ts-ignore
                atualizarAtividadeAuto(alvoId, idHash);
                console.log("[AutoMode] Pedido automático encaminhado.");
                break;
            }

            // ── Desregistrar ID automático ──────────────────────────────────
            case "desregistrar-id-automatico": {
                if (!socketAtual(ws, deviceId)) break;
                const idHash = String(msg.idHash || "").toLowerCase();
                const requestId = typeof msg.requestId === "string" && /^[A-Za-z0-9_-]{8,80}$/.test(msg.requestId)
                    ? msg.requestId : undefined;
                if (!permitirOperacaoAutomatica(deviceId, "desregistrar")) {
                    enviar(ws, {
                        type: "erro",
                        mensagem: "Muitas tentativas de desativação de ID automático. Aguarde um minuto.",
                        ...(requestId ? { requestId } : {}),
                        ...( /^[0-9a-f]{64}$/.test(idHash) ? { idHash } : {}),
                    });
                    break;
                }
                await enfileirarOperacaoAutomatica(deviceId, async () => {
                    if (!socketAtual(ws, deviceId)) return;
                    const responderErro = (/** @type {string} */ mensagem) => enviar(ws, {
                        type: "erro", mensagem, ...(requestId ? { requestId } : {}),
                        ...( /^[0-9a-f]{64}$/.test(idHash) ? { idHash } : {}),
                    });
                    const responderOk = (jaInativo = false) => enviar(ws, {
                        type: "desregistrar-id-automatico-ok", idHash,
                        ...(requestId ? { requestId } : {}),
                        ...(jaInativo ? { jaInativo: true } : {}),
                    });

                    if (!/^[0-9a-f]{64}$/.test(idHash)) {
                        responderErro("Hash de dispositivo inválido.");
                        return;
                    }
                    const donoEmMemoria = autoHashParaDeviceId.get(idHash);
                    if (donoEmMemoria && donoEmMemoria !== deviceId) {
                        responderErro("Você não é o proprietário deste ID automático.");
                        return;
                    }
                    try {
                        const { data: existente, error: erroBusca } = await supabase
                            .from("dispositivos_automaticos")
                            .select("device_id")
                            .eq("id_hash", idHash)
                            .maybeSingle();
                        if (erroBusca) throw erroBusca;
                        // Idempotência: se não existe registro, o ID já está revogado.
                        if (!existente) {
                            if (autoHashParaDeviceId.get(idHash) === deviceId) autoHashParaDeviceId.delete(idHash);
                            responderOk(true);
                            return;
                        }
                        if (existente.device_id !== deviceId) {
                            responderErro("Você não é o proprietário deste ID automático.");
                            return;
                        }
                        const { data: atualizado, error } = await supabase
                            .from("dispositivos_automaticos")
                            .update({ ativo: false, ultima_atividade: new Date().toISOString() })
                            .eq("id_hash", idHash)
                            .eq("device_id", deviceId)
                            .select("id_hash")
                            .maybeSingle();
                        if (error) throw error;
                        if (!atualizado) throw new Error("O registro não foi atualizado.");
                        if (autoHashParaDeviceId.get(idHash) === deviceId) autoHashParaDeviceId.delete(idHash);
                    } catch (e) {
                        console.error("[AutoMode] Supabase desregistro falhou:", e);
                        responderErro("Não foi possível confirmar a propriedade/revogação do ID automático.");
                        return;
                    }
                    responderOk(false);
                    console.log("[AutoMode] Dispositivo desativado no modo automático.");
                });
                break;
            }

            default:
                // Desconhecido — ignora silenciosamente
                break;
        }
    });

    ws.on("close", () => {
        clearTimeout(timeoutIdentificacao);
        conexoesAbertas = Math.max(0, conexoesAbertas - 1);
        const atual = Math.max(0, (conexoesPorIp.get(ipOrigem) || 1) - 1);
        if (atual === 0) conexoesPorIp.delete(ipOrigem);
        else conexoesPorIp.set(ipOrigem, atual);
        if (!contabilizadaComoAutenticada) conexoesNaoAutenticadas = Math.max(0, conexoesNaoAutenticadas - 1);

        const devId = wsPorDeviceId.get(wsId(ws));
        if (devId) {
            // Só desconecta se o ws atual ainda é o registrado (evita race condition)
            if (dispositivos.get(devId)?.ws === ws) desconectarDevice(devId);
            wsPorDeviceId.delete(wsId(ws));
        }
    });

    ws.on("error", (err) => {
        console.error("WebSocket erro:", err.message);
    });
});

wss.on("error", (err) => {
    console.error("Servidor erro:", err);
});

function limparEstadoServidor() {
    clearInterval(limpezaRateLimit);
    for (const sessaoId of sessoesPendentes.keys()) removerPedidoPendente(sessaoId);
    for (const timer of timersCodigo.values()) clearTimeout(timer);
    timersCodigo.clear();
}
process.once("SIGTERM", limparEstadoServidor);
process.once("SIGINT", limparEstadoServidor);

console.log("Physical Cloud Signaling Server iniciado.");
