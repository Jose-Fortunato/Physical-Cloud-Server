// server.js — Physical Cloud Signaling Server
// @ts-check
"use strict";

const { WebSocketServer, WebSocket } = require("ws");
const { createClient } = require("@supabase/supabase-js");

const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios.");
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// @ts-ignore
const wss = new WebSocketServer({ port: PORT });
console.log(`Servidor WebSocket rodando na porta ${PORT}`);

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
            sessoesPendentes.delete(sessaoId);
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
    // Remove hash automático
    for (const [hash, devId] of autoHashParaDeviceId) {
        if (devId === deviceId) autoHashParaDeviceId.delete(hash);
    }
    // Remove do mapa de dispositivos
    dispositivos.delete(deviceId);
}

// ─── Lógica de código de 6 dígitos ──────────────────────────────────────────

const VALIDADE_CODIGO_MS = 5 * 60 * 1000; // 5 minutos

/** @param {string} deviceId @param {string} nome */
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

wss.on("connection", (ws) => {
    wsId(ws); // garante _pcId
    /**
   * @type {string | null}
   */
    let deviceId = null;

    ws.on("message", async (raw) => {
        let msg;
        try { msg = JSON.parse(raw.toString()); } catch { return; }
        if (!msg || typeof msg.type !== "string") return;

        switch (msg.type) {

            // ── Identificação ───────────────────────────────────────────────
            case "identificar": {
                const id = String(msg.deviceId || "");
                const nome = String(msg.nome || "Dispositivo").slice(0, 60);
                if (!id) break;
                // Se já existe outro ws com esse deviceId, derruba o antigo
                const existente = dispositivos.get(id);
                if (existente && existente.ws !== ws) {
                    existente.ws.terminate();
                }
                deviceId = id;
                dispositivos.set(deviceId, { ws, nome });
                wsPorDeviceId.set(wsId(ws), deviceId);
                break;
            }

            // ── Tornar-se servidor (gerar código) ───────────────────────────
            case "tornar-servidor": {
                if (!deviceId) break;
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;
                criarCodigo(deviceId, nome);
                break;
            }

            // ── Parar de ser servidor ────────────────────────────────────────
            case "parar-servidor": {
                if (!deviceId) break;
                for (const [c, dId] of codigos) {
                    if (dId === deviceId) codigos.delete(c);
                }
                const timer = timersCodigo.get(deviceId);
                if (timer) clearTimeout(timer);
                timersCodigo.delete(deviceId);
                enviar(ws, { type: "servidor-parado" });
                break;
            }

            // ── Conectar por código ─────────────────────────────────────────
            case "conectar": {
                if (!deviceId) break;
                const codigo = String(msg.codigo || "");
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

                const sessaoId = gerarSessaoId();
                sessoesPendentes.set(sessaoId, {
                    requerenteId: deviceId,
                    alvoId: servidorId,
                    via: "codigo",
                });

                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "codigo" });
                enviar(wsServidor, {
                    type: "pedido-conexao",
                    sessaoId,
                    nome,
                    deviceId,
                    via: "codigo",
                });
                break;
            }

            // ── Responder pedido (aceitar/recusar) ──────────────────────────
            case "responder-pedido": {
                const sessaoId = String(msg.sessaoId || "");
                const pedido = sessoesPendentes.get(sessaoId);
                if (!pedido) break;
                sessoesPendentes.delete(sessaoId);

                const wsRequerente = wsDeDevice(pedido.requerenteId);
                const wsAlvo = wsDeDevice(pedido.alvoId);

                if (!msg.aceitar) {
                    if (wsRequerente) enviar(wsRequerente, { type: "sessao-encerrada", sessaoId, motivo: "recusado" });
                    break;
                }

                const permissao = String(msg.permissao || "leitura-escrita");
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
                const sessaoId = String(msg.sessaoId || "");
                const sessao = sessoesAtivas.get(sessaoId);
                if (!sessao) break;
                sessoesAtivas.delete(sessaoId);
                const outroId = sessao.id1 === deviceId ? sessao.id2 : sessao.id1;
                const wsOutro = wsDeDevice(outroId);
                if (wsOutro) enviar(wsOutro, { type: "sessao-encerrada", sessaoId, motivo: "encerrado-pelo-dispositivo" });
                break;
            }

            // ── Sinal WebRTC ────────────────────────────────────────────────
            case "sinal": {
                const sessaoId = String(msg.sessaoId || "");
                const sessao = sessoesAtivas.get(sessaoId);
                if (!sessao || !deviceId) break;
                const outroId = sessao.id1 === deviceId ? sessao.id2 : sessao.id1;
                const wsOutro = wsDeDevice(outroId);
                if (wsOutro) enviar(wsOutro, { type: "sinal", sessaoId, dados: msg.dados });
                break;
            }

            // ── Atualizar nome ──────────────────────────────────────────────
            case "atualizar-nome": {
                if (!deviceId) break;
                const nome = String(msg.nome || "").slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;
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
                if (!deviceId) break;
                const alvoId = String(msg.deviceIdAlvo || "");
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;

                const wsAlvo = wsDeDevice(alvoId);
                if (!wsAlvo) {
                    enviar(ws, { type: "erro", mensagem: "Dispositivo não encontrado ou offline." });
                    break;
                }

                const sessaoId = gerarSessaoId();
                sessoesPendentes.set(sessaoId, { requerenteId: deviceId, alvoId, via: "reconexao" });
                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "reconexao" });
                enviar(wsAlvo, { type: "pedido-conexao", sessaoId, nome, deviceId, via: "reconexao" });
                break;
            }

            // ── Reconectar silencioso (sem caixa, direto para o alvo decidir) ─
            case "reconectar-silencioso": {
                if (!deviceId) break;
                const alvoId = String(msg.deviceIdAlvo || "");
                const nome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                // @ts-ignore
                if (dispositivos.get(deviceId)) dispositivos.get(deviceId).nome = nome;

                const wsAlvo = wsDeDevice(alvoId);
                if (!wsAlvo) break; // silencioso, não reporta erro

                const sessaoId = gerarSessaoId();
                sessoesPendentes.set(sessaoId, { requerenteId: deviceId, alvoId, via: "reconexao" });
                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "reconexao" });
                enviar(wsAlvo, { type: "pedido-conexao", sessaoId, nome, deviceId, via: "reconexao" });
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
                if (!deviceId) break;
                const idHash = String(msg.idHash || "");
                if (!idHash || idHash.length !== 64) break; // SHA-256 hex = 64 chars

                // Salva em memória
                autoHashParaDeviceId.set(idHash, deviceId);

                // Persiste no Supabase (upsert — se já existir, atualiza)
                try {
                    const { error } = await supabase
                        .from("dispositivos_automaticos")
                        .upsert({ id_hash: idHash }, { onConflict: "id_hash" });
                    if (error) console.error("[AutoMode] Supabase upsert erro:", error.message);
                } catch (e) {
                    console.error("[AutoMode] Supabase upsert exceção:", e);
                }

                enviar(ws, { type: "registrar-id-automatico-ok" });
                console.log(`[AutoMode] Device ${deviceId} registrado com hash.`);
                break;
            }

            // ── Conectar por ID (web / qualquer cliente) ────────────────────
            // O cliente envia o hash SHA-256 do ID que ele quer conectar.
            // O servidor localiza o dispositivo dono desse hash e envia
            // um "pedido-automatico" para ele. O app Android auto-aceita.
            case "conectar-por-id": {
                if (!deviceId) break;
                const idHash = String(msg.idHash || "");
                const clienteNome = String(msg.nome || nomeDeDevice(deviceId)).slice(0, 60);
                if (!idHash || idHash.length !== 64) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Hash inválido." });
                    break;
                }

                // Busca em memória primeiro
                let alvoId = autoHashParaDeviceId.get(idHash) || null;

                // Se não está em memória (device nunca reconectou nessa sessão do servidor),
                // verifica o Supabase para confirmar que o hash existe, mas o device está offline.
                if (!alvoId) {
                    try {
                        const { data, error } = await supabase
                            .from("dispositivos_automaticos")
                            .select("id_hash")
                            .eq("id_hash", idHash)
                            .maybeSingle();
                        if (!error && data) {
                            // Hash existe no banco mas device está offline agora
                            enviar(ws, {
                                type: "conectar-por-id-nao-encontrado",
                                mensagem: "O dispositivo está offline no momento. Abra o app Physical Cloud e tente novamente.",
                            });
                        } else {
                            enviar(ws, {
                                type: "conectar-por-id-nao-encontrado",
                                mensagem: "ID não encontrado. Verifique o ID e certifique-se que o modo automático está ativado no app.",
                            });
                        }
                    } catch (e) {
                        enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Erro ao verificar dispositivo." });
                    }
                    break;
                }

                // Device está em memória — verifica se o ws ainda está aberto
                const wsAlvo = wsDeDevice(alvoId);
                if (!wsAlvo || wsAlvo.readyState !== WebSocket.OPEN) {
                    autoHashParaDeviceId.delete(idHash); // limpa entrada stale
                    enviar(ws, {
                        type: "conectar-por-id-nao-encontrado",
                        mensagem: "O dispositivo está offline no momento. Abra o app Physical Cloud e tente novamente.",
                    });
                    break;
                }

                // Não pode conectar a si mesmo
                if (alvoId === deviceId) {
                    enviar(ws, { type: "conectar-por-id-nao-encontrado", mensagem: "Você não pode conectar ao próprio dispositivo." });
                    break;
                }

                // Cria sessão pendente e notifica ambos
                const sessaoId = gerarSessaoId();
                sessoesPendentes.set(sessaoId, {
                    requerenteId: deviceId,
                    alvoId,
                    via: "id-automatico",
                });

                // Avisa o requerente que está aguardando
                enviar(ws, { type: "aguardando-aprovacao", sessaoId, via: "id-automatico" });

                // Envia pedido automático ao alvo (app Android)
                enviar(wsAlvo, {
                    type: "pedido-automatico",
                    sessaoId,
                    clienteNome,
                    clienteDeviceId: deviceId,
                });

                console.log(`[AutoMode] Pedido automático: ${deviceId} → ${alvoId}, sessao=${sessaoId}`);
                break;
            }

            default:
                // Desconhecido — ignora silenciosamente
                break;
        }
    });

    ws.on("close", () => {
        const devId = wsPorDeviceId.get(wsId(ws));
        if (devId) {
            // Só desconecta se o ws atual ainda é o registrado (evita race condition)
            if (dispositivos.get(devId)?.ws === ws) {
                desconectarDevice(devId);
            }
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

console.log("Physical Cloud Signaling Server iniciado.");
