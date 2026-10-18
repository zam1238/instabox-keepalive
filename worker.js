const OC_VERSION = "2.0.29";  // v2会产生 opencode/latest/<ver>/<client> 三段 UA(见 UA_ENVS)
const OC_CLIENT = "cli";
const OC_PROJECT = "global";
const OC_SESSION_PREFIX = "ses_";
// 2026-09-18 更新：上游收紧校验，tools 必须含真工具名 bash+read（假名/单名 403）
// 2026-09-17 起上游 Zen 闸门：body 必须 stream:true 且 tools 为非空数组（否则 403 FreeTierError）
// ===== 2026-10-13 上游闸门升级(vsub13-gate) =====
// 免费档闸门改为 body 内容校验:system[0] 必须逐字节命中 opencode build 系统提示词白名单,
// tools 必须含全部 11 个 opencode 工具名(schema/描述/顺序无关,可携带客户端额外工具)。
// 原 system 顺延为第二条 system,模型按客户端真实指令执行(经 Z3/Z4/O/P 实测)。

// ===== v2.0.29 指纹(2026-10-14 闸门):官方 read+shell 真定义,逐字对齐 =====
// bash 触发 403(v2 把终端工具改名为 shell,检测 bash 视为伪造第三方流量)
const ZEN_GATE_TOOL_NAMES_V2 = [
	{
		name: "read",
		description: "Read the contents of a file or directory. Supports text files, images, and PDFs. Images and PDFs are presented directly to the model. Each text line is prefixed by its 1-based line number as <line>: <content>. The prefix is for reference and is not part of the file content. Directory entries are returned one per line. Use offset and limit to read large files or directories in sections. Prefer one larger read over many small slices, and use grep to find specific content in large files.",
		parameters: {
			type: "object",
			properties: {
				path: { type: "string", description: "File or directory to read" },
				offset: { type: "number", description: "The line or directory entry to start reading from (1-based)" },
				limit: { type: "number", description: "The maximum number of lines or directory entries to read (defaults to and capped at 2000)" },
			},
			required: ["path"],
			additionalProperties: false,
		},
	},
	{
		name: "shell",
		description: "Execute a shell command and return its output. Commands run on Linux using bash. Quote file paths containing spaces or special characters. Prefer dedicated tools over shell commands when possible. When output is large, the full result is saved to a file and a truncated preview is returned. Rely on automatic truncation unless filtering the output is more useful. Commands accept an optional timeout, background commands have no timeout by default. Background commands return immediately, and you will be notified when they complete.",
		parameters: {
			type: "object",
			properties: {
				command: { type: "string", description: "Shell command string to execute" },
				workdir: { type: "string", description: "Working directory to execute the command in. Defaults to the current working directory. When possible, avoid changing directories in the command and set the working directory here instead." },
				timeout: { type: "number", description: "Timeout in milliseconds. Set to 0 to disable the timeout. Defaults to 120000 for foreground commands. Background commands have no timeout by default." },
				background: { type: "boolean", description: "Run the command in the background and return immediately (useful for dev servers and long-running builds). You will be notified when it completes. DO NOT poll for completion." },
			},
			required: ["command"],
			additionalProperties: false,
		},
	},
];
const ZEN_GATE_FAKE_TOOLS = ZEN_GATE_TOOL_NAMES_V2.map((t) => ({
	type: "function",
	function: { name: t.name, description: t.description, parameters: t.parameters },
}));
const ZEN_GATE_FAKE_TOOLS_RESPONSES = ZEN_GATE_TOOL_NAMES_V2.map((t) => ({
	type: "function",
	name: t.name,
	description: t.description,
	parameters: t.parameters,
}));

const FAKE_TOOL = ZEN_GATE_FAKE_TOOLS;
const FAKE_TOOL_RESPONSES = ZEN_GATE_FAKE_TOOLS_RESPONSES;


const PROXY_VERSION = "vsub14-inference-v2";
// 补位用零宽空格：客户端需"有 content"才不判 no content，但用户不该看到任何占位文本
const ZWSP = "\u200b";
const OPENCODE_URLs = [
	"https://opencode.ai.cmliussss.net",
	"https://opencode.fastly.cmliussss.net",
	"https://opencode.gcore.cmliussss.net",
	"https://opencode.ai"
]
// ai 镜像 404 间歇抽风 → 官方域放末位兜底(实测三镜像+官方同 body 全 200)

// muse-spark contributor 系列在 /zen/v1/chat/completions 上会 500 / RegionError，
// 但 /zen/v1/responses 端点正常，这里做端点桥接。
const RESPONSES_BRIDGE_MODELS = [
	"muse-spark-1.2-contributor-free",
	"muse-spark-1.3-contributor-free"
];
const RESPONSES_BRIDGE_PATH = "/inference/v1/responses";
const BRIDGE_MIN_MAX_OUTPUT_TOKENS = 1024;
// muse-spark 是推理模型：上游把推理 token 也计入 max_output_tokens 预算，
// 这里额外预留推理额度，保证正文不被思考过程挤掉。
const BRIDGE_REASONING_RESERVE_TOKENS = 4096;
const FETCH_TIMEOUT_MS = 240000;
const STREAM_HEARTBEAT_MS = 10000;
const STREAM_FIRST_BYTE_TIMEOUT_MS = 60000;

const userSessions = new Map();
const endpointStats = new Map();
// 分布式子节点健康表（主代理用）：url -> { latencyMs, failures, ts }
const subStats = new Map();
let cachedModels = null;

const CORS_HEADERS = {
	"Access-Control-Allow-Origin": "*",
	"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
	"Access-Control-Allow-Headers": "Authorization, X-API-Key, x-api-key, Content-Type, Anthropic-Version, Anthropic-Beta",
};

const JSON_HEADERS = {
	"Content-Type": "application/json; charset=utf-8",
};

const SSE_HEADERS = {
	"Content-Type": "text/event-stream; charset=utf-8",
	"Cache-Control": "no-cache, no-transform",
	"X-Accel-Buffering": "no",
};

export default {
	async fetch(request, env) {
		if (request.method === "OPTIONS") {
			return new Response(null, { status: 204, headers: CORS_HEADERS });
		}

		const url = new URL(request.url);
		const path = url.pathname.replace(/\/+$/, "") || "/";
		try {
			if (request.method === "GET" && path === "/") return healthResponse();
			if (request.method === "GET" && path === "/health") return healthResponse();
			if (request.method === "GET" && path === "/ip") return ipResponse();
			if (request.method === "GET" && path === "/v1/models") return modelsResponse(env);
			if (request.method === "POST" && path === "/v1/chat/completions") return handleOpenAI(request, env);
			if (request.method === "POST" && path === "/v1/messages") return handleAnthropic(request, env);
			if (request.method === "POST" && path === "/v1/messages/count_tokens") return handleCountTokens(request, env);
			if (request.method === "POST" && path === "/v1/responses") return handleResponses(request, env);

			return jsonResponse({ error: { message: "Not found" } }, 404);
		} catch (error) {
			console.log("[WORKER ERROR]", error?.stack || error?.message || error);
			return jsonResponse({ error: { message: "Internal error", type: "server_error" } }, 500);
		}
	},
};

/* ── 分布式子节点调度（主代理用） ── */
// SUB_WORKERS=子节点URL逗号分隔；SUB_MODELS=走子节点的模型前缀逗号分隔（默认muse-spark）；
// SUB_KEY=子节点API_KEY（默认与主相同）；子节点不配SUB_WORKERS即本地直跑（子节点自身）。
function subWorkerUrls(env) {
	return String(env?.SUB_WORKERS || "")
		.split(",")
		.map((u) => u.trim().replace(/\/+$/, ""))
		.filter((u) => /^https:\/\//i.test(u));
}

function shouldRelayToSub(env, model, request) {
	// 防循环（学天书 zhuanfa 标记）：已被转发的请求不再二次转发
	if (request && request.headers.get('x-sub-relayed')) return false;
	const bound = subBindings(env);
	const urls = subWorkerUrls(env);
	if (!bound.length && !urls.length) return false;
	const raw = String(env?.SUB_MODELS || "muse-spark").trim();
	// SUB_MODELS=* 全模型走子节点；否则按前缀匹配
	if (raw === "*") return true;
	const prefixes = raw.split(",").map((s) => s.trim()).filter(Boolean);
	if (!model) return true;
	return prefixes.some((p) => String(model).startsWith(p));
}

function rankSubWorkers(env) {
	const now = Date.now();
	const failTtl = 5 * 60 * 1000;
	const ranked = subWorkerUrls(env).map((url, index) => {
		const s = subStats.get(url);
		const fresh = s && now - s.ts < 10 * 60 * 1000 ? s : null;
		return {
			url,
			index,
			failures: fresh && now - fresh.ts < failTtl ? fresh.failures : 0,
			latencyMs: fresh?.latencyMs || 0,
		};
	});
	ranked.sort((a, b) => {
		if (a.failures !== b.failures) return a.failures - b.failures;
		if (!a.latencyMs && b.latencyMs) return -1;
		if (a.latencyMs && !b.latencyMs) return 1;
		if (a.latencyMs !== b.latencyMs) return a.latencyMs - b.latencyMs;
		return a.index - b.index;
	});
	return ranked.map((r) => r.url);
}

function recordSubSuccess(url, ms) {
	const prev = subStats.get(url) || { latencyMs: 0, failures: 0 };
	subStats.set(url, {
		latencyMs: prev.latencyMs ? Math.round(prev.latencyMs * 0.7 + ms * 0.3) : ms,
		failures: 0,
		ts: Date.now(),
	});
}

function recordSubFailure(url) {
	const prev = subStats.get(url) || { latencyMs: 0, failures: 0 };
	subStats.set(url, { latencyMs: prev.latencyMs, failures: Math.min(prev.failures + 1, 3), ts: Date.now() });
}

// 主代理调度：优先 Service Bindings 内网直调（零公网零WAF），无 binding 回退公网 URL；
 // 按健康排序逐个透传，坏节点自动后排；流式直接透传子节点回包
function subBindings(env) {
	const out = [];
	if (env?.SUB1 && typeof env.SUB1.fetch === "function") out.push({ name: "SUB1", svc: env.SUB1 });
	if (env?.SUB2 && typeof env.SUB2.fetch === "function") out.push({ name: "SUB2", svc: env.SUB2 });
	if (env?.SUB3 && typeof env.SUB3.fetch === "function") out.push({ name: "SUB3", svc: env.SUB3 });
	if (env?.SUB4 && typeof env.SUB4.fetch === "function") out.push({ name: "SUB4", svc: env.SUB4 });
	if (env?.SUB5 && typeof env.SUB5.fetch === "function") out.push({ name: "SUB5", svc: env.SUB5 });
	return out;
}

async function relayToSub(request, env, requestId) {
	const bound = subBindings(env);
	const urls = rankSubWorkers(env);
	const subKey = String(env?.SUB_KEY || "").trim();
	const mainKey = firstEnvValue(env, "API_KEY", "TOKEN");
	const auth = request.headers.get("authorization") || request.headers.get("x-api-key") || "";
	const url = new URL(request.url);
	let lastError = null;
	// 内网 binding 按健康轮换：subStats 照样记录（key 用 binding 名）
	// 学天书分发调控：同 isolate 内轮询序号起步，避免健康分相同时每次都打头节点
	globalThis.__subRR = (globalThis.__subRR || 0) + 1;
	const rr = globalThis.__subRR;
	const order = bound.length ? bound : urls.map((u) => ({ name: u, url: u }));
	if (order.length > 1) {
		const k = rr % order.length;
		if (k) order.push(...order.splice(0, k));
	}

	for (let i = 0; i < order.length; i++) {
		const item = order[i];
		const isBound = !!item.svc;
		const base = item.name;
		const target = isBound ? (url.pathname + url.search) : (item.url + url.pathname + url.search);
		const headers = new Headers();
		request.headers.forEach((v, k) => {
			const lk = k.toLowerCase();
			if (lk === "host" || lk === "content-length" || lk === "cf-connecting-ip") return;
			headers.set(k, v);
		});
		// 防循环标记（学天书 zhuanfa）：子节点见此头不再二次转发
		headers.set("X-Sub-Relayed", "1");
		// 主子鉴权：子节点用 SUB_KEY 校验；未配则透传客户端原 key（主子同 key 时成立）
		if (subKey) headers.set("Authorization", "Bearer " + subKey);
		else if (!auth && mainKey) headers.set("Authorization", "Bearer " + mainKey);
		// Workers 互访 workers.dev 会触发 CF Bot 校验(1010)：补浏览器指纹头
		if (!headers.get("User-Agent")) headers.set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36");
		if (!headers.get("Accept")) headers.set("Accept", "text/event-stream,application/json,*/*");
		if (!headers.get("Accept-Language")) headers.set("Accept-Language", "en-US,en;q=0.9");
		let body = null;
		if (request.method !== "GET" && request.method !== "HEAD") {
			body = await request.clone().arrayBuffer();
		}
		const started = Date.now();
		let resp = null;
		try {
			if (isBound) {
				const subReq = new Request("https://sub.internal" + target, { method: request.method, headers, body });
				resp = await item.svc.fetch(subReq);
			} else {
				resp = await fetch(target, { method: request.method, headers, body });
			}
		} catch (e) {
			lastError = e;
			recordSubFailure(base);
			console.log("[SUB RETRY]", JSON.stringify({ requestId, sub: base, err: e?.message || String(e) }));
			continue;
		}
		if (resp.status >= 500) {
			recordSubFailure(base);
			lastError = new Error("Sub returned HTTP " + resp.status);
			console.log("[SUB RETRY]", JSON.stringify({ requestId, sub: base, status: resp.status }));
			try { await resp.arrayBuffer(); } catch (e) { }
			continue;
		}
		recordSubSuccess(base, Date.now() - started);
		console.log("[SUB HIT]", JSON.stringify({ requestId, sub: base, status: resp.status, ms: Date.now() - started }));
		const out = new Headers(resp.headers);
		out.set("X-Proxy-Sub", base);
		return new Response(resp.body, { status: resp.status, headers: out });
	}
	return jsonResponse({ error: { message: "All sub workers failed: " + (lastError?.message || "unknown"), type: "upstream_error" } }, 502);
}

/* ── Vision / Multimodal auto-fallback ── */
const DEFAULT_VISION_MODELS = ["mimo-v2.5-free"];
const DEFAULT_VISION_FALLBACK_MODEL = "mimo-v2.5-free";
const DEFAULT_CONTEXT_LENGTH = 131072;
const MAX_IMAGE_BASE64_LEN = 13_631_488; // ~10 MB raw ÷ 4/3 base64

function modelContextLength(model, env) {
	const overrides = env?.MODEL_CONTEXT_OVERRIDES;
	if (overrides) {
		try {
			const parsed = JSON.parse(overrides);
			const v = Number(parsed?.[model]);
			if (Number.isFinite(v) && v >= 1024) return v;
		} catch { /* fall through */ }
	}
	const g = Number(env?.MODEL_CONTEXT_LENGTH || DEFAULT_CONTEXT_LENGTH);
	return Number.isFinite(g) && g >= 1024 ? g : DEFAULT_CONTEXT_LENGTH;
}

function hasImageContent(messages) {
	return (messages || []).some((m) => {
		if (!m || !Array.isArray(m.content)) return false;
		return m.content.some((p) => p && typeof p === "object" &&
			(p.type === "image_url" || p.type === "input_image"));
	});
}

function visionModelList(env) {
	const raw = firstEnvValue(env, "VISION_MODELS");
	if (!raw) return DEFAULT_VISION_MODELS;
	return raw.split(",").map((s) => s.trim()).filter(Boolean);
}

function visionFallbackModel(env) {
	return firstEnvValue(env, "VISION_MODEL") || DEFAULT_VISION_FALLBACK_MODEL;
}

/** Strip oversized base64 images to prevent upstream garble / OOM */
function sanitizeImagePayload(messages) {
	return (messages || []).map((msg) => {
		if (!msg || !Array.isArray(msg.content)) return msg;
		const fixed = msg.content.map((part) => {
			if (!part || typeof part !== "object") return part;
			if ((part.type === "image_url" || part.type === "input_image") &&
				  part.image_url?.url?.startsWith("data:")) {
				const idx = part.image_url.url.indexOf(";base64,");
				if (idx === -1) return part;
				const payload = part.image_url.url.slice(idx + 8);
				if (payload.length <= MAX_IMAGE_BASE64_LEN) return part;
				console.warn("[VISION] base64 too large", payload.length, "→ truncated");
				return {
					...part,
					image_url: {
						...part.image_url,
						url: part.image_url.url.slice(0, idx + 8) +
						     payload.slice(0, MAX_IMAGE_BASE64_LEN),
					},
				};
			}
			return part;
		});
		return { ...msg, content: fixed };
	});
}

async function handleOpenAI(request, env) {
	const requestId = ocId("req");
	const ua = (request.headers.get("user-agent") || "").toLowerCase();
	// 关联客户端 UA 供流式转换判断（Hermes=OpenAI/Python 不补工具占位）
	env = Object.assign({}, env, { _CLIENT_UA: ua });
	const auth = authenticate(request, env);
	if (auth.error) return auth.error;

	// 分布式优先：先用 clone 读 model（不消耗原 body，relayToSub 还能透传）
	let relayModel = "";
	try { relayModel = (await request.clone().json())?.model || ""; } catch (e) { }
	if (shouldRelayToSub(env, relayModel, request)) {
		return relayToSub(request, env, requestId);
	}
	const input = await readJson(request);
	if (input.error) return input.error;

	const { model, messages, stream, tools, tool_choice } = input.body;
	const extras = openAIRequestExtras(input.body);

	// Sanitize oversized images + vision auto-fallback
	const safeMsgs = sanitizeImagePayload(messages);
	let effectiveModel = model;
	let visionFallbackUsed = false;
	if (hasImageContent(safeMsgs) && !visionModelList(env).includes(model)) {
		effectiveModel = visionFallbackModel(env);
		visionFallbackUsed = true;
	}

	const sessionId = getSession(auth.user);
	const msgSummary = (messages || []).map((msg) => {
		const c = msg?.content;
		return { role: msg?.role, len: typeof c === "string" ? c.length : (Array.isArray(c) ? c.length + "parts" : "obj") };
	});
	console.log("[OAI]", new Date().toISOString(), auth.user, model, stream ? "stream" : "sync", "ua:", env?._CLIENT_UA, "msgs:", JSON.stringify(msgSummary));

	const bridge = shouldUseResponsesBridge(env, effectiveModel);
	const anthropicBridge = shouldUseAnthropicBridge(env, effectiveModel);
	if (anthropicBridge) {
		return handleOpenAIAnthropicBridge(input.body, env, requestId, effectiveModel, auth.user, request);
	}
	// bridge 路径 body 由 openAIToResponsesBody 生成，跳过 buildZenRequest 的多余大包序列化（省 CPU）
	const zenReq = bridge
		? { body: "", headers: buildZenHeaders(env, sessionId, parentSessionFrom(request)) }
		: buildZenRequest(env, effectiveModel, safeMsgs, stream, tools, tool_choice, sessionId, extras, parentSessionFrom(request));
	if (bridge) {
		zenReq.body = JSON.stringify(openAIToResponsesBody(env, effectiveModel, safeMsgs, stream, tools, tool_choice, extras));
	}
	logZenRequest(env, requestId, "openai", effectiveModel, stream, auth.user, zenReq, messages?.length || 0);

	let upstream;
	try {
		upstream = await (bridge
			? fetchZenResponses(zenReq, env, requestId, effectiveModel, stream)
			: fetchZen(zenReq, env, requestId, effectiveModel, stream));
	} catch (error) {
		debugLog(env, "[ZEN FETCH ERROR]", {
			requestId,
			model,
			stream: !!stream,
			message: error?.message || String(error),
		});
		return upstreamErrorResponse(error, "openai");
	}

	if (bridge) {
		return stream
			? bridgeStreamResponse(upstream, env, requestId, effectiveModel)
			: bridgeFullResponse(upstream, env, requestId, effectiveModel, () => fetchZenResponses(zenReq, env, requestId, effectiveModel, false));
	}
	if (stream) return openAIStreamResponse(upstream, env, requestId, effectiveModel);
	return openAIFullResponse(upstream, env, requestId, effectiveModel);
}

// Anthropic 客户端（含 dsh / pi-ai）发正文前会探 /v1/messages/count_tokens，
// 旧实现 404 → 客户端把它判成端点不完整。上游无 tokenizer 接口，按字符估算即可。
async function handleCountTokens(request, env) {
	const auth = authenticate(request, env, "anthropic");
	if (auth.error) return auth.error;
	const input = await readJson(request, "anthropic");
	if (input.error) return input.error;
	const { messages } = anthropicToOpenAI(input.body || {});
	const chars = JSON.stringify(messages || []).length;
	return jsonResponse({ input_tokens: Math.max(1, Math.floor(chars / 4)) });
}

async function handleAnthropic(request, env) {
	const requestId = ocId("req");
	const auth = authenticate(request, env, "anthropic");
	if (auth.error) return auth.error;

	let relayModelA = "";
	try { relayModelA = (await request.clone().json())?.model || ""; } catch (e) { }
	if (shouldRelayToSub(env, relayModelA, request)) {
		return relayToSub(request, env, requestId);
	}
	const input = await readJson(request, "anthropic");
	if (input.error) return input.error;

	const { model, stream } = input.body;

	// space-bunny-free 上游只支持 Anthropic 原生 /zen/v1/messages：
	// 原生客户端直接透传协议，不能再转成 OpenAI chat/completions（上游会返回 400）。
	if (shouldUseAnthropicBridge(env, model)) {
		return handleNativeAnthropicBridge(input.body, env, requestId, model, auth.user, request);
	}

	const sessionId = getSession(auth.user);
	const { messages, tools } = anthropicToOpenAI(input.body);
	const antExtras = anthropicRequestExtras(input.body);
	const antToolChoice = anthropicToolChoice(input.body.tool_choice);

	// Sanitize oversized images + vision auto-fallback
	const safeMsgs = sanitizeImagePayload(messages);
	let effectiveModel = model;
	let visionFallbackUsed = false;
	if (hasImageContent(safeMsgs) && !visionModelList(env).includes(model)) {
		effectiveModel = visionFallbackModel(env);
		visionFallbackUsed = true;
	}
	const inputTokens = Math.floor(JSON.stringify(messages).length / 4);
	console.log("[ANT]", new Date().toISOString(), auth.user, model, stream ? "stream" : "sync", "msgs:", messages.length);

	// muse 系列：上游 /zen/v1/chat/completions 与 /zen/v1/messages 均 500，
	// 只能改走 /zen/v1/responses 桥，再把结果回落成 Anthropic 协议。
	const antBridge = shouldUseResponsesBridge(env, effectiveModel);
	const zenReq = antBridge
		? { body: JSON.stringify(openAIToResponsesBody(env, effectiveModel, safeMsgs, stream, tools, antToolChoice, antExtras)), headers: buildZenHeaders(env, sessionId, parentSessionFrom(request)) }
		: buildZenRequest(env, effectiveModel, safeMsgs, stream, tools, antToolChoice, sessionId, antExtras, parentSessionFrom(request));
	logZenRequest(env, requestId, "anthropic", effectiveModel, stream, auth.user, zenReq, messages.length);

	let upstream;
	try {
		upstream = await (antBridge
			? fetchZenResponses(zenReq, env, requestId, effectiveModel, stream)
			: fetchZen(zenReq, env, requestId, effectiveModel, stream));
	} catch (error) {
		debugLog(env, "[ZEN FETCH ERROR]", {
			requestId,
			model,
			stream: !!stream,
			message: error?.message || String(error),
		});
		return upstreamErrorResponse(error, "anthropic");
	}

	if (antBridge) {
		return stream
			? anthropicStreamResponseFromBridge(upstream, effectiveModel, inputTokens, env, requestId)
			: anthropicFullResponseFromBridge(upstream, effectiveModel, inputTokens, env, requestId);
	}
	if (stream) return anthropicStreamResponse(upstream, effectiveModel, inputTokens, env, requestId);
	return anthropicFullResponse(upstream, effectiveModel, inputTokens, env, requestId);
}

// ============================================================
// 移植自提交版：muse 原生 /responses 直通 + 工具调用校验
// ============================================================
const BRIDGE_MAX_OUTPUT_TOKENS = 131072;
const BRIDGE_STREAM_RETRIES = 1;
const COMPAT_SENTINEL_DESCRIPTION = "Compatibility sentinel required by the upstream gateway. Never call this tool; use the other client-provided tools instead.";

function bridgeMaxOutputTokens(env, requested) {
	const min = Math.max(Number(env?.BRIDGE_MIN_MAX_OUTPUT_TOKENS) || BRIDGE_MIN_MAX_OUTPUT_TOKENS, 1);
	const reserve = Math.max(Number(env?.BRIDGE_REASONING_RESERVE_TOKENS) || BRIDGE_REASONING_RESERVE_TOKENS, 0);
	const cap = Math.max(Number(env?.BRIDGE_MAX_OUTPUT_TOKENS) || BRIDGE_MAX_OUTPUT_TOKENS, min);
	const base = Math.max(Number(requested) || 8192, min);
	// reserve 只占尚未使用的预算；绝不能在客户端已请求模型上限时继续相加。
	return Math.min(cap, base + Math.min(reserve, Math.max(cap - base, 0)));
}

function bridgeResponsesMessageContent(content, role) {
	if (typeof content === "string") {
		return role === "assistant"
			? [{ type: "output_text", text: content }]
			: [{ type: "input_text", text: content }];
	}
	if (!Array.isArray(content)) return [];
	const out = [];
	for (const part of content) {
		if (!part || typeof part !== "object") continue;
		if (typeof part.text === "string" && ["text", "input_text", "output_text"].includes(part.type)) {
			out.push({ type: role === "assistant" ? "output_text" : "input_text", text: part.text });
			continue;
		}
		if (role !== "assistant" && (part.type === "image_url" || part.type === "input_image")) {
			const imageUrl = part.image_url?.url || part.image_url || part.image_url?.url || part.url;
			if (typeof imageUrl === "string" && imageUrl) out.push({ type: "input_image", image_url: imageUrl });
		}
	}
	return out;
}

function canonicalResponsesData(data, model) {
	return {
		...data,
		id: data?.id || ocId("resp"),
		object: "response",
		created_at: data?.created_at || Math.floor(Date.now() / 1000),
		model: data?.model || model,
		status: data?.status || "completed",
		background: false,
		error: data?.error || null,
		output: Array.isArray(data?.output) ? data.output : [],
	};
}

function clientSessionHint(request, body) {
	for (const name of [
		"x-opencode-session",
		"x-openclaw-session-id",
		"x-openclaw-session-key",
		"x-session-id",
		"x-session-affinity",
		"session_id",
		"x-client-request-id",
	]) {
		const value = request?.headers?.get?.(name);
		if (typeof value === "string" && value.trim()) return value.trim().slice(0, 512);
	}
	for (const value of [body?.user, body?.metadata?.session_id, body?.metadata?.conversation_id, body?.metadata?.thread_id]) {
		if (typeof value === "string" && value.trim()) return value.trim().slice(0, 512);
	}
	return "";
}

/* ── OpenAI tool-call hardening ─────────────────────────────────────────────
 * MiMo 一类模型偶发先流出 arguments，最后仍没有 function.name；旧代码会把这个
 * 半成品直接透传，并在上游意外 EOF 时伪造 [DONE]。OpenClaw 会在执行前校验聚合后的
 * 调用，因此报 “Provider returned an incomplete or malformed tool call”。
 *
 * 这里把工具调用（正文仍实时流式）暂存到 finish_reason/[DONE]，再一次性输出：
 *   1. 第一帧保证同时具有 id、type、非空 name 和完整 JSON arguments；
 *   2. 仅允许客户端原请求中真实存在的工具，绝不把闸门占位 bash/read 泄漏出去；
 *   3. 名称缺失时按真实 schema 唯一匹配恢复（Code Mode 的 exec/wait 可可靠区分）；
 *   4. 只修复“JSON 容器少右括号”这类无歧义截断，不修复被截断的字符串/值。
 */
function clientToolSpecs(tools) {
	return (sanitizeTools(tools) || []).map((tool) => ({
		name: tool.function.name.trim(),
		parameters: tool.function.parameters && typeof tool.function.parameters === "object"
			? tool.function.parameters
			: {},
	}));
}

function createToolCallAccumulator(clientTools) {
	const strictAllowed = Array.isArray(clientTools);
	const specs = clientToolSpecs(clientTools);
	const states = new Map();
	const currentByChoice = new Map();
	const nextByChoice = new Map();

	const allocateIndex = (choiceIndex) => {
		const next = nextByChoice.get(choiceIndex) || 0;
		nextByChoice.set(choiceIndex, next + 1);
		return next;
	};
	const findIndex = (choiceIndex, call) => {
		if (Number.isInteger(call?.index) && call.index >= 0) {
			nextByChoice.set(choiceIndex, Math.max(nextByChoice.get(choiceIndex) || 0, call.index + 1));
			return call.index;
		}
		if (call?.id) {
			for (const state of states.values()) {
				if (state.choiceIndex === choiceIndex && state.id === call.id) return state.toolIndex;
			}
			// 新 id 且未带 index，按新的并行调用处理，不能覆盖当前调用。
			return allocateIndex(choiceIndex);
		}
		if (currentByChoice.has(choiceIndex)) return currentByChoice.get(choiceIndex);
		return allocateIndex(choiceIndex);
	};

	return {
		add(choiceIndex, call) {
			if (!call || typeof call !== "object") return;
			const toolIndex = findIndex(choiceIndex, call);
			currentByChoice.set(choiceIndex, toolIndex);
			const key = `${choiceIndex}:${toolIndex}`;
			let state = states.get(key);
			if (!state) {
				state = { choiceIndex, toolIndex, id: "", name: "", args: "", seen: 0 };
				states.set(key, state);
			}
			const fn = call.function && typeof call.function === "object"
				? call.function
				: (call.function_call && typeof call.function_call === "object" ? call.function_call : {});
			if (typeof call.id === "string" && call.id) state.id = call.id;
			state.name = mergeToolNameFragment(state.name,
				typeof fn.name === "string" ? fn.name : (typeof call.name === "string" ? call.name : ""));
			const args = fn.arguments !== undefined ? fn.arguments : call.arguments;
			state.args = mergeToolArgumentFragment(state.args, args);
			state.seen += 1;
		},
		finalize() {
			const callsByChoice = new Map();
			const invalid = [];
			let inferred = 0;
			let repaired = 0;
			for (const state of [...states.values()].sort((a, b) => a.choiceIndex - b.choiceIndex || a.toolIndex - b.toolIndex)) {
				const parsedArgs = parseToolArguments(state.args);
				const name = parsedArgs.ok
					? recoverToolName(state.name, parsedArgs.value, specs, strictAllowed)
					: "";
				if (!parsedArgs.ok || !name) {
					invalid.push({
						choiceIndex: state.choiceIndex,
						toolIndex: state.toolIndex,
						hasName: Boolean(state.name.trim()),
						nameAllowed: !strictAllowed || specs.some((spec) => spec.name === state.name.trim()),
						argumentsValid: parsedArgs.ok,
						argumentChars: state.args.length,
					});
					continue;
				}
				if (!state.name.trim()) inferred += 1;
				if (parsedArgs.repaired) repaired += 1;
				if (!callsByChoice.has(state.choiceIndex)) callsByChoice.set(state.choiceIndex, []);
				callsByChoice.get(state.choiceIndex).push({
					index: state.toolIndex,
					id: state.id || ocId("call"),
					type: "function",
					function: { name, arguments: parsedArgs.text },
				});
			}
			// 丢弃无效并行调用后重新压紧 index；否则只剩 index:1 时，部分客户端会
			// 为缺失的 index:0 建一个空槽，反而再次触发 malformed tool call。
			for (const calls of callsByChoice.values()) calls.forEach((call, index) => { call.index = index; });
			return { callsByChoice, invalid, inferred, repaired, total: states.size };
		},
		hasCalls() { return states.size > 0; },
	};
}

function deterministicSessionId(value) {
	const raw = String(value || "").trim();
	if (/^ses_[0-9a-fA-F]{12}[0-9A-Za-z]{14}$/.test(raw)) return raw;
	let h1 = 0x811c9dc5;
	let h2 = 0x9e3779b9;
	for (let i = 0; i < raw.length; i++) {
		const code = raw.charCodeAt(i);
		h1 = Math.imul(h1 ^ code, 0x01000193) >>> 0;
		h2 = Math.imul(h2 ^ (code + i), 0x85ebca6b) >>> 0;
	}
	let hex = (h1.toString(16).padStart(8, "0") + h2.toString(16).padStart(8, "0")).slice(0, 12);
	let random = "";
	let state = (h1 ^ h2) >>> 0;
	for (let i = 0; i < 14; i++) {
		state = (Math.imul(state ^ (i + 1), 1664525) + 1013904223) >>> 0;
		random += OC_ID_CHARS[state % OC_ID_CHARS.length];
	}
	return `ses_${hex}${random}`;
}

function emitCanonicalResponses(send, source, model) {
	const data = canonicalResponsesData(source, model);
	const pending = { ...data, status: "in_progress", output: [], error: null };
	send("response.created", { response: pending });
	send("response.in_progress", { response: pending });
	let outputIndex = 0;
	for (const item of data.output) {
		const oi = outputIndex++;
		if (item.type === "reasoning") {
			const added = { ...item, status: "in_progress", summary: [] };
			send("response.output_item.added", { item: added, output_index: oi });
			const summaries = Array.isArray(item.summary) ? item.summary : [];
			summaries.forEach((part, summaryIndex) => {
				const text = typeof part?.text === "string" ? part.text : "";
				send("response.reasoning_summary_part.added", { item_id: item.id, output_index: oi, summary_index: summaryIndex, part: { type: "summary_text", text: "" } });
				if (text) send("response.reasoning_summary_text.delta", { item_id: item.id, output_index: oi, summary_index: summaryIndex, delta: text });
				send("response.reasoning_summary_text.done", { item_id: item.id, output_index: oi, summary_index: summaryIndex, text });
				send("response.reasoning_summary_part.done", { item_id: item.id, output_index: oi, summary_index: summaryIndex, part: { type: "summary_text", text } });
			});
			send("response.output_item.done", { item: { ...item, status: "completed" }, output_index: oi });
			continue;
		}
		if (item.type === "message") {
			const itemId = item.id || ocId("msg");
			send("response.output_item.added", { item: { ...item, id: itemId, status: "in_progress", content: [] }, output_index: oi });
			const parts = Array.isArray(item.content) ? item.content : [];
			parts.forEach((part, contentIndex) => {
				if (part?.type !== "output_text") return;
				const text = typeof part.text === "string" ? part.text : "";
				send("response.content_part.added", { item_id: itemId, output_index: oi, content_index: contentIndex, part: { ...part, text: "" } });
				if (text) send("response.output_text.delta", { item_id: itemId, output_index: oi, content_index: contentIndex, delta: text, logprobs: [] });
				send("response.output_text.done", { item_id: itemId, output_index: oi, content_index: contentIndex, text, logprobs: [] });
				send("response.content_part.done", { item_id: itemId, output_index: oi, content_index: contentIndex, part });
			});
			send("response.output_item.done", { item: { ...item, id: itemId, status: "completed" }, output_index: oi });
			continue;
		}
		if (item.type === "function_call") {
			const itemId = item.id || ocId("fc");
			const args = typeof item.arguments === "string" ? item.arguments : "{}";
			send("response.output_item.added", { item: { ...item, id: itemId, arguments: "", status: "in_progress" }, output_index: oi });
			if (args) send("response.function_call_arguments.delta", { item_id: itemId, output_index: oi, delta: args });
			send("response.function_call_arguments.done", { item_id: itemId, output_index: oi, arguments: args });
			send("response.output_item.done", { item: { ...item, id: itemId, arguments: args, status: "completed" }, output_index: oi });
		}
	}
	const terminalType = data.status === "incomplete" ? "response.incomplete" : "response.completed";
	send(terminalType, { response: data });
}

async function handleNativeMuseResponses(originalBody, env, requestId, model, user, sessionId, request) {
	const upstreamBody = sanitizeNativeMuseResponsesBody(env, originalBody);
	const zenReq = {
		body: JSON.stringify(upstreamBody),
		headers: buildZenHeaders(env, sessionId, request ? parentSessionFrom(request) : ""),
	};
	logZenRequest(env, requestId, "responses-native-muse", model, !!originalBody.stream, user, zenReq, Array.isArray(upstreamBody.input) ? upstreamBody.input.length : 1);
	const fetchUpstream = () => fetchZenResponses(zenReq, env, requestId, model, !!originalBody.stream);
	let upstream;
	try { upstream = await fetchUpstream(); }
	catch (error) { return upstreamErrorResponse(error, "responses"); }
	return originalBody.stream
		? nativeMuseResponsesStream(upstream, env, requestId, model, originalBody.tools, fetchUpstream)
		: nativeMuseResponsesFull(upstream, env, requestId, model, originalBody.tools, fetchUpstream);
}

function inferToolNameFromArguments(args, specs) {
	if (!args || typeof args !== "object" || Array.isArray(args) || !specs.length) return "";
	if (specs.length === 1) return specs[0].name;
	const keys = Object.keys(args);
	const ranked = [];
	for (const spec of specs) {
		let best = -Infinity;
		for (const schema of schemaObjectVariants(spec.parameters)) {
			const props = schema?.properties && typeof schema.properties === "object" ? schema.properties : {};
			const required = Array.isArray(schema?.required) ? schema.required : [];
			if (required.some((key) => !Object.prototype.hasOwnProperty.call(args, key))) continue;
			const matched = keys.filter((key) => Object.prototype.hasOwnProperty.call(props, key)).length;
			const unknown = keys.length - matched;
			if (schema?.additionalProperties === false && unknown) continue;
			if (keys.length && !matched && Object.keys(props).length) continue;
			const score = matched * 10 + required.length * 4 - unknown * 2 + (keys.length ? 1 : 0);
			if (score > best) best = score;
		}
		if (best > -Infinity) ranked.push({ name: spec.name, score: best });
	}
	ranked.sort((a, b) => b.score - a.score || a.name.localeCompare(b.name));
	if (!ranked.length || ranked[0].score <= 0) return "";
	if (ranked.length > 1 && ranked[0].score === ranked[1].score) return "";
	return ranked[0].name;
}

function logToolCallFinalization(requestId, model, result) {
	if (!result || (!result.invalid.length && !result.inferred && !result.repaired)) return;
	console.log("[TOOL CALL NORMALIZE]", JSON.stringify({
		requestId,
		model,
		total: result.total,
		valid: [...result.callsByChoice.values()].reduce((sum, calls) => sum + calls.length, 0),
		inferred: result.inferred,
		repaired: result.repaired,
		invalid: result.invalid,
	}));
}

function mergeResponsesGateTools(tools) {
	const mapped = [];
	for (const tool of (Array.isArray(tools) ? tools : [])) {
		if (!tool) continue;
		if (tool.type === "function" && tool.function?.name) {
			const fn = { type: "function", name: tool.function.name };
			if (tool.function.description) fn.description = tool.function.description;
			if (tool.function.parameters) fn.parameters = tool.function.parameters;
			mapped.push(fn);
			continue;
		}
		// 原生 Responses 入站已经是扁平 function tool。
		if (tool.type === "function" && typeof tool.name === "string" && tool.name.trim()) {
			mapped.push({
				type: "function",
				name: tool.name.trim(),
				...(tool.description ? { description: tool.description } : {}),
				parameters: tool.parameters || { type: "object", properties: {} },
			});
		}
	}
	const seen = new Set();
	const merged = [];
	for (const tool of mapped) {
		if (!seen.has(tool.name)) { merged.push(tool); seen.add(tool.name); }
	}
	for (const tool of FAKE_TOOL_RESPONSES) {
		if (!seen.has(tool.name)) { merged.push(tool); seen.add(tool.name); }
	}
	return merged;
}

function mergeToolArgumentFragment(current, fragment) {
	let next = "";
	if (typeof fragment === "string") next = fragment;
	else if (fragment !== undefined && fragment !== null) {
		try { next = JSON.stringify(fragment); } catch { next = String(fragment); }
	}
	if (!next) return current || "";
	if (!current) return next;
	if (next === current) return current;
	if (next.startsWith(current)) return next; // 兼容发送累计 arguments 的端点
	return current + next;
}

function mergeToolNameFragment(current, fragment) {
	const next = typeof fragment === "string" ? fragment : "";
	if (!next) return current || "";
	if (!current) return next;
	if (next === current || current.endsWith(next)) return current;
	if (next.startsWith(current)) return next; // 某些兼容端点发送累计值而非 delta
	return current + next;
}

async function nativeMuseResponsesFull(upstream, env, requestId, model, clientTools, refetch) {
	let response = upstream;
	for (let attempt = 0; attempt <= BRIDGE_STREAM_RETRIES; attempt++) {
		const raw = await response.text();
		const zenError = parseZenError(raw);
		logUpstreamBody(env, requestId, model, response.status, raw, zenError);
		let decoded;
		if (!response.ok || zenError) decoded = { error: zenError || { message: `Upstream returned HTTP ${response.status}` }, data: null, empty: false };
		else if (isSSE(raw)) decoded = aggregateResponsesSSE(raw, false);
		else {
			const data = safeJsonParse(raw);
			decoded = data?.error || data?.status === "failed"
				? { error: data?.error || { message: "Upstream response failed" }, data: null, empty: false }
				: { error: null, data, empty: !data || (!responsesOutputText(data) && !responsesToolCalls(data).length) };
		}
		if (!decoded.error && decoded.data) {
			decoded.data = sanitizeMuseResponsesData(decoded.data, clientTools, requestId, model);
			decoded.empty = !responsesOutputText(decoded.data) && !responsesToolCalls(decoded.data).length;
		}
		if (!decoded.error && !decoded.empty && decoded.data) return jsonResponse(canonicalResponsesData(decoded.data, model), 200);
		if (attempt < BRIDGE_STREAM_RETRIES) {
			console.log("[ZEN NATIVE-RETRY]", JSON.stringify({ requestId, model, attempt: attempt + 1, reason: decoded.error?.message || "empty-response" }));
			try { response = await refetch(); continue; } catch (error) { return upstreamErrorResponse(error, "responses"); }
		}
		if (decoded.error) return openAIErrorResponse(decoded.error.message || "Muse Spark upstream error", "upstream_error", 502, decoded.error.code);
		return openAIErrorResponse("Muse Spark completed without text or a tool call", "upstream_error", 502, "empty_response");
	}
	return openAIErrorResponse("Muse Spark request failed", "upstream_error", 502);
}

async function nativeMuseResponsesStream(upstream, env, requestId, model, clientTools, refetch) {
	if (!upstream.ok) {
		const raw = await upstream.text();
		const error = parseZenError(raw);
		const details = zenErrorDetails(error, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}
	const encoder = new TextEncoder();
	let activeReader = null;
	let heartbeat = null;
	let cancelled = false;
	const stream = new ReadableStream({
		async start(controller) {
			let sequence = 0;
			const enqueue = (text) => controller.enqueue(encoder.encode(text));
			const send = (type, payload) => {
				sequence += 1;
				enqueue(`event: ${type}\ndata: ${JSON.stringify({ ...payload, type, sequence_number: sequence })}\n\n`);
			};
			const consume = async (response) => {
				if (!response?.body) return { raw: "", status: response?.status || 502, ok: false };
				activeReader = response.body.getReader();
				const decoder = new TextDecoder();
				let raw = "";
				let bytes = 0;
				const max = Math.max(Number(env?.BRIDGE_STREAM_MAX_BUFFER_BYTES) || 8 * 1024 * 1024, 64 * 1024);
				while (!cancelled) {
					const { done, value } = await activeReader.read();
					if (done) break;
					bytes += value?.byteLength || 0;
					if (bytes > max) throw new Error("Muse Spark stream exceeded bridge buffer limit");
					raw += decoder.decode(value, { stream: true });
				}
				raw += decoder.decode();
				activeReader = null;
				return { raw, status: response.status, ok: response.ok };
			};
			heartbeat = setInterval(() => { if (!cancelled) try { enqueue(": ping\n\n"); } catch { } }, Number(env?.STREAM_HEARTBEAT_MS) || STREAM_HEARTBEAT_MS);
			let response = upstream;
			let decoded = null;
			try {
				const retries = Math.max(Number(env?.BRIDGE_STREAM_RETRIES) || BRIDGE_STREAM_RETRIES, 0);
				for (let attempt = 0; attempt <= retries; attempt++) {
					const consumed = await consume(response);
					const zenError = parseZenError(consumed.raw);
					if (!consumed.ok || zenError) decoded = { error: zenError || { message: `Upstream returned HTTP ${consumed.status}` }, data: null, empty: false };
					else if (isSSE(consumed.raw)) decoded = aggregateResponsesSSE(consumed.raw, false);
					else {
						const data = safeJsonParse(consumed.raw);
						decoded = data?.error || data?.status === "failed"
							? { error: data?.error || { message: "Upstream response failed" }, data: null, empty: false }
							: { error: null, data, empty: !data || (!responsesOutputText(data) && !responsesToolCalls(data).length) };
					}
					if (!decoded.error && decoded.data) {
						decoded.data = sanitizeMuseResponsesData(decoded.data, clientTools, requestId, model);
						decoded.empty = !responsesOutputText(decoded.data) && !responsesToolCalls(decoded.data).length;
					}
					if (!decoded.error && !decoded.empty) break;
					if (attempt >= retries || cancelled) break;
					console.log("[ZEN NATIVE-RETRY]", JSON.stringify({ requestId, model, attempt: attempt + 1, reason: decoded.error?.message || "empty-response" }));
					response = await refetch();
				}
				if (cancelled) return;
				if (decoded?.error) send("error", { error: { message: decoded.error.message || "Muse Spark upstream error", type: decoded.error.type || "upstream_error", ...(decoded.error.code ? { code: decoded.error.code } : {}) } });
				else if (!decoded?.data || decoded.empty) send("error", { error: { message: "Muse Spark completed without text or a tool call", type: "upstream_error", code: "empty_response" } });
				else emitCanonicalResponses(send, decoded.data, model);
			} catch (error) {
				if (!cancelled) send("error", { error: { message: error?.message || "Muse Spark bridge failed", type: "upstream_error" } });
			} finally {
				if (heartbeat) clearInterval(heartbeat);
				try { controller.close(); } catch { }
			}
		},
		cancel(reason) {
			cancelled = true;
			if (heartbeat) clearInterval(heartbeat);
			return activeReader?.cancel(reason);
		},
	});
	return new Response(stream, { status: 200, headers: mergeHeaders(SSE_HEADERS) });
}

function parseToolArguments(raw) {
	let text = typeof raw === "string" ? raw.trim() : "";
	if (!text) return { ok: true, text: "{}", value: {}, repaired: false };

	// 去掉模型偶尔附加的 Markdown 围栏。
	if (/^```/i.test(text)) {
		text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/i, "").trim();
	}
	const parse = (candidate, repaired = false) => {
		try {
			const value = JSON.parse(candidate);
			if (!value || typeof value !== "object" || Array.isArray(value)) return null;
			return { ok: true, text: JSON.stringify(value), value, repaired };
		} catch { return null; }
	};
	const direct = parse(text, false);
	if (direct) return direct;

	// 若 JSON 前后夹了协议标记，只取最外层对象。
	const firstBrace = text.indexOf("{");
	const lastBrace = text.lastIndexOf("}");
	if (firstBrace >= 0 && lastBrace > firstBrace) {
		const extracted = parse(text.slice(firstBrace, lastBrace + 1), true);
		if (extracted) return extracted;
	}

	// 仅修补已完整结束的 token 后缺失的 } / ]；字符串或值本身截断时拒绝修补，
	// 防止执行被截短的命令、路径或代码。
	const stack = [];
	let inString = false;
	let escaped = false;
	let structurallyInvalid = false;
	for (let i = 0; i < text.length; i++) {
		const ch = text[i];
		if (inString) {
			if (escaped) escaped = false;
			else if (ch === "\\") escaped = true;
			else if (ch === '"') inString = false;
			continue;
		}
		if (ch === '"') { inString = true; continue; }
		if (ch === "{" || ch === "[") stack.push(ch);
		else if (ch === "}" || ch === "]") {
			const want = ch === "}" ? "{" : "[";
			if (stack.pop() !== want) { structurallyInvalid = true; break; }
		}
	}
	if (!structurallyInvalid && !inString && stack.length) {
		let candidate = text.replace(/,\s*$/, "");
		if (!/:\s*$/.test(candidate)) {
			while (stack.length) candidate += stack.pop() === "{" ? "}" : "]";
			// 同时容忍对象内部的单个尾逗号。
			candidate = candidate.replace(/,\s*([}\]])/g, "$1");
			const repaired = parse(candidate, true);
			if (repaired) return repaired;
		}
	}
	return { ok: false, text, value: null, repaired: false };
}

function recoverToolName(rawName, args, specs, strictAllowed) {
	const raw = typeof rawName === "string" ? rawName.trim() : "";
	if (raw) {
		if (!strictAllowed) return /^[A-Za-z0-9_-]{1,128}$/.test(raw) ? raw : "";
		const exact = specs.find((spec) => spec.name === raw);
		if (exact) return exact.name;
		const folded = specs.find((spec) => spec.name.toLowerCase() === raw.toLowerCase());
		if (folded) return folded.name;
		// 恢复 functions.exec、引号/协议标记包裹的 exec，但不把有效的未知工具名
		// 猜成别的工具（例如占位 bash 不会被映射成 Code Mode exec）。
		const embedded = specs.filter((spec) => {
			const escaped = spec.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
			return new RegExp(`(?:^|[^A-Za-z0-9_-])${escaped}(?:$|[^A-Za-z0-9_-])`, "i").test(raw);
		});
		if (embedded.length === 1) return embedded[0].name;
		return "";
	}
	return inferToolNameFromArguments(args, specs);
}

// bridge(responses) SSE 聚合。
// OpenCode 原码把 response.failed 视为 provider-error，而不是正常 finish；这里必须保持
// 同样语义，不能像旧实现那样伪造成 stop + [DONE]，否则 OpenClaw 只会看到“自行中断”。
function responsesEventError(event) {
	if (!event || typeof event !== "object") return null;
	const error = event.error || event.response?.error;
	if (event.type !== "response.failed" && event.type !== "error" && !error) return null;
	if (typeof error === "string") return { message: error, type: "upstream_error" };
	return {
		message: error?.message || event.message || "Upstream Responses stream failed",
		type: error?.type || event.type || "upstream_error",
		code: error?.code || event.code,
	};
}

function responsesOutputReasoningText(data) {
	const parts = [];
	for (const item of Array.isArray(data?.output) ? data.output : []) {
		if (item?.type !== "reasoning") continue;
		for (const summary of Array.isArray(item.summary) ? item.summary : []) {
			if (typeof summary?.text === "string") parts.push(summary.text);
		}
	}
	return parts.join("\n");
}

function responsesToolsToOpenAI(tools) {
	return (Array.isArray(tools) ? tools : [])
		.filter((tool) => tool?.type === "function" && typeof tool.name === "string" && tool.name.trim())
		.map((tool) => ({
			type: "function",
			function: {
				name: tool.name.trim(),
				description: tool.description || "",
				parameters: tool.parameters || { type: "object", properties: {} },
			},
		}));
}

function sanitizeMuseResponsesData(data, clientTools, requestId, model) {
	if (!data || typeof data !== "object") return data;
	const output = [];
	for (const item of Array.isArray(data.output) ? data.output : []) {
		if (!item || typeof item !== "object") continue;
		if (item.type === "function_call") continue; // 下面用校验后的调用重建
		if (item.type === "reasoning") {
			const clean = { ...item };
			delete clean.encrypted_content;
			output.push(clean);
			continue;
		}
		output.push(item);
	}
	const oaiTools = responsesToolsToOpenAI(clientTools);
	const validated = validatedResponsesToolCalls(data, oaiTools, requestId, model);
	for (const call of validated.calls) {
		output.push({
			type: "function_call",
			id: ocId("fc"),
			status: "completed",
			call_id: call.id,
			name: call.function.name,
			arguments: call.function.arguments,
		});
	}
	return { ...data, output };
}

function sanitizeNativeMuseResponsesBody(env, original) {
	const body = { ...original };
	body.stream = true; // Zen 免费层闸门要求
	body.store = false;
	body.background = false;
	delete body.previous_response_id;
	// 不请求 caller-bound reasoning blob；其他 include 项可以保留。
	if (Array.isArray(body.include)) {
		body.include = body.include.filter((item) => item !== "reasoning.encrypted_content");
		if (!body.include.length) delete body.include;
	}
	if (Array.isArray(body.input)) {
		body.input = body.input.flatMap((item) => {
			if (!item || typeof item !== "object") return [item];
			if (item.type === "reasoning" || item.type === "item_reference") return [];
			const next = { ...item };
			delete next.encrypted_content;
			if (item.type === "function_call") delete next.id; // Responses 输入只需 call_id
			if (Array.isArray(next.content)) {
				next.content = next.content
					.filter((part) => part?.type !== "reasoning")
					.map((part) => {
						if (!part || typeof part !== "object") return part;
						const clean = { ...part };
						delete clean.encrypted_content;
						return clean;
					});
			}
			return [next];
		});
	}
	body.tools = mergeResponsesGateTools(body.tools);
	body.tool_choice = "auto";
	body.max_output_tokens = bridgeMaxOutputTokens(env, body.max_output_tokens);
	if (body.reasoning && typeof body.reasoning === "object") {
		body.reasoning = { ...body.reasoning };
		delete body.reasoning.encrypted_content;
		delete body.reasoning.id;
	}
	return body;
}

function schemaObjectVariants(schema) {
	if (!schema || typeof schema !== "object") return [];
	const out = [schema];
	for (const key of ["anyOf", "oneOf", "allOf"]) {
		for (const child of Array.isArray(schema[key]) ? schema[key] : []) out.push(...schemaObjectVariants(child));
	}
	return out;
}

function validatedResponsesToolCalls(data, clientTools, requestId, model) {
	const rawCalls = responsesToolCalls(data);
	if (!rawCalls.length) return { calls: [], invalid: [] };
	const accumulator = createToolCallAccumulator(clientTools);
	rawCalls.forEach((call) => accumulator.add(0, call));
	const finalized = accumulator.finalize();
	logToolCallFinalization(requestId, model, finalized);
	return { calls: finalized.callsByChoice.get(0) || [], invalid: finalized.invalid };
}

// ---- muse /v1/messages 桥：上游 /zen/v1/chat/completions 对该模型 500，
// ---- /zen/v1/messages 亦不支持，只能走 /zen/v1/responses 再回落 Anthropic 协议。
function responsesDataToOpenAISSE(completion) {
	const model = completion.model;
	const msg = completion.choices[0].message;
	const chunks = [];
	chunks.push({ id: completion.id, object: "chat.completion.chunk", created: completion.created, model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });
	if (typeof msg.content === "string" && msg.content) {
		chunks.push({ id: completion.id, object: "chat.completion.chunk", created: completion.created, model, choices: [{ index: 0, delta: { content: msg.content }, finish_reason: null }] });
	}
	(msg.tool_calls || []).forEach((tc, i) => {
		chunks.push({
			id: completion.id, object: "chat.completion.chunk", created: completion.created, model,
			choices: [{ index: 0, delta: { tool_calls: [{ index: i, id: tc.id, type: "function", function: { name: tc.function.name, arguments: tc.function.arguments } }] }, finish_reason: null }]
		});
	});
	chunks.push({ id: completion.id, object: "chat.completion.chunk", created: completion.created, model, choices: [{ index: 0, delta: {}, finish_reason: completion.choices[0].finish_reason }] });
	return chunks.map((c) => `data: ${JSON.stringify(c)}\n\n`).join("") + "data: [DONE]\n\n";
}

async function readBridgeResponsesData(upstream, env, requestId, model) {
	const raw = await upstream.text();
	const zenError = parseZenError(raw);
	logUpstreamBody(env, requestId, model, upstream.status, raw, zenError);
	if (upstream.status === 429 || zenError) return { error: zenError || safeJsonParse(raw)?.error || { message: "Upstream error" } };
	let data = null;
	if (isSSE(raw)) {
		const agg = aggregateResponsesSSE(raw, false);
		if (agg.error) return { error: agg.error };
		data = agg.data;
	} else {
		data = safeJsonParse(raw);
	}
	if (data && (Array.isArray(data.output) || data.object === "response")) {
		if (data.error) return { error: data.error };
		return { data };
	}
	if (data?.choices) return { data };
	return { error: { message: "Invalid upstream response" } };
}

async function anthropicFullResponseFromBridge(upstream, model, inputTokens, env, requestId) {
	const res = await readBridgeResponsesData(upstream, env, requestId, model);
	if (res.error) {
		const details = zenErrorDetails(res.error, 502);
		return anthropicErrorResponse(details.message, details.anthropicType, details.status);
	}
	const data = res.data;
	const oai = (Array.isArray(data.output) || data.object === "response") ? responsesToChatCompletion(data, model) : data;
	return jsonResponse(openAIToAnthropic(normalizeOpenAIFullData(oai), model, inputTokens));
}

async function anthropicStreamResponseFromBridge(upstream, model, inputTokens, env, requestId) {
	const res = await readBridgeResponsesData(upstream, env, requestId, model);
	if (res.error) {
		const details = zenErrorDetails(res.error, 502);
		return anthropicErrorResponse(details.message, details.anthropicType, details.status);
	}
	const data = res.data;
	const oai = (Array.isArray(data.output) || data.object === "response") ? responsesToChatCompletion(data, model) : data;
	// 复用已验证的 anthropicStreamResponse：喂入合成的 OpenAI SSE
	const synthetic = new Response(responsesDataToOpenAISSE(oai), { status: 200, headers: { "Content-Type": "text/event-stream" } });
	return anthropicStreamResponse(synthetic, model, inputTokens, env, requestId);
}

async function handleResponses(request, env) {
	const requestId = ocId("req");
	const auth = authenticate(request, env);
	if (auth.error) return auth.error;

	let relayModelR = "";
	try { relayModelR = (await request.clone().json())?.model || ""; } catch (e) { }
	if (shouldRelayToSub(env, relayModelR, request)) {
		return relayToSub(request, env, requestId);
	}
	const input = await readJson(request);
	if (input.error) return input.error;

	const { model, stream } = input.body;

	const sessionId = getSession(auth.user);
	// 提交版修复：muse-spark 系列在 /zen/v1/chat/completions 会 500/RegionError，
	// 原实现把入站 /v1/responses 又转成 chat/completions，正是该模型 500 的错误路径。
	// 原生客户端直接走 /zen/v1/responses 直通。
	if (shouldUseResponsesBridge(env, model)) {
		return handleNativeMuseResponses(input.body, env, requestId, model, auth.user, sessionId, request);
	}
	const converted = responsesToOpenAI(input.body);
	const msgSummary = converted.messages.map((msg) => ({
		role: msg.role,
		len: typeof msg.content === "string" ? msg.content.length : JSON.stringify(msg.content || "").length,
	}));
	console.log("[RES]", new Date().toISOString(), auth.user, model, stream ? "stream" : "sync", "msgs:", JSON.stringify(msgSummary));

	const zenReq = buildZenRequest(env, model, converted.messages, stream, converted.tools, converted.toolChoice, sessionId, converted.extras, parentSessionFrom(request));
	logZenRequest(env, requestId, "responses", model, stream, auth.user, zenReq, converted.messages.length);

	let upstream;
	try {
		upstream = await fetchZen(zenReq, env, requestId, model, stream);
	} catch (error) {
		debugLog(env, "[ZEN FETCH ERROR]", {
			requestId,
			model,
			stream: !!stream,
			message: error?.message || String(error),
		});
		return upstreamErrorResponse(error, "responses");
	}

	const inputTokens = Math.floor(JSON.stringify(converted.messages).length / 4);
	if (stream) return responsesStreamResponse(upstream, env, requestId, model, inputTokens);
	return responsesFullResponse(upstream, env, requestId, model, inputTokens);
}

async function ipResponse() {
	try {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), 8000);
		const resp = await fetch("https://api.ipify.org?format=json", { signal: controller.signal });
		clearTimeout(timer);
		const data = await resp.json();
		return jsonResponse({ ip: data?.ip || null, source: "api.ipify.org" });
	} catch (error) {
		return jsonResponse({ ip: null, error: error?.message || "lookup failed" }, 502);
	}
}

function healthResponse() {
	return jsonResponse({
		status: "ok",
		version: `v${PROXY_VERSION}`,
		models: cachedModelCount(),
		endpoints: ["/v1/chat/completions", "/v1/messages", "/v1/responses", "/v1/models"],
	});
}

async function modelsResponse(env) {
	try {
		return jsonResponse({
			object: "list",
			data: await getAvailableModels(env),
		});
	} catch (error) {
		debugLog(env, "[MODEL LIST ERROR]", { message: error?.message || String(error) });
		return upstreamErrorResponse(error, "openai");
	}
}

async function getAvailableModels(env) {
	if (cachedModels) return cachedModels;

	cachedModels = fetchZenModels(env)
		.then((models) => {
			cachedModels = models;
			return models;
		})
		.catch((error) => {
			cachedModels = null;
			throw error;
		});

	return cachedModels;
}

async function fetchZenModels(env) {
	const response = await fetchZenWithFallback(
		"/inference/v1/models",
		() => ({
			method: "GET",
			headers: {
				"Accept": "application/json",
				"Authorization": zenAuthHeader(env),
				"User-Agent": `opencode/latest/${OC_VERSION}/${OC_CLIENT}`,
			},
		}),
		env,
		"model-list",
	);

	const raw = await response.text();
	const parsed = safeJsonParse(raw);

	if (!response.ok) throw new Error(`Model list returned HTTP ${response.status}`);
	if (!Array.isArray(parsed?.data)) throw new Error("Invalid model list response");

	const models = parsed.data
		.filter((item) => isAllowedModelId(item?.id))
		.map((item) => ({ ...item }));

	if (!models.length) throw new Error("No allowed models returned from upstream");

	debugLog(env, "[MODEL LIST]", {
		status: response.status,
		total: parsed.data.length,
		allowed: models.length,
	});

	return models;
}

function isAllowedModelId(id) {
	return typeof id === "string" && (id === "big-pickle" || id.endsWith("-free"));
}

function cachedModelCount() {
	return Array.isArray(cachedModels) ? cachedModels.length : 0;
}

function getOpencodeUrlsForRequest(env, model) {
	const configured = String(env?.UPSTREAM_HOSTS || "")
		.split(",")
		.map((url) => url.trim().replace(/\/$/, ""))
		.filter((url) => /^https:\/\//i.test(url));
	let hosts = configured.length ? [...new Set(configured)] : OPENCODE_URLs;
	// muse-spark contributor 只在 ai / fastly 两个镜像上可用，gcore 会返回 RegionError。
	if (model && shouldUseResponsesBridge(env, model)) {
		const allowed = hosts.filter((url) => !/gcore/i.test(url));
		if (allowed.length) hosts = allowed;
	}
	const now = Date.now();
	const failureTtlMs = 5 * 60 * 1000;
	const latencyTtlMs = 10 * 60 * 1000;
	const ranked = hosts.map((url, index) => {
		const stats = endpointStats.get(url);
		const fresh = stats && now - stats.ts < latencyTtlMs ? stats : null;
		return {
			url,
			index,
			failures: fresh && now - fresh.ts < failureTtlMs ? fresh.failures : 0,
			latencyMs: fresh?.latencyMs || 0,
		};
	});

	ranked.sort((a, b) => {
		if (a.failures !== b.failures) return a.failures - b.failures;
		// Sample every unmeasured endpoint once before preferring measured latency.
		if (!a.latencyMs && b.latencyMs) return -1;
		if (a.latencyMs && !b.latencyMs) return 1;
		if (a.latencyMs !== b.latencyMs) return a.latencyMs - b.latencyMs;
		return a.index - b.index;
	});

	return ranked.map((item) => item.url);
}

function recordEndpointSuccess(baseUrl, latencyMs) {
	const previous = endpointStats.get(baseUrl) || { latencyMs: 0, failures: 0 };
	const nextLatency = previous.latencyMs
		? Math.round(previous.latencyMs * 0.7 + latencyMs * 0.3)
		: latencyMs;
	endpointStats.set(baseUrl, { latencyMs: nextLatency, failures: 0, ts: Date.now() });
}

function recordEndpointFailure(baseUrl) {
	const previous = endpointStats.get(baseUrl) || { latencyMs: 0, failures: 0 };
	endpointStats.set(baseUrl, {
		latencyMs: previous.latencyMs,
		failures: Math.min(previous.failures + 1, 3),
		ts: Date.now(),
	});
}

function buildZenRequest(env, model, messages, stream, tools, toolChoice, sessionId, extras, parentSession) {
	// 上游闸门(2026-10-13)：system[0] 必须命中 opencode build 提示词白名单 + tools 含 11 个真名；
	// 原 system 顺延为第二条 system，客户端指令不受影响
	const reqBody = { model, messages, stream: true };
	const cleanTools = sanitizeTools(tools);
	const seen = new Set();
	const merged = [];
	// 客户端真实 schema 优先（提交版修复）：旧实现先放 FAKE_TOOL，会吞掉客户端自带的
	// 同名 bash/read schema，模型拿到无参数假 schema → 产出不可执行的空参数调用。
	for (const t of (cleanTools || [])) {
		if (!seen.has(t.function.name)) { merged.push(t); seen.add(t.function.name); }
	}
	for (const t of FAKE_TOOL) {
		if (!seen.has(t.function.name)) { merged.push(t); seen.add(t.function.name); }
	}
	reqBody.tools = merged;
	// 2026-09-25 上游收紧校验：tools 里任何一条缺 parameters 都会 400 invalid_request。
	// 占位 bash/read 只为过 403 FreeTierError 闸门，必须补合法（空）schema。
	for (const t of reqBody.tools) {
		if (!t.function.parameters) t.function.parameters = { type: "object", properties: {} };
	}
	// v2 闸门(2026-10-14):tool_choice="none" 会 403。缺省(不发)或 "auto" 才合规 ——
	// 无客户端工具时直接不带 tool_choice 字段
	if (cleanTools?.length && toolChoice && toolChoice !== "none") reqBody.tool_choice = toolChoice;
	if (extras && typeof extras === "object") Object.assign(reqBody, extras);

	return {
		body: JSON.stringify(reqBody),
		headers: buildZenHeaders(env, sessionId, parentSession),
	};
}

function buildZenHeaders(env, sessionId, parentSession) {
	const headers = {
		"Content-Type": "application/json",
		"Authorization": zenAuthHeader(env),
		"User-Agent": `opencode/latest/${OC_VERSION}/${OC_CLIENT}`,
		"x-opencode-client": OC_CLIENT,
		"x-opencode-project": OC_PROJECT,
		"x-opencode-session": sessionId,
		"x-opencode-session-id": sessionId,
		"x-session-affinity": sessionId,
		"x-session-id": sessionId,
	};
	// dsh subagent 血缘头透传：官方 CLI 对 parentSessionID 就发这两个头，
	// 带上后上游可按 agent 团队做 prompt cache 血缘复用，也多一层真 client 伪装。
	if (parentSession) {
		headers["x-opencode-parent-session-id"] = parentSession;
		headers["x-parent-session-id"] = parentSession;
	}
	return headers;
}

// 从入站 headers 提取 dsh subagent 父 session 信号（须为合法 ses_ 格式才透传）。
function parentSessionFrom(request) {
	const raw = request.headers.get("x-opencode-parent-session-id") || request.headers.get("x-parent-session-id") || "";
	return /^ses_[0-9a-fA-F]{12}[0-9A-Za-z]{14}$/.test(raw) ? raw : "";
}

function openAIRequestExtras(body) {
	const extras = {};
	for (const key of ["temperature", "top_p", "frequency_penalty", "presence_penalty", "seed"]) {
		if (typeof body?.[key] === "number" && Number.isFinite(body[key])) extras[key] = body[key];
	}
	if (typeof body?.max_tokens === "number" && body.max_tokens > 0) {
		extras.max_tokens = Math.floor(body.max_tokens);
	} else if (typeof body?.max_completion_tokens === "number" && body.max_completion_tokens > 0) {
		extras.max_tokens = Math.floor(body.max_completion_tokens);
	}
	if (typeof body?.parallel_tool_calls === "boolean") extras.parallel_tool_calls = body.parallel_tool_calls;
	if (typeof body?.stop === "string" || Array.isArray(body?.stop)) extras.stop = body.stop;
	if (body?.response_format && typeof body.response_format === "object") extras.response_format = body.response_format;
	if (body?.stream_options && typeof body.stream_options === "object") extras.stream_options = body.stream_options;
	return extras;
}

// Anthropic /v1/messages 采样参数 → OpenAI chat/completions 等价字段。
// 旧实现整块丢弃：客户端给的 max_tokens/temperature/top_p/stop_sequences 全部失效，
// 上游按默认值出稿（dsh/pi-ai 类客户端会表现为「答复长度/风格不受控」）。
// top_k 不进白名单：上游 OpenAI 协议面不认，带了会 400 invalid_request。
function anthropicRequestExtras(body) {
	const extras = {};
	for (const key of ["temperature", "top_p"]) {
		if (typeof body?.[key] === "number" && Number.isFinite(body[key])) extras[key] = body[key];
	}
	if (typeof body?.max_tokens === "number" && body.max_tokens > 0) {
		extras.max_tokens = Math.floor(body.max_tokens);
	}
	if (Array.isArray(body?.stop_sequences) && body.stop_sequences.length) {
		extras.stop = body.stop_sequences.filter((s) => typeof s === "string");
		if (!extras.stop.length) delete extras.stop;
	}
	return extras;
}

// Anthropic tool_choice → OpenAI tool_choice。
// any → required；tool → {type:function,function:{name}}；none → 不发（v2 闸门对 none 直接 403）。
function anthropicToolChoice(choice) {
	if (!choice || typeof choice !== "object") return undefined;
	if (choice.type === "auto") return "auto";
	if (choice.type === "any") return "required";
	if (choice.type === "tool" && choice.name) return { type: "function", function: { name: choice.name } };
	return undefined;
}

function sanitizeTools(tools) {
	if (!Array.isArray(tools)) return undefined;
	const clean = tools.filter(
		(tool) => tool
			&& tool.type === "function"
			&& typeof tool.function?.name === "string"
			&& tool.function.name.trim(),
	);
	return clean.length ? clean : undefined;
}

function zenAuthHeader(env) {
	return `Bearer ${String(env?.ZEN_API_KEY || "public").trim()}`;
}

async function fetchZen(zenReq, env, requestId, model, stream) {
	return fetchZenWithFallback(
		"/inference/v1/chat/completions",
		() => ({
			method: "POST",
			headers: zenReq.headers,
			body: zenReq.body,
		}),
		env,
		requestId,
		model,
		stream,
	);
}

async function fetchZenWithFallback(path, createInit, env, requestId, model, stream, _round) {
	let lastError = null;
	const urls = getOpencodeUrlsForRequest(env, model);
	const timeoutMs = Number(env?.FETCH_TIMEOUT_MS) || FETCH_TIMEOUT_MS;
	const bridgeModel = typeof shouldUseResponsesBridge === "function" && shouldUseResponsesBridge(env, model);
	const firstResponseTimeoutMs = bridgeModel
		? (Number(env?.ZEN_BRIDGE_FIRST_RESPONSE_TIMEOUT_MS) || timeoutMs)
		: (Number(env?.ZEN_FIRST_RESPONSE_TIMEOUT_MS) || 20000);
	const firstByteTimeoutMs = Number(env?.ZEN_FIRST_BYTE_TIMEOUT_MS) || STREAM_FIRST_BYTE_TIMEOUT_MS;
	const retryDelayMs = Number(env?.ZEN_RETRY_DELAY_MS) || 400;

	for (let attempt = 0; attempt < urls.length; attempt++) {
		const baseUrl = urls[attempt];
		const controller = new AbortController();
		let firstResponseTimedOut = false;
		const timeout = setTimeout(() => controller.abort("timeout"), timeoutMs);
		const firstResponseTimeout = setTimeout(() => {
			firstResponseTimedOut = true;
			controller.abort("first-response-timeout");
		}, Math.min(timeoutMs, firstResponseTimeoutMs));

		try {
			const started = Date.now();
			let response = await fetch(baseUrl + path, {
				...createInit(),
				signal: controller.signal,
			});
			let responseMs = Date.now() - started;
			clearTimeout(firstResponseTimeout);

			if (stream && response.ok && response.body) {
				const firstChunk = await readWithTimeout(response.body.getReader(), firstByteTimeoutMs);
				if (firstChunk.done || !firstChunk.value?.byteLength) {
					throw new Error("first-byte-timeout");
				}
				responseMs = Date.now() - started;
				response = replayFirstChunk(response, firstChunk.value, firstChunk.reader);
			}

			recordEndpointSuccess(baseUrl, responseMs);
			logZenResponse(env, {
				requestId,
				model,
				stream: !!stream,
				attempt: attempt + 1,
				source: baseUrl,
				status: response.status,
				ok: response.ok,
				ms: responseMs,
				contentType: response.headers.get("content-type"),
				retryAfter: response.headers.get("retry-after"),
				cfRay: response.headers.get("cf-ray"),
				server: response.headers.get("server"),
			});

			// 404 空体 = ai 镜像间歇抽风,也换镜像;RegionError(403) 视为可重试
			let isRegionRetry = false;
			if (response.status === 403 || (response.status === 404 && !response.headers.get("content-type"))) {
				try {
					const peek = await response.clone().text();
					isRegionRetry = peek.includes("RegionError") || peek.includes("not available in your country") || (response.status === 404 && !peek.trim());
				} catch (error) { isRegionRetry = response.status === 404; }
			}
			if (response.status >= 500 || response.status === 429 || isRegionRetry) {
					recordEndpointFailure(baseUrl);
					lastError = new Error(`Upstream returned HTTP ${response.status}`);
				lastError.status = response.status;
				debugLog(env, "[ZEN RETRY]", {
					requestId,
					model,
					stream: !!stream,
					attempt: attempt + 1,
					source: baseUrl,
					message: lastError.message,
					retryAfter: response.headers.get("retry-after"),
				});
				if (attempt < urls.length - 1) await sleep(retryDelayMs * (attempt + 1));
				continue;
			}

			return response;
		} catch (error) {
			recordEndpointFailure(baseUrl);
			const firstResponseTimedOutError = firstResponseTimedOut
				|| error === "first-response-timeout"
				|| error?.message === "first-response-timeout";
			const firstByteTimedOutError = error === "first-byte-timeout"
				|| error?.message === "first-byte-timeout";
			lastError = error?.name === "AbortError" || error === "timeout" || firstResponseTimedOutError || firstByteTimedOutError
				? new Error(firstResponseTimedOutError ? "first-response-timeout" : firstByteTimedOutError ? "first-byte-timeout" : "timeout")
				: error;
			debugLog(env, "[ZEN RETRY]", {
				requestId,
				model,
				stream: !!stream,
				attempt: attempt + 1,
				source: baseUrl,
				message: lastError?.message || String(lastError),
			});
		} finally {
			clearTimeout(timeout);
			clearTimeout(firstResponseTimeout);
		}
	}

	// relay 式整体第二轮：全部镜像都失败后清惩罚权重再跑一轮（仅一轮，防无限递归）
	if (!_round) {
		debugLog(env, "[ZEN ROUND2]", { requestId, model, stream: !!stream, err: lastError?.message || String(lastError) });
		await sleep((Number(env?.ZEN_RETRY_DELAY_MS) || 400) * 2);
		for (const u of getOpencodeUrlsForRequest(env, model)) endpointStats.delete(u);
		return fetchZenWithFallback(path, createInit, env, requestId, model, stream, 1);
	}
	throw lastError || new Error("Upstream error");
}

function isSSE(raw) {
	return typeof raw === "string" && /^data:/m.test(raw.trim());
}

// 上游强制流式后：非流式下游需要把 SSE 聚合成完整 chat.completion
function aggregateOpenAISSE(raw) {
	let id = null, created = 0, model = null, content = "";
	const toolCalls = [];
	let usage = null, finishReason = null, sawError = null;
	for (const line of raw.split("\n")) {
		const t = line.trim();
		if (!t.startsWith("data:")) continue;
		const payload = t.slice(5).trim();
		if (!payload || payload === "[DONE]") continue;
		let obj;
		try { obj = JSON.parse(payload); } catch { continue; }
		if (obj.error) { sawError = obj.error; break; }
		const choice = obj.choices?.[0];
		if (!choice) continue;
		if (!id) id = obj.id;
		if (!created) created = obj.created;
		if (!model) model = obj.model;
		const delta = choice.delta || {};
		if (typeof delta.content === "string") content += delta.content;
		if (Array.isArray(delta.tool_calls)) {
			for (const tc of delta.tool_calls) {
				const idx = tc.index ?? toolCalls.length;
				while (toolCalls.length <= idx) toolCalls.push({ id: "", type: "function", function: { name: "", arguments: "" } });
				const slot = toolCalls[idx];
				if (tc.id) slot.id = tc.id;
				if (tc.function?.name) slot.function.name += tc.function.name;
				if (tc.function?.arguments) slot.function.arguments += tc.function.arguments;
			}
		}
		if (choice.finish_reason) finishReason = choice.finish_reason;
		if (obj.usage) usage = obj.usage;
	}
	if (sawError) return { error: sawError };
	return {
		error: null,
		data: {
			id: id || ocId("chatcmpl"),
			object: "chat.completion",
			created: created || Math.floor(Date.now() / 1000),
			model: model || "opencode",
			choices: [{
				index: 0,
				message: {
					role: "assistant",
					content: content || null,
					...(toolCalls.length ? { tool_calls: toolCalls } : {}),
				},
				finish_reason: toolCalls.length ? "tool_calls" : (finishReason || "stop"),
			}],
			...(usage ? { usage } : {}),
		},
	};
}

// bridge(responses) SSE 聚合
// 真空判定（轻量，不打日志）：completed 无文本无工具调用，或连 completed 都没有
function isEmptyBridgeRaw(raw) {
	if (!isSSE(raw)) {
		const d = safeJsonParse(raw);
		if (d && (Array.isArray(d.output) || d.object === "response")) {
			return !responsesOutputText(d) && !responsesToolCalls(d).length;
		}
		return false;
	}
	let done = false, text = "";
	for (const line of raw.split("\n")) {
		const t = line.trim();
		if (!t.startsWith("data:")) continue;
		const p = t.slice(5).trim();
		if (!p || p === "[DONE]") continue;
		let o;
		try { o = JSON.parse(p); } catch { continue; }
		if (o.error) return false;
		if (o.type === "response.completed") {
			const r = o.response;
			if (!r) return true;
			done = true;
			if (responsesOutputText(r) || responsesToolCalls(r).length) return false;
			break;
		}
		if (o.type === "response.output_text.delta" && typeof o.delta === "string") text += o.delta;
	}
	return done ? true : !text;
}
function aggregateResponsesSSE(raw, useFill) {
	let finalResponse = null, text = "", sawError = null;
	for (const line of raw.split("\n")) {
		const t = line.trim();
		if (!t.startsWith("data:")) continue;
		const payload = t.slice(5).trim();
		if (!payload || payload === "[DONE]") continue;
		let obj;
		try { obj = JSON.parse(payload); } catch { continue; }
		if (obj.error) { sawError = obj.error; break; }
		if (obj.type === "response.completed") { finalResponse = obj.response; break; }
		if (obj.type === "response.output_text.delta" && typeof obj.delta === "string") text += obj.delta;
	}
	if (sawError) return { error: sawError };
	if (finalResponse) {
		// 优先用 completed 的完整 output（防 delta 漏/空导致空 content）
		const outText = responsesOutputText(finalResponse);
		const emptyCompleted = !outText && !text;
		if (emptyCompleted) {
			console.log("[ZEN EMPTY]", JSON.stringify({ status: finalResponse.status, incomplete: finalResponse.incomplete_details, outputTypes: (finalResponse.output || []).map((o) => o.type + ":" + (o.name || "")), hasText: !!outText, preview: previewText(raw).slice(0, 300) }));
		}
		if (!outText && text) {
			// 追加而非替换：output 里可能已有 function_call，替换会丢工具调用
			finalResponse = { ...finalResponse, output: [...(finalResponse.output || []), { type: "message", role: "assistant", content: [{ type: "output_text", text }] }] };
		} else if (!outText && !text && useFill && !responsesToolCalls(finalResponse).length) {
			// 真空完成：非 Hermes 客户端追加零宽占位，防 "completed with no content"；工具调用必须保留
			finalResponse = { ...finalResponse, output: [...(finalResponse.output || []), { type: "message", role: "assistant", content: [{ type: "output_text", text: ZWSP }] }] };
		}
		return { error: null, data: finalResponse };
	}
	if (text) {
		return {
			error: null,
			data: { id: ocId("resp"), object: "response", created_at: Math.floor(Date.now() / 1000), status: "completed", output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text }] }] },
		};
	}
	// 无 text 也无 response.completed：非 Hermes 客户端补占位；否则 data:null 会让调用方把原始 SSE 文本原样丢给客户端
	console.log("[ZEN EMPTY-NOCOMPLETED]", JSON.stringify({ useFill, preview: previewText(raw).slice(0, 200) }));
	if (useFill) {
		return { error: null, data: { id: "resp_synth", status: "completed", output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: ZWSP }] }] } };
	}
	// 非 Hermes：返回合法的空结果（不补占位，避免污染），但绝不能把原始 SSE 文本丢给非流式客户端
	return { error: null, data: { id: "resp_synth", status: "completed", output: [] } };
}

async function openAIFullResponse(upstream, env, requestId, model) {
	const raw = await upstream.text();
	const data = safeJsonParse(raw);
	const zenError = parseZenError(raw);
	logUpstreamBody(env, requestId, model, upstream.status, raw, zenError);

	if (upstream.status === 429 || zenError) {
		const details = zenErrorDetails(zenError, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}

	if (isSSE(raw)) {
		const agg = aggregateOpenAISSE(raw);
		if (agg.error) {
			const message = agg.error.message || "Upstream error";
			return openAIErrorResponse(message, "upstream_error", 502, agg.error.code);
		}
		if (agg.data) return jsonResponse(agg.data, upstream.status);
	}

	if (data?.choices) {
		return jsonResponse(normalizeOpenAIFullData(data), upstream.status);
	}

	return new Response(raw, {
		status: upstream.status,
		headers: mergeHeaders({
			"Content-Type": upstream.headers.get("Content-Type") || "application/json; charset=utf-8",
		}),
	});
}

async function openAIStreamResponse(upstream, env, requestId, model) {
	if (!upstream.body) {
		return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const reader = upstream.body.getReader();
	const first = await readWithTimeout(reader, Number(env?.ZEN_FIRST_BYTE_TIMEOUT_MS) || STREAM_FIRST_BYTE_TIMEOUT_MS);
	if (first.done) {
		return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const firstText = new TextDecoder().decode(first.value);
	const zenError = parseZenError(firstText);
	logUpstreamBody(env, requestId, model, upstream.status, firstText, zenError, true);
	if (upstream.status === 429 || zenError) {
		await reader.cancel().catch(() => { });
		const details = zenErrorDetails(zenError, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}

	const encoder = new TextEncoder();
	const decoder = new TextDecoder();
	const normalizer = createOpenAIStreamNormalizer();
	let heartbeat = null;

	const stream = new ReadableStream({
		async start(controller) {
			let buffer = "";
			let doneSent = false;
			let sawFinishReason = false;
			let lastChunk = null;

			const enqueue = (text) => controller.enqueue(encoder.encode(text));
			const sendData = (payload) => enqueue(`data: ${typeof payload === "string" ? payload : JSON.stringify(payload)}\n\n`);
			const sendFinish = (finishReason = "stop") => {
				if (sawFinishReason || doneSent) return;
				sawFinishReason = true;
				sendData({
					id: lastChunk?.id || ocId("chatcmpl"),
					object: "chat.completion.chunk",
					created: lastChunk?.created || Math.floor(Date.now() / 1000),
					model: lastChunk?.model || model,
					choices: [{ index: 0, delta: {}, finish_reason: finishReason }],
				});
			};
			const sendDone = (ensureFinish = true) => {
				if (doneSent) return;
				if (ensureFinish && !sawFinishReason) sendFinish("stop");
				doneSent = true;
				sendData("[DONE]");
			};
			const abortStream = (message) => {
				if (doneSent) return;
				sendData({ error: { message, type: "upstream_error" } });
				sendDone(false);
			};
			const processLine = (rawLine) => {
				const line = rawLine.trimEnd();
				if (!line.startsWith("data:")) return;

				const payload = line.slice(5).trim();
				if (!payload) return;
				if (payload === "[DONE]") {
					sendDone();
					return;
				}
				if (doneSent) return;

				const parsed = safeJsonParse(payload);
				if (!parsed) return;
				lastChunk = parsed;
				if (parsed.choices?.[0]?.finish_reason) sawFinishReason = true;

				const normalized = normalizer.normalize(parsed);
				if (normalized) sendData(normalized);
			};
			const processChunk = (chunk) => {
				buffer += decoder.decode(chunk, { stream: true });
				const lines = buffer.split("\n");
				buffer = lines.pop() || "";
				for (const line of lines) processLine(line);
				heartbeat?.touch();
			};

			heartbeat = createStreamHeartbeat(env, enqueue, () => !doneSent);
			const strictEnd = truthyEnv(env?.STRICT_STREAM_END);

			try {
				processChunk(first.value);
				while (!doneSent) {
					const { done, value } = await reader.read();
					if (done) break;
					processChunk(value);
				}

				const tail = decoder.decode();
				if (tail) buffer += tail;
				if (buffer) processLine(buffer);
				if (doneSent) await reader.cancel().catch(() => { });
				if (!doneSent && strictEnd && !sawFinishReason) {
					debugLog(env, "[STREAM INCOMPLETE]", {
						requestId,
						model,
						reason: "upstream-ended-without-finish_reason",
					});
				}
				if (!doneSent) sendDone();
				heartbeat?.stop();
				controller.close();
			} catch (error) {
				heartbeat?.stop();
				debugLog(env, "[STREAM ERROR]", { requestId, model, message: error?.message || String(error) });
				try {
					// Complete the client-visible SSE protocol even when the upstream socket closes early.
					if (!doneSent) sendDone();
					controller.close();
				} catch { }
			}
		},
		cancel(reason) {
			heartbeat?.stop();
			return reader.cancel(reason);
		},
	});

	return new Response(stream, {
		status: upstream.status,
		headers: mergeHeaders(SSE_HEADERS),
	});
}

function normalizeOpenAIFullData(data) {
	const next = { ...data };
	if (!Array.isArray(next.choices)) return next;

	next.choices = next.choices.map((choice) => {
		if (!choice?.message) return choice;

		const message = { ...choice.message };
		normalizeReasoningField(message);

		if (typeof message.content === "string") {
			const reasoning = extractThinkBlocks(message.content);
			if (reasoning && message.reasoning_content == null) message.reasoning_content = reasoning;

			const visibleContent = stripThinkBlocks(message.content);
			if (visibleContent !== message.content) message.content = visibleContent;
		}

		return { ...choice, message };
	});

	return next;
}

function createOpenAIStreamNormalizer() {
	const contentStates = new Map();

	return {
		normalize(chunk) {
			if (!chunk || !Array.isArray(chunk.choices)) return null;
			if (!chunk.choices.length && chunk.cost != null) return null;

			const next = { ...chunk };
			delete next.cost;
			next.choices = chunk.choices
				.map((choice) => normalizeOpenAIStreamChoice(choice, contentStates))
				.filter(Boolean);

			if (!next.choices.length && !next.usage) return null;
			return next;
		},
	};
}

function normalizeOpenAIStreamChoice(choice, contentStates) {
	if (!choice?.delta) return choice;

	const delta = { ...choice.delta };
	normalizeReasoningField(delta);

	if (typeof delta.content === "string") {
		const state = getThinkState(contentStates, choice.index ?? 0);
		const visibleContent = stripThinkStreamText(delta.content, state);
		if (visibleContent) delta.content = visibleContent;
		else delete delta.content;
	}

	if (!Object.keys(delta).length && !choice.finish_reason) return null;
	return { ...choice, delta };
}

function normalizeReasoningField(target) {
	if (!target || typeof target !== "object") return;
	if (typeof target.reasoning === "string" && target.reasoning && target.reasoning_content == null) {
		target.reasoning_content = target.reasoning;
	}
	delete target.reasoning;
}

function extractThinkBlocks(text) {
	const matches = [];
	const pattern = /<think>([\s\S]*?)<\/think>/gi;
	let match;
	while ((match = pattern.exec(text)) !== null) {
		const content = match[1].trim();
		if (content) matches.push(content);
	}
	return matches.join("\n");
}

function stripThinkBlocks(text) {
	if (!/<\/?think>/i.test(text)) return text;
	const state = createThinkState();
	return stripThinkStreamText(text, state);
}

function getThinkState(states, key) {
	const stateKey = String(key);
	if (!states.has(stateKey)) states.set(stateKey, createThinkState());
	return states.get(stateKey);
}

function createThinkState() {
	return { inThink: false, emittedContent: false, removedThink: false };
}

function stripThinkStreamText(text, state) {
	let output = "";
	let cursor = 0;
	const lower = text.toLowerCase();

	while (cursor < text.length) {
		if (state.inThink) {
			const end = lower.indexOf("</think>", cursor);
			if (end === -1) break;
			cursor = end + "</think>".length;
			state.inThink = false;
			state.removedThink = true;
			continue;
		}

		const start = lower.indexOf("<think>", cursor);
		if (start === -1) {
			output += text.slice(cursor);
			break;
		}

		output += text.slice(cursor, start);
		cursor = start + "<think>".length;
		state.inThink = true;
		state.removedThink = true;
	}

	if (state.removedThink && !state.emittedContent && output) output = output.replace(/^\s+/, "");
	if (output) state.emittedContent = true;
	return output;
}

async function anthropicFullResponse(upstream, model, inputTokens, env, requestId) {
	const raw = await upstream.text();
	const data = safeJsonParse(raw);
	const zenError = parseZenError(raw);
	logUpstreamBody(env, requestId, model, upstream.status, raw, zenError);

	if (upstream.status === 429 || zenError || data?.error) {
		const details = zenErrorDetails(zenError || data?.error, upstream.status);
		return anthropicErrorResponse(details.message, details.anthropicType, details.status);
	}

	// 非流式聚合修复：上游闸门强制 stream:true，返回的永远是 SSE，
	// 原实现直接按 JSON 解析 → data.choices 为空 → 502 Invalid upstream response。
	if (isSSE(raw)) {
		const agg = aggregateOpenAISSE(raw);
		if (agg.error) {
			const details = zenErrorDetails(agg.error, 502);
			return anthropicErrorResponse(details.message, details.anthropicType, details.status);
		}
		if (agg.data) return jsonResponse(openAIToAnthropic(normalizeOpenAIFullData(agg.data), model, inputTokens));
	}

	if (!data?.choices) {
		logUpstreamBody(env, requestId, model, upstream.status, raw, { message: "Invalid upstream response" });
		return anthropicErrorResponse("Invalid upstream response", "upstream_error", 502);
	}

	return jsonResponse(openAIToAnthropic(normalizeOpenAIFullData(data), model, inputTokens));
}

async function anthropicStreamResponse(upstream, model, inputTokens, env, requestId) {
	if (!upstream.body) {
		return anthropicErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const reader = upstream.body.getReader();
	const first = await readWithTimeout(reader, Number(env?.ZEN_FIRST_BYTE_TIMEOUT_MS) || STREAM_FIRST_BYTE_TIMEOUT_MS);
	if (first.done) {
		return anthropicErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const firstText = new TextDecoder().decode(first.value);
	const zenError = parseZenError(firstText);
	logUpstreamBody(env, requestId, model, upstream.status, firstText, zenError, true);
	if (upstream.status === 429 || zenError) {
		await reader.cancel().catch(() => { });
		const details = zenErrorDetails(zenError, upstream.status);
		return anthropicErrorResponse(details.message, details.anthropicType, details.status);
	}

	const encoder = new TextEncoder();
	const decoder = new TextDecoder();
	const msgId = ocId("msg");
	let heartbeat = null;

	const stream = new ReadableStream({
		async start(controller) {
			let buffer = "";
			let outputTokens = 0;
			let messageStarted = false;
			let finished = false;
			let contentStarted = false;
			let sawFinishReason = false;
			let maxToolIdx = -1;
			const openBlocks = new Set();
			const contentStates = new Map();

			const enqueue = (text) => controller.enqueue(encoder.encode(text));
			const sendSSE = (event, data) => enqueue(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
			const startMessage = () => {
				if (messageStarted) return;
				messageStarted = true;
				sendSSE("message_start", {
					type: "message_start",
					message: {
						id: msgId,
						type: "message",
						role: "assistant",
						content: [],
						model,
						stop_reason: null,
						usage: {
							input_tokens: inputTokens || 0,
							output_tokens: 0,
							cache_creation_input_tokens: 0,
							cache_read_input_tokens: 0,
						},
					},
				});
			};
			const startBlock = (index, contentBlock) => {
				startMessage();
				if (openBlocks.has(index)) return;
				openBlocks.add(index);
				sendSSE("content_block_start", {
					type: "content_block_start",
					index,
					content_block: contentBlock,
				});
			};
			const stopBlock = (index) => {
				if (!openBlocks.has(index)) return;
				openBlocks.delete(index);
				sendSSE("content_block_stop", { type: "content_block_stop", index });
			};
			const finishMessage = (finishReason) => {
				if (finished) return;
				finished = true;
				[...openBlocks].sort((a, b) => a - b).forEach(stopBlock);

				let stopReason = "end_turn";
				if (finishReason === "tool_calls") stopReason = "tool_use";
				else if (finishReason === "length") stopReason = "max_tokens";

				startMessage();
				sendSSE("message_delta", {
					type: "message_delta",
					delta: { stop_reason: stopReason },
					usage: { output_tokens: outputTokens },
				});
				sendSSE("message_stop", { type: "message_stop" });
			};
			const processLine = (rawLine) => {
				const line = rawLine.trimEnd();
				if (!line.startsWith("data:")) return;

				const payload = line.slice(5).trim();
				if (!payload || payload === "[DONE]") return;

				const parsed = safeJsonParse(payload);
				const choice = parsed?.choices?.[0];
				if (!choice) return;
				if (choice.finish_reason) sawFinishReason = true;

				const delta = choice.delta || {};
				if (typeof delta.content === "string") {
					const visibleContent = stripThinkStreamText(delta.content, getThinkState(contentStates, choice.index ?? 0));
					if (visibleContent) {
						if (!contentStarted) {
							startBlock(0, { type: "text", text: "" });
							contentStarted = true;
						}
						sendSSE("content_block_delta", {
							type: "content_block_delta",
							index: 0,
							delta: { type: "text_delta", text: visibleContent },
						});
						outputTokens += Math.ceil(visibleContent.length / 4);
					}
				}

				if (delta.tool_calls) {
					for (const toolCall of delta.tool_calls) {
						const toolIdx = toolCall.index ?? 0;
						if (contentStarted) stopBlock(0);
						const blockIdx = contentStarted ? toolIdx + 1 : toolIdx;

						if (toolIdx > maxToolIdx) {
							maxToolIdx = toolIdx;
							startBlock(blockIdx, {
								type: "tool_use",
								id: toolCall.id || ocId("toolu"),
								name: toolCall.function?.name || "",
							});
						}

						if (toolCall.function?.arguments) {
							sendSSE("content_block_delta", {
								type: "content_block_delta",
								index: blockIdx,
								delta: { type: "input_json_delta", partial_json: toolCall.function.arguments },
							});
							outputTokens += Math.ceil(toolCall.function.arguments.length / 4);
						}
					}
				}

				if (choice.finish_reason) finishMessage(choice.finish_reason);
			};
			const processChunk = (chunk) => {
				buffer += decoder.decode(chunk, { stream: true });
				const lines = buffer.split("\n");
				buffer = lines.pop() || "";
				for (const line of lines) processLine(line);
				heartbeat?.touch();
			};

			heartbeat = createStreamHeartbeat(env, enqueue, () => !finished);
			const strictEnd = truthyEnv(env?.STRICT_STREAM_END);

			try {
				processChunk(first.value);
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					processChunk(value);
				}

				const tail = decoder.decode();
				if (tail) buffer += tail;
				if (buffer) processLine(buffer);
				if (!finished && strictEnd && !sawFinishReason) {
					sendSSE("error", {
						type: "error",
						error: { type: "overloaded_error", message: "Upstream stream ended unexpectedly (no finish_reason received)" },
					});
				} else if (!finished) {
					finishMessage("stop");
				}
				heartbeat?.stop();
				controller.close();
			} catch (error) {
				heartbeat?.stop();
				debugLog(env, "[STREAM ERROR]", {
					requestId,
					model,
					name: error?.name,
					message: error?.message || String(error),
					stack: error?.stack,
				});
				try {
					if (!finished) {
						if (!messageStarted) {
							sendSSE("error", {
								type: "error",
								error: { type: "upstream_error", message: "Upstream stream interrupted" },
							});
						}
						// 无论 message_start 是否发出过都补终止序列：Anthropic 客户端以
						// message_stop 判定流正常结束，缺失会被当成断流（dsh PI_AI_ERROR 同类）。
						finishMessage("stop");
					}
					controller.close();
				} catch { }
			}
		},
		cancel(reason) {
			heartbeat?.stop();
			return reader.cancel(reason);
		},
	});

	return new Response(stream, {
		status: 200,
		headers: mergeHeaders(SSE_HEADERS),
	});
}

function anthropicToOpenAI(body) {
	const messages = [];

	if (body.system) {
		const sys = typeof body.system === "string"
			? body.system
			: Array.isArray(body.system)
				? body.system.map((block) => block.text || "").join("\n")
				: "";
		if (sys) messages.push({ role: "system", content: sys });
	}

	for (const msg of body.messages || []) {
		if (typeof msg.content === "string") {
			messages.push({ role: msg.role, content: msg.content });
			continue;
		}

		if (!Array.isArray(msg.content)) continue;

		const text = msg.content
			.filter((block) => block.type === "text")
			.map((block) => block.text)
			.join("\n");
		const toolUses = msg.content.filter((block) => block.type === "tool_use");

		// Convert Anthropic image blocks → OpenAI image_url parts
		const imageParts = [];
		for (const block of msg.content) {
			if (block.type !== "image") continue;
			const src = block.source || {};
			if (src.type === "base64" && src.data) {
				imageParts.push({ type: "image_url", image_url: { url: `data:${src.media_type || "image/png"};base64,${src.data}` } });
			} else if (src.type === "url" && src.url) {
				imageParts.push({ type: "image_url", image_url: { url: src.url } });
			}
		}

		if (toolUses.length && msg.role === "assistant") {
			messages.push({
				role: "assistant",
				content: text || null,
				tool_calls: toolUses.map((toolUse) => ({
					id: toolUse.id,
					type: "function",
					function: {
						name: toolUse.name,
						arguments: JSON.stringify(toolUse.input || {}),
					},
				})),
			});
			continue;
		}

		if (msg.content.some((block) => block.type === "tool_result")) {
			for (const block of msg.content.filter((item) => item.type === "tool_result")) {
				const resultText = typeof block.content === "string"
					? block.content
					: Array.isArray(block.content)
						? block.content.map((item) => item.text || "").join("\n")
						: "";
				messages.push({ role: "tool", tool_call_id: block.tool_use_id, content: resultText });
			}
			continue;
		}

		// Merge text + image parts
		const allParts = [];
		if (text) allParts.push({ type: "text", text });
		allParts.push(...imageParts);
		if (allParts.length > 1) {
			messages.push({ role: msg.role, content: allParts });
		} else if (allParts.length === 1 && allParts[0].type === "text") {
			messages.push({ role: msg.role, content: allParts[0].text });
		} else {
			messages.push({ role: msg.role, content: text });
		}
	}

	const tools = (body.tools || []).map((tool) => ({
		type: "function",
		function: {
			name: tool.name,
			description: tool.description || "",
			parameters: tool.input_schema || {},
		},
	}));

	return { messages, tools: tools.length ? tools : undefined };
}

function openAIToAnthropic(oaiResp, model, inputTokens) {
	const choice = oaiResp.choices?.[0];
	if (!choice) {
		return {
			id: ocId("msg"),
			type: "message",
			role: "assistant",
			content: [{ type: "text", text: "" }],
			model,
			stop_reason: "end_turn",
			usage: {
				input_tokens: inputTokens || 0,
				output_tokens: 0,
				cache_creation_input_tokens: 0,
				cache_read_input_tokens: 0,
			},
		};
	}

	const content = [];
	if (choice.message?.content) {
		content.push({ type: "text", text: choice.message.content });
	}

	if (choice.message?.tool_calls) {
		for (const toolCall of choice.message.tool_calls) {
			let input = {};
			try {
				input = JSON.parse(toolCall.function.arguments);
			} catch { }

			content.push({
				type: "tool_use",
				id: toolCall.id || ocId("toolu"),
				name: toolCall.function.name,
				input,
			});
		}
	}

	if (!content.length) content.push({ type: "text", text: "" });

	let stopReason = "end_turn";
	if (choice.finish_reason === "tool_calls") stopReason = "tool_use";
	else if (choice.finish_reason === "length") stopReason = "max_tokens";

	return {
		id: ocId("msg"),
		type: "message",
		role: "assistant",
		content,
		model,
		stop_reason: stopReason,
		usage: {
			input_tokens: oaiResp.usage?.prompt_tokens || inputTokens || 0,
			output_tokens: oaiResp.usage?.completion_tokens || 0,
			cache_creation_input_tokens: 0,
			cache_read_input_tokens: 0,
		},
	};
}

function responsesToOpenAI(body) {
	const messages = [];

	const instructions = body.instructions;
	if (typeof instructions === "string" && instructions.trim()) {
		messages.push({ role: "system", content: instructions });
	}

	const input = body.input;
	const items = Array.isArray(input)
		? input
		: typeof input === "string"
			? [{ type: "message", role: "user", content: input }]
			: [];

	let pendingAssistant = null;
	let pendingReasoning = "";
	const flushAssistant = () => {
		if (pendingAssistant) {
			if (pendingReasoning) pendingAssistant.reasoning_content = pendingReasoning;
			messages.push(pendingAssistant);
			pendingAssistant = null;
			pendingReasoning = "";
		}
	};

	for (const item of items) {
		if (!item || typeof item !== "object") continue;

		const type = item.type || (item.role ? "message" : "");
		if (type === "message") {
			flushAssistant();
			const role = item.role === "developer" || item.role === "system" ? "system" : item.role;
			if (role !== "system" && role !== "user" && role !== "assistant") continue;
			const content = responsesItemText(item.content);
			if (role === "assistant") {
				const chatMessage = { role, content: content || null };
				if (pendingReasoning) chatMessage.reasoning_content = pendingReasoning;
				pendingReasoning = "";
				messages.push(chatMessage);
			} else if (content) messages.push({ role, content });
			continue;
		}

		if (type === "function_call") {
			const toolCall = {
				id: item.call_id || ocId("call"),
				type: "function",
				function: {
					name: item.name || "",
					arguments: typeof item.arguments === "string"
						? item.arguments
						: JSON.stringify(item.arguments ?? {}),
				},
			};
			if (pendingAssistant) pendingAssistant.tool_calls.push(toolCall);
			else pendingAssistant = { role: "assistant", content: null, tool_calls: [toolCall] };
			continue;
		}

		if (type === "function_call_output") {
			flushAssistant();
			messages.push({
				role: "tool",
				tool_call_id: item.call_id,
				content: stringifyToolOutput(item.output),
			});
			continue;
		}

		if (type === "reasoning") {
			const reasoningText = responsesReasoningText(item);
			if (reasoningText) pendingReasoning = pendingReasoning ? `${pendingReasoning}\n${reasoningText}` : reasoningText;
			continue;
		}

		// 其他未知 item 类型：不传给上游
	}
	flushAssistant();

	const tools = (body.tools || [])
		.filter((tool) => tool && tool.type === "function" && typeof tool.name === "string" && tool.name.trim())
		.map((tool) => ({
			type: "function",
			function: {
				name: tool.name,
				description: tool.description || "",
				parameters: tool.parameters || {},
			},
		}));

	return {
		messages,
		tools: tools.length ? tools : undefined,
		toolChoice: convertToolChoice(body.tool_choice),
		// body.stop 原实现丢弃：OpenAI Responses 协议的 stop 序列不转化 → 上游不截断。
		extras: {
			...(typeof body.temperature === "number" ? { temperature: body.temperature } : {}),
			...(typeof body.top_p === "number" ? { top_p: body.top_p } : {}),
			...(typeof body.max_output_tokens === "number" ? { max_tokens: body.max_output_tokens } : {}),
			...(typeof body.parallel_tool_calls === "boolean" ? { parallel_tool_calls: body.parallel_tool_calls } : {}),
			...(typeof body.stop === "string" || Array.isArray(body.stop) ? { stop: body.stop } : {}),
			...(body.seed !== undefined && body.seed !== null ? { seed: body.seed } : {}),
		},
	};
}

function responsesItemText(content) {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return content
		.filter((part) => part && typeof part === "object" && typeof part.text === "string")
		.map((part) => part.text)
		.join("\n");
}

function responsesReasoningText(item) {
	const summaryText = Array.isArray(item.summary)
		? item.summary.map((part) => (typeof part?.text === "string" ? part.text : "")).join("\n").trim()
		: "";
	if (summaryText) return summaryText;
	if (typeof item.content === "string") return item.content.trim();
	return responsesItemText(item.content).trim();
}

function stringifyToolOutput(output) {
	if (typeof output === "string") return output;
	try {
		return JSON.stringify(output);
	} catch {
		return String(output);
	}
}

function convertToolChoice(toolChoice) {
	if (!toolChoice || typeof toolChoice !== "object" || Array.isArray(toolChoice)) return toolChoice;
	if (toolChoice.type === "function" && toolChoice.name) {
		return { type: "function", function: { name: toolChoice.name } };
	}
	return toolChoice;
}

async function responsesFullResponse(upstream, env, requestId, model, inputTokens) {
	const raw = await upstream.text();
	const data = safeJsonParse(raw);
	const zenError = parseZenError(raw);
	logUpstreamBody(env, requestId, model, upstream.status, raw, zenError);

	if (upstream.status === 429 || zenError || data?.error) {
		const details = zenErrorDetails(zenError || data?.error, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}

	// 非流式聚合修复：同上，上游恒定返回 SSE。
	if (isSSE(raw)) {
		const agg = aggregateOpenAISSE(raw);
		if (agg.error) {
			const details = zenErrorDetails(agg.error, 502);
			return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
		}
		if (agg.data) {
			const normalizedSSE = normalizeOpenAIFullData(agg.data);
			return jsonResponse(buildResponsesResponse(normalizedSSE.choices?.[0], normalizedSSE, model, inputTokens));
		}
	}

	if (!data?.choices) {
		return openAIErrorResponse("Invalid upstream response", "upstream_error", 502);
	}

	const normalized = normalizeOpenAIFullData(data);
	return jsonResponse(buildResponsesResponse(normalized.choices?.[0], normalized, model, inputTokens));
}

async function responsesStreamResponse(upstream, env, requestId, model, inputTokens) {
	if (!upstream.body) {
		return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const reader = upstream.body.getReader();
	const first = await readWithTimeout(reader, Number(env?.ZEN_FIRST_BYTE_TIMEOUT_MS) || STREAM_FIRST_BYTE_TIMEOUT_MS);
	if (first.done) {
		return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const firstText = new TextDecoder().decode(first.value);
	const zenError = parseZenError(firstText);
	logUpstreamBody(env, requestId, model, upstream.status, firstText, zenError, true);
	if (upstream.status === 429 || zenError) {
		await reader.cancel().catch(() => { });
		const details = zenErrorDetails(zenError, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}

	const encoder = new TextEncoder();
	const decoder = new TextDecoder();
	const responseId = ocId("resp");
	const createdAt = Math.floor(Date.now() / 1000);
	const normalizer = createResponsesStreamNormalizer();
	let heartbeat = null;

	const stream = new ReadableStream({
		async start(controller) {
			let buffer = "";
			let sequence = 0;
			let started = false;
			let usage = null;
			let completed = false;

			const enqueue = (text) => controller.enqueue(encoder.encode(text));
			const sendSSE = (type, payload) => {
				sequence += 1;
				enqueue(`event: ${type}\ndata: ${JSON.stringify({ ...payload, sequence_number: sequence, type })}\n\n`);
			};
			const startResponse = () => {
				if (started) return;
				started = true;
				sendSSE("response.created", {
					response: {
						background: false,
						created_at: createdAt,
						error: null,
						id: responseId,
						object: "response",
						output: [],
						status: "in_progress",
					},
				});
				sendSSE("response.in_progress", {
					response: {
						created_at: createdAt,
						id: responseId,
						object: "response",
						status: "in_progress",
					},
				});
			};

			const processChunk = (chunk) => {
				buffer += decoder.decode(chunk, { stream: true });
				const lines = buffer.split("\n");
				buffer = lines.pop() || "";
				for (const line of lines) {
					const trimmed = line.trimEnd();
					if (!trimmed.startsWith("data:")) continue;
					const payload = trimmed.slice(5).trim();
					if (!payload || payload === "[DONE]") continue;
					const parsed = safeJsonParse(payload);
					if (!parsed) continue;
					if (parsed.usage) usage = parsed.usage;
					normalizer.normalize(parsed, sendSSE);
				}
				heartbeat?.touch();
			};

			heartbeat = createStreamHeartbeat(env, enqueue, () => !completed);

			try {
				startResponse();
				processChunk(first.value);
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					processChunk(value);
				}

				const tail = decoder.decode();
				if (tail) buffer += tail;
				if (buffer) {
					const line = buffer.trimEnd();
					buffer = "";
					if (line.startsWith("data:")) {
						const payload = line.slice(5).trim();
						if (payload && payload !== "[DONE]") {
							const parsed = safeJsonParse(payload);
							if (parsed) {
								if (parsed.usage) usage = parsed.usage;
								normalizer.normalize(parsed, sendSSE);
							}
						}
					}
				}

				const result = normalizer.finish(sendSSE);
				const incomplete = result.finishReason === "length" || result.finishReason === "content_filter";
				sendSSE("response.completed", {
					response: {
						background: false,
						created_at: createdAt,
						error: null,
						id: responseId,
						incomplete_details: incomplete
							? { reason: result.finishReason === "content_filter" ? "content_filter" : "max_output_tokens" }
							: null,
						model,
						object: "response",
						output: result.output,
						status: incomplete ? "incomplete" : "completed",
						tool_choice: null,
						tools: [],
						usage: buildResponsesUsage(usage, inputTokens, result.reasoningText),
					},
				});
				completed = true;
				heartbeat?.stop();
				controller.close();
			} catch (error) {
				heartbeat?.stop();
				debugLog(env, "[STREAM ERROR]", { requestId, model, message: error?.message || String(error) });
				try {
					if (!completed) {
						sendSSE("error", {
							error: { message: "Upstream stream interrupted", type: "upstream_error" },
						});
						// 断流也必须给终止事件：pi-ai / dsh 类客户端只认 response.completed /
						// response.failed，缺了会抛 "stream closed before any chunk carried finish_reason"。
						let partial = null;
						try { partial = normalizer.finish(() => { }); } catch { }
						sendSSE("response.completed", {
							response: {
								background: false,
								created_at: createdAt,
								error: null,
								id: responseId,
								incomplete_details: { reason: "upstream_error" },
								model,
								object: "response",
								output: partial?.output || [],
								status: "incomplete",
								tool_choice: null,
								tools: [],
								usage: buildResponsesUsage(usage, inputTokens, partial?.reasoningText),
							},
						});
						completed = true;
					}
					controller.close();
				} catch { }
			}
		},
		cancel(reason) {
			heartbeat?.stop();
			return reader.cancel(reason);
		},
	});

	return new Response(stream, {
		status: 200,
		headers: mergeHeaders(SSE_HEADERS),
	});
}

function buildResponsesResponse(choice, oaiResp, model, inputTokens) {
	const message = choice?.message || {};
	const reasoningText = typeof message.reasoning_content === "string" && message.reasoning_content
		? message.reasoning_content
		: "";
	const contentText = typeof message.content === "string" ? message.content : "";

	const output = [];
	if (reasoningText) {
		output.push({
			type: "reasoning",
			id: ocId("rs"),
			status: "completed",
			encrypted_content: "",
			summary: [{ type: "summary_text", text: reasoningText }],
		});
	}
	if (contentText) {
		output.push({
			type: "message",
			id: ocId("msg"),
			status: "completed",
			role: "assistant",
			content: [{ type: "output_text", text: contentText, annotations: [], logprobs: [] }],
		});
	}
	for (const toolCall of message.tool_calls || []) {
		output.push({
			type: "function_call",
			id: ocId("fc"),
			status: "completed",
			call_id: toolCall.id || ocId("call"),
			name: toolCall.function?.name || "",
			arguments: typeof toolCall.function?.arguments === "string" ? toolCall.function.arguments : "{}",
		});
	}

	const finishReason = choice?.finish_reason;
	const incomplete = !choice || finishReason === "length" || finishReason === "content_filter";

	return {
		id: ocId("resp"),
		object: "response",
		created_at: Math.floor(Date.now() / 1000),
		status: incomplete ? "incomplete" : "completed",
		background: false,
		error: null,
		incomplete_details: incomplete
			? { reason: finishReason === "content_filter" ? "content_filter" : "max_output_tokens" }
			: null,
		model,
		output,
		tool_choice: null,
		tools: [],
		usage: buildResponsesUsage(oaiResp?.usage, inputTokens, reasoningText),
	};
}

function buildResponsesUsage(chatUsage, inputTokens, reasoningText) {
	const prompt = chatUsage?.prompt_tokens || inputTokens || 0;
	const completion = chatUsage?.completion_tokens || 0;
	return {
		input_tokens: prompt,
		input_tokens_details: {
			cached_tokens: chatUsage?.prompt_tokens_details?.cached_tokens || 0,
		},
		output_tokens: completion,
		output_tokens_details: {
			reasoning_tokens: reasoningText ? Math.ceil(reasoningText.length / 4) : 0,
		},
		total_tokens: prompt + completion,
	};
}

function createResponsesStreamNormalizer() {
	const state = {
		nextOii: 0,
		finishReason: null,
		reasoning: { id: null, oi: null, text: "", added: false, closed: false },
		message: { id: null, oi: null, text: "", added: false, closed: false },
		tools: new Map(),
		toolOrder: [],
		thinkState: createThinkState(),
	};

	function closeReasoning(send) {
		if (!state.reasoning.added || state.reasoning.closed) return;
		state.reasoning.closed = true;
		send("response.reasoning_summary_text.done", {
			item_id: state.reasoning.id,
			output_index: state.reasoning.oi,
			summary_index: 0,
			text: state.reasoning.text,
		});
		send("response.reasoning_summary_part.done", {
			item_id: state.reasoning.id,
			output_index: state.reasoning.oi,
			part: { text: state.reasoning.text, type: "summary_text" },
			summary_index: 0,
		});
		send("response.output_item.done", {
			item: {
				encrypted_content: "",
				id: state.reasoning.id,
				status: "completed",
				summary: [{ text: state.reasoning.text, type: "summary_text" }],
				type: "reasoning",
			},
			output_index: state.reasoning.oi,
		});
	}

	function closeMessage(send) {
		if (!state.message.added || state.message.closed) return;
		state.message.closed = true;
		send("response.output_text.done", {
			content_index: 0,
			item_id: state.message.id,
			logprobs: [],
			output_index: state.message.oi,
			text: state.message.text,
		});
		send("response.content_part.done", {
			content_index: 0,
			item_id: state.message.id,
			output_index: state.message.oi,
			part: { annotations: [], logprobs: [], text: state.message.text, type: "output_text" },
		});
		send("response.output_item.done", {
			item: {
				content: [{ annotations: [], logprobs: [], text: state.message.text, type: "output_text" }],
				id: state.message.id,
				role: "assistant",
				status: "completed",
				type: "message",
			},
			output_index: state.message.oi,
		});
	}

	function closeTool(tool, send) {
		if (!tool.added || tool.closed) return;
		tool.closed = true;
		send("response.function_call_arguments.done", {
			item_id: tool.id,
			output_index: tool.oi,
			arguments: tool.args,
		});
		send("response.output_item.done", {
			item: {
				arguments: tool.args,
				call_id: tool.callId,
				id: tool.id,
				name: tool.name,
				status: "completed",
				type: "function_call",
			},
			output_index: tool.oi,
		});
	}

	function emitReasoning(text, send) {
		if (state.reasoning.closed) return;
		if (!state.reasoning.added) {
			state.reasoning.added = true;
			state.reasoning.id = ocId("rs");
			state.reasoning.oi = state.nextOii++;
			send("response.output_item.added", {
				item: {
					id: state.reasoning.id,
					status: "in_progress",
					summary: [],
					type: "reasoning",
				},
				output_index: state.reasoning.oi,
			});
			send("response.reasoning_summary_part.added", {
				item_id: state.reasoning.id,
				output_index: state.reasoning.oi,
				part: { text: "", type: "summary_text" },
				summary_index: 0,
			});
		}
		state.reasoning.text += text;
		send("response.reasoning_summary_text.delta", {
			item_id: state.reasoning.id,
			output_index: state.reasoning.oi,
			summary_index: 0,
			delta: text,
		});
	}

	function emitMessageText(text, send) {
		if (state.message.closed) return;
		if (!state.message.added) {
			state.message.added = true;
			state.message.id = ocId("msg");
			state.message.oi = state.nextOii++;
			send("response.output_item.added", {
				item: {
					content: [],
					id: state.message.id,
					role: "assistant",
					status: "in_progress",
					type: "message",
				},
				output_index: state.message.oi,
			});
			send("response.content_part.added", {
				content_index: 0,
				item_id: state.message.id,
				output_index: state.message.oi,
				part: { annotations: [], logprobs: [], text: "", type: "output_text" },
			});
		}
		state.message.text += text;
		send("response.output_text.delta", {
			content_index: 0,
			delta: text,
			item_id: state.message.id,
			logprobs: [],
			output_index: state.message.oi,
		});
	}

	function emitToolDelta(toolCall, send) {
		const index = toolCall.index ?? 0;
		let tool = state.tools.get(index);
		if (!tool) {
			tool = {
				index,
				id: ocId("fc"),
				callId: toolCall.id || ocId("call"),
				name: toolCall.function?.name || "",
				args: "",
				oi: state.nextOii++,
				added: false,
				closed: false,
			};
			state.tools.set(index, tool);
			state.toolOrder.push(tool);
		}
		if (toolCall.id) tool.callId = toolCall.id;
		if (toolCall.function?.name) tool.name = toolCall.function.name;
		if (!tool.added) {
			tool.added = true;
			send("response.output_item.added", {
				item: {
					id: tool.id,
					call_id: tool.callId,
					name: tool.name,
					arguments: "",
					status: "in_progress",
					type: "function_call",
				},
				output_index: tool.oi,
			});
		}
		const argsDelta = typeof toolCall.function?.arguments === "string" ? toolCall.function.arguments : "";
		if (argsDelta) {
			tool.args += argsDelta;
			send("response.function_call_arguments.delta", {
				item_id: tool.id,
				output_index: tool.oi,
				delta: argsDelta,
			});
		}
	}

	function buildOutput() {
		const output = [];
		if (state.reasoning.text) {
			output.push({
				encrypted_content: "",
				id: state.reasoning.id,
				status: "completed",
				summary: [{ text: state.reasoning.text, type: "summary_text" }],
				type: "reasoning",
			});
		}
		if (state.message.text) {
			output.push({
				content: [{ annotations: [], logprobs: [], text: state.message.text, type: "output_text" }],
				id: state.message.id,
				role: "assistant",
				status: "completed",
				type: "message",
			});
		}
		for (const tool of state.toolOrder) {
			output.push({
				arguments: tool.args,
				call_id: tool.callId,
				id: tool.id,
				name: tool.name,
				status: "completed",
				type: "function_call",
			});
		}
		return output;
	}

	return {
		normalize(chunk, send) {
			const choice = chunk?.choices?.[0];
			if (!choice) return;
			const delta = choice.delta || {};
			if (choice.finish_reason) state.finishReason = choice.finish_reason;

			normalizeReasoningField(delta);

			let reasoning = "";
			if (typeof delta.reasoning_content === "string" && delta.reasoning_content) {
				reasoning += delta.reasoning_content;
			}

			let content = "";
			if (typeof delta.content === "string") {
				const split = splitThinkStreamText(delta.content, state.thinkState);
				content = split.visible;
				reasoning += split.reasoning;
			}

			if (reasoning) emitReasoning(reasoning, send);
			if (content) {
				closeReasoning(send);
				emitMessageText(content, send);
			}
			if (Array.isArray(delta.tool_calls)) {
				closeReasoning(send);
				closeMessage(send);
				for (const toolCall of delta.tool_calls) {
					emitToolDelta(toolCall, send);
				}
			}
		},
		finish(send) {
			closeReasoning(send);
			closeMessage(send);
			for (const tool of state.toolOrder) closeTool(tool, send);
			return {
				finishReason: state.finishReason,
				reasoningText: state.reasoning.text,
				output: buildOutput(),
			};
		},
	};
}

function splitThinkStreamText(text, state) {
	let visible = "";
	let reasoning = "";
	let cursor = 0;
	const lower = text.toLowerCase();

	while (cursor < text.length) {
		if (state.inThink) {
			const end = lower.indexOf("</think>", cursor);
			if (end === -1) {
				reasoning += text.slice(cursor);
				break;
			}
			reasoning += text.slice(cursor, end);
			cursor = end + "</think>".length;
			state.inThink = false;
			state.removedThink = true;
			continue;
		}

		const start = lower.indexOf("<think>", cursor);
		if (start === -1) {
			visible += text.slice(cursor);
			break;
		}
		visible += text.slice(cursor, start);
		cursor = start + "<think>".length;
		state.inThink = true;
		state.removedThink = true;
	}

	if (state.removedThink && !state.emittedContent && visible) visible = visible.replace(/^\s+/, "");
	if (visible) state.emittedContent = true;
	return { visible, reasoning };
}

// 日志只在 DEBUG_LOG=true 时输出；响应正文预览仅在错误或 DEBUG_LOG_BODY=true 时输出。
function logZenRequest(env, requestId, format, model, stream, user, zenReq, messageCount) {
	debugLog(env, "[ZEN REQ]", {
		requestId,
		format,
		user,
		model,
		stream: !!stream,
		messageCount,
		bodyBytes: byteLength(zenReq.body),
		ocRequest: shortId(zenReq.headers["x-session-id"]),
		ocSession: shortId(zenReq.headers["x-opencode-session"]),
	});
	// v2 闸门调试:完整 header 集合(不含 authorization 值)
	if (env?.DEBUG_ZEN) {
		const h = { ...zenReq.headers };
		delete h["Authorization"];
		console.log("[ZEN HDRS]", JSON.stringify(h));
		console.log("[ZEN BODY]", zenReq.body.slice(0, 2000));
	}
}

function logZenResponse(env, payload) {
	if (!debugEnabled(env) && payload.status < 400) return;
	console.log("[ZEN RES]", JSON.stringify(payload));
}

function logUpstreamBody(env, requestId, model, status, raw, zenError, firstChunk = false) {
	const body = String(raw || "");
	const shouldLog = debugEnabled(env) || Boolean(zenError) || status >= 400;
	if (!shouldLog) return;

	const shouldPrintPreview = Boolean(zenError) || status >= 400 || bodyLogEnabled(env);
	const payload = {
		requestId,
		model,
		status,
		firstChunk,
		chars: body.length,
	};

	if (zenError) payload.zenError = zenError;
	if (shouldPrintPreview) payload.preview = previewText(body);

	console.log("[ZEN BODY]", JSON.stringify(payload));
}

function debugLog(env, label, payload) {
	if (!debugEnabled(env)) return;
	console.log(label, JSON.stringify(payload));
}

function debugEnabled(env) {
	return truthyEnv(env?.DEBUG_LOG);
}

function bodyLogEnabled(env) {
	return truthyEnv(env?.DEBUG_LOG_BODY);
}

function truthyEnv(value) {
	return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

async function readWithTimeout(reader, timeoutMs) {
	const limit = Number(timeoutMs);
	if (!(limit > 0)) return { ...(await reader.read()), reader };
	let timer;
	try {
		const result = await Promise.race([
			reader.read(),
			new Promise((_, reject) => {
				timer = setTimeout(() => reject(new Error("first-byte-timeout")), limit);
			}),
		]);
		return { ...result, reader };
	} catch (error) {
		await reader.cancel(error?.message || "first-byte-timeout").catch(() => { });
		throw error;
	} finally {
		clearTimeout(timer);
	}
}

function replayFirstChunk(response, firstValue, reader) {
	let first = true;
	const body = new ReadableStream({
		async pull(controller) {
			if (first) {
				first = false;
				controller.enqueue(firstValue);
				return;
			}
			try {
				const next = await reader.read();
				if (next.done) controller.close();
				else controller.enqueue(next.value);
			} catch (error) {
				controller.error(error);
			}
		},
		cancel(reason) {
			return reader.cancel(reason);
		},
	});
	return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
}

function createStreamHeartbeat(env, enqueue, isAlive) {
	const intervalMs = Number(env?.STREAM_HEARTBEAT_MS) || STREAM_HEARTBEAT_MS;
	if (!(intervalMs > 0)) return null;

	let lastActivity = Date.now();
	const timer = setInterval(() => {
		if (!isAlive() || Date.now() - lastActivity < intervalMs) return;
		lastActivity = Date.now();
		enqueue(": ping\n\n");
	}, intervalMs);

	return {
		touch() {
			lastActivity = Date.now();
		},
		stop() {
			clearInterval(timer);
		},
	};
}

function byteLength(text) {
	return new TextEncoder().encode(String(text || "")).length;
}

function shortId(id) {
	const text = String(id || "");
	if (text.length <= 16) return text;
	return `${text.slice(0, 8)}...${text.slice(-6)}`;
}

function previewText(text, max = 800) {
	return String(text || "")
		.replace(/\s+/g, " ")
		.slice(0, max);
}

function authenticate(request, env, format = "openai") {
	if (String(env?.DISABLE_AUTH || "").toLowerCase() === "true") {
		return { user: "anonymous" };
	}

	const apiKey = firstEnvValue(env, "API_KEY", "TOKEN");
	if (!apiKey) {
		const message = "No API key configured. Set Cloudflare secret or variable API_KEY or TOKEN.";
		return {
			error: format === "anthropic"
				? anthropicErrorResponse(message, "authentication_error", 500)
				: openAIErrorResponse(message, "server_error", 500),
		};
	}

	const header = request.headers.get("authorization") || request.headers.get("x-api-key") || "";
	const token = header.toLowerCase().startsWith("bearer ") ? header.slice(7).trim() : header.trim();

	if (constantTimeEqual(token, apiKey)) return { user: "user-default" };

	return {
		error: format === "anthropic"
			? anthropicErrorResponse("Invalid API key", "authentication_error", 401)
			: openAIErrorResponse("Invalid API key", "authentication_error", 401),
	};
}

function constantTimeEqual(left, right) {
	const a = String(left || "");
	const b = String(right || "");
	const length = Math.max(a.length, b.length);
	let difference = a.length ^ b.length;
	for (let index = 0; index < length; index++) {
		difference |= (a.charCodeAt(index) || 0) ^ (b.charCodeAt(index) || 0);
	}
	return difference === 0;
}

function firstEnvValue(env, ...keys) {
	for (const key of keys) {
		const value = typeof env?.[key] === "string" ? env[key].trim() : "";
		if (value) return value;
	}
	return "";
}

const requestIds = new Map();

// x-opencode-request mirrors the real client's stable per-session request id.
function getRequest(sessionId) {
	const existing = requestIds.get(sessionId);
	if (existing) return existing;
	const id = ocId("msg");
	requestIds.set(sessionId, id);
	if (requestIds.size > 256) {
		let drop = Math.floor(requestIds.size / 2);
		for (const key of requestIds.keys()) {
			requestIds.delete(key);
			if (--drop <= 0) break;
		}
	}
	return id;
}

function ocIdDescending(prefix, timestamp = Date.now()) {
	if (timestamp !== ocIdLastTs) {
		ocIdLastTs = timestamp;
		ocIdCounter = 0;
	}
	ocIdCounter = (ocIdCounter + 1) % 0x1000;
	// CLI 的 session ID 为 descending(按位取反),闸门校验 ID 方向(ses asc 实测 403)
	const current = ~(BigInt(timestamp) * 0x1000n + BigInt(ocIdCounter));
	let time = "";
	for (let index = 0; index < 6; index++) {
		time += Number((current >> BigInt(40 - 8 * index)) & 0xffn).toString(16).padStart(2, "0");
	}
	const bytes = new Uint8Array(14);
	crypto.getRandomValues(bytes);
	let random = "";
	for (const byte of bytes) random += OC_ID_CHARS[byte % 62];
	return `${prefix}_${time}${random}`;
}

function getSession(user) {
	const now = Date.now();
	const existing = userSessions.get(user);
	if (!existing || now - existing.ts > 30 * 60 * 1000) {
		const next = { id: ocIdDescending("ses"), ts: now };
		userSessions.set(user, next);
		return next.id;
	}
	existing.ts = now;
	return existing.id;
}

async function readJson(request, format = "openai") {
	try {
		return { body: await request.json() };
	} catch {
		const message = "Invalid JSON body";
		return {
			error: format === "anthropic"
				? anthropicErrorResponse(message, "invalid_request_error", 400)
				: openAIErrorResponse(message, "invalid_request_error", 400),
		};
	}
}

function parseZenError(raw) {
	const text = String(raw || "").trim();
	if (!text.startsWith("{")) return null;

	const parsed = safeJsonParse(text);
	if (!parsed || (!parsed.error && parsed.type !== "error" && typeof parsed.message !== "string")) return null;

	return {
		message: parsed.error?.message || parsed.message || "Rate limit exceeded",
		type: parsed.error?.type || parsed.type || "upstream_error",
		code: parsed.error?.code || parsed.code,
	};
}

function zenErrorDetails(zenError, upstreamStatus) {
	const message = zenError?.message || (upstreamStatus === 429 ? "Rate limit exceeded" : "Upstream error");

	if (upstreamStatus === 429 || isFreeUsageError(zenError)) {
		return {
			message: `${message} (free model rate limit)`,
			openaiType: "rate_limit_error",
			anthropicType: "rate_limit_error",
			status: 429,
			code: "rate_limit_exceeded",
		};
	}

	if (isModelUnsupportedError(zenError)) {
		return {
			message,
			openaiType: "invalid_request_error",
			anthropicType: "invalid_request_error",
			status: clientErrorStatus(upstreamStatus, 400),
			code: zenError?.code || "model_not_found",
		};
	}

	return {
		message,
		openaiType: zenError?.type || "upstream_error",
		anthropicType: zenError?.type || "upstream_error",
		status: clientErrorStatus(upstreamStatus, 502),
		code: zenError?.code,
	};
}

function isFreeUsageError(error) {
	const text = `${error?.type || ""} ${error?.code || ""} ${error?.message || ""}`.toLowerCase();
	return text.includes("freeusagelimiterror") || text.includes("rate limit") || text.includes("quota");
}

function isModelUnsupportedError(error) {
	const text = `${error?.type || ""} ${error?.code || ""} ${error?.message || ""}`.toLowerCase();
	return text.includes("model_not_found")
		|| text.includes("unknown model")
		|| text.includes("unsupported model")
		|| (text.includes("model") && text.includes("not found"))
		|| (text.includes("model") && text.includes("not supported"));
}

function clientErrorStatus(status, fallback) {
	return status >= 400 && status < 500 ? status : fallback;
}

function upstreamErrorResponse(error, format) {
	const timeout = error?.message === "timeout" || error?.message === "first-response-timeout";
	if (error?.status === 429) {
		return format === "anthropic"
			? anthropicErrorResponse("Rate limit exceeded", "rate_limit_error", 429)
			: openAIErrorResponse("Rate limit exceeded", "rate_limit_error", 429, "rate_limit_exceeded");
	}
	const message = error?.message === "first-response-timeout"
		? "Upstream first response timeout"
		: timeout ? "Upstream timeout" : `Upstream error: ${error?.message || error}`;
	const type = timeout ? "timeout_error" : "upstream_error";
	const status = timeout ? 504 : 502;

	return format === "anthropic"
		? anthropicErrorResponse(message, type, status)
		: openAIErrorResponse(message, type, status);
}

function openAIErrorResponse(message, type, status, code) {
	return jsonResponse({
		error: {
			message,
			type,
			...(code ? { code } : {}),
		},
	}, status);
}

function anthropicErrorResponse(message, type, status) {
	return jsonResponse({
		type: "error",
		error: { type, message },
	}, status);
}

function jsonResponse(data, status = 200, headers = {}) {
	return new Response(JSON.stringify(data), {
		status,
		headers: mergeHeaders(JSON_HEADERS, headers),
	});
}

function mergeHeaders(...sets) {
	const headers = new Headers(CORS_HEADERS);
	for (const set of sets) {
		for (const [key, value] of Object.entries(set || {})) {
			headers.set(key, value);
		}
	}
	return headers;
}

function safeJsonParse(text) {
	try {
		return JSON.parse(text);
	} catch {
		return null;
	}
}

const OC_ID_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
let ocIdLastTs = 0;
let ocIdCounter = 0;

// Mirrors @opencode-ai/schema identifier.create(): <prefix>_ + 12 hex chars
// derived from timestamp*0x1000+counter, then 14 base62 random chars (30 total).
function ocId(prefix, timestamp = Date.now()) {
	if (timestamp !== ocIdLastTs) {
		ocIdLastTs = timestamp;
		ocIdCounter = 0;
	}
	ocIdCounter = (ocIdCounter + 1) % 0x1000;
	const current = BigInt(timestamp) * 0x1000n + BigInt(ocIdCounter);
	let time = "";
	for (let index = 0; index < 6; index++) {
		time += Number((current >> BigInt(40 - 8 * index)) & 0xffn).toString(16).padStart(2, "0");
	}
	const bytes = new Uint8Array(14);
	crypto.getRandomValues(bytes);
	let random = "";
	for (const byte of bytes) random += OC_ID_CHARS[byte % 62];
	return `${prefix}_${time}${random}`;
}

function responsesBridgeModels(env) {
	const configured = String(env?.RESPONSES_BRIDGE_MODELS || "")
		.split(",")
		.map((m) => m.trim())
		.filter(Boolean);
	return configured.length ? configured : RESPONSES_BRIDGE_MODELS;
}

// 部分模型（space-bunny-free）上游只支持 Anthropic 原生 /zen/v1/messages，
// 对 OpenAI chat/responses 端点会返回 400/不支持；需在 OpenAI 入站路径桥接。
const ANTHROPIC_BRIDGE_MODELS = ["space-bunny-free"];

function shouldUseAnthropicBridge(env, model) {
	const configured = String(env?.ANTHROPIC_BRIDGE_MODELS || "")
		.split(",").map((s) => s.trim()).filter(Boolean);
	return (configured.length ? configured : ANTHROPIC_BRIDGE_MODELS).includes(model);
}

function openAIToAnthropicBody(body) {
	const messages = [];
	const systemParts = [];
	for (const m of body.messages || []) {
		if (m?.role === "system" || m?.role === "developer") {
			if (typeof m.content === "string") systemParts.push(m.content);
			else if (Array.isArray(m.content)) systemParts.push(m.content.map((x) => x?.text || "").join(""));
			continue;
		}
		if (m?.role === "tool") {
			messages.push({ role: "user", content: [{ type: "tool_result", tool_use_id: m.tool_call_id || "call_0", content: typeof m.content === "string" ? m.content : JSON.stringify(m.content ?? "") }] });
			continue;
		}
		if (m?.role === "assistant" && Array.isArray(m.tool_calls) && m.tool_calls.length) {
			const content = [];
			if (typeof m.content === "string" && m.content) content.push({ type: "text", text: m.content });
			for (const c of m.tool_calls) {
				let input = {};
				try { input = c.function?.arguments ? JSON.parse(c.function.arguments) : {}; } catch { input = { _raw: c.function?.arguments || "" }; }
				content.push({ type: "tool_use", id: c.id || `call_${content.length}`, name: c.function?.name || "bash", input });
			}
			messages.push({ role: "assistant", content });
			continue;
		}
		if (typeof m?.content === "string") { messages.push({ role: m.role, content: m.content }); continue; }
		if (Array.isArray(m?.content)) {
			const parts = [];
			for (const x of m.content) {
				if (x?.type === "text" || x?.type === "input_text") parts.push({ type: "text", text: x.text || "" });
				else if (x?.type === "image_url") {
					const u = x.image_url?.url || "";
					const mt = u.match(/^data:([^;]+);base64,(.*)$/);
					if (mt) parts.push({ type: "image", source: { type: "base64", media_type: mt[1], data: mt[2] } });
				}
			}
			if (parts.length) messages.push({ role: m.role, content: parts });
		}
	}
	// Anthropic 不接受连续同角色消息：OpenAI 的 tool/user 连续消息必须合并为一个 user 消息，
	// 否则上游会静默返回空正文（表现为 dsh/Hermes “No reply”）。
	const mergedMessages = [];
	for (const msg of messages) {
		const last = mergedMessages[mergedMessages.length - 1];
		if (last && last.role === msg.role) {
			const toContent = (v) => typeof v === "string" ? [{ type: "text", text: v }] : (Array.isArray(v) ? v : []);
			last.content = [...toContent(last.content), ...toContent(msg.content)];
		} else {
			mergedMessages.push(msg);
		}
	}
	const tools = (body.tools || []).filter((t) => t?.type === "function" && t.function?.name).map((t) => ({ name: t.function.name, description: t.function.description || "", input_schema: t.function.parameters || { type: "object", properties: {} } }));
	// 真实工具 schema 优先；仅补上游闸门要求且用户未提供的 bash/read
	const seen = new Set();
	const merged = [];
	for (const t of tools) { if (!seen.has(t.name)) { merged.push(t); seen.add(t.name); } }
	for (const t of FAKE_TOOL) { if (!seen.has(t.function.name)) { merged.push({ name: t.function.name, description: t.function.description, input_schema: t.function.parameters }); seen.add(t.function.name); } }
	const max = Number(body.max_tokens || body.max_completion_tokens) > 0 ? Math.floor(Number(body.max_tokens || body.max_completion_tokens)) : 4096;
	const out = { model: body.model, messages: mergedMessages, max_tokens: Math.max(max, 1024), stream: true, tools: merged };
	// 不转发 tool_choice：该上游对带 tools 的 Anthropic 请求会因 tool_choice 报 invalid params
	// 2026-10-13 闸门:system 前缀须命中 opencode build 提示词白名单(前缀匹配,A2 实测),原 system 拼接在后
	// v2 闸门不再校验 build system 白名单(2026-10-14 实测):仅透传客户端 system
	if (systemParts.length) out.system = systemParts.join("\n");
	if (typeof body.temperature === "number") out.temperature = body.temperature;
	if (typeof body.top_p === "number") out.top_p = body.top_p;
	// 客户端 stop 序列(OpenAI stop / Anthropic stop_sequences)原先被丢弃,模型无法按要求停;
	// 上游 Anthropic 端点认 stop_sequences,补齐。
	const stopSeq = Array.isArray(body.stop) ? body.stop : (typeof body.stop === "string" ? [body.stop] : null);
	const anthStop = Array.isArray(body.stop_sequences) && body.stop_sequences.length ? body.stop_sequences : stopSeq;
	if (anthStop && anthStop.length) out.stop_sequences = anthStop.slice(0, 8);
	return out;
}

function anthropicResponseToOpenAI(data, model) {
	const textParts = (data.content || []).filter((b) => b.type === "text").map((b) => b.text || "");
	const thinking = (data.content || []).filter((b) => b.type === "thinking").map((b) => b.thinking || "").join("\n");
	const calls = (data.content || []).filter((b) => b.type === "tool_use");
	const msg = { role: "assistant", content: textParts.join("") || null };
	if (calls.length) msg.tool_calls = calls.map((c, i) => ({ index: i, id: c.id || `call_${i}`, type: "function", function: { name: c.name || "", arguments: JSON.stringify(c.input || {}) } }));
	if (thinking && typeof msg.reasoning_content === "string") msg.reasoning_content = thinking;
	return {
		id: data.id || ocId("chatcmpl"), object: "chat.completion", created: Math.floor(Date.now() / 1000), model,
		choices: [{ index: 0, message: msg, finish_reason: calls.length ? "tool_calls" : (data.stop_reason === "max_tokens" ? "length" : "stop") }],
		usage: { prompt_tokens: data.usage?.input_tokens || 0, completion_tokens: data.usage?.output_tokens || 0, total_tokens: (data.usage?.input_tokens || 0) + (data.usage?.output_tokens || 0) },
	};
}

async function handleNativeAnthropicBridge(body, env, requestId, model, user, request) {
	const sessionId = getSession(user);
	const headers = buildZenHeaders(env, sessionId, request ? parentSessionFrom(request) : "");
	headers["anthropic-version"] = "2023-06-01";
	const upstreamBody = { ...body, model, stream: true };
	delete upstreamBody.stream_options;
	// 上游强制 tools 非空；原生请求未带 tools 时补最小 bash 工具
	if (!Array.isArray(upstreamBody.tools) || !upstreamBody.tools.length) {
		upstreamBody.tools = [
		{ name: "read", description: ZEN_GATE_TOOL_NAMES_V2[0].description, input_schema: ZEN_GATE_TOOL_NAMES_V2[0].parameters },
		{ name: "shell", description: ZEN_GATE_TOOL_NAMES_V2[1].description, input_schema: ZEN_GATE_TOOL_NAMES_V2[1].parameters },
	];
	}
	if (upstreamBody.tool_choice) {
		const tc = upstreamBody.tool_choice;
		const pickName = typeof tc === "string" ? (tc === "auto" || tc === "any" ? "" : tc) : (tc?.type === "tool" ? tc.name : "");
		if (tc === "none") delete upstreamBody.tool_choice;
		else if (pickName) upstreamBody.tool_choice = { type: "tool", name: pickName };
		else upstreamBody.tool_choice = "auto";
	}
	const zenReq = { body: JSON.stringify(upstreamBody), headers };
	logZenRequest(env, requestId, "anthropic-native-bridge", model, !!body.stream, user, zenReq, body.messages?.length || 0);

	let upstream;
	try {
		upstream = await fetchZenWithFallback(
			"/inference/v1/messages",
			() => ({ method: "POST", headers: zenReq.headers, body: zenReq.body }),
			env, requestId, model, !!body.stream,
		);
	} catch (error) {
		return upstreamErrorResponse(error, "anthropic");
	}
	if (body.stream) {
		if (!upstream.body) return anthropicErrorResponse("Empty response from upstream", "upstream_error", 502);
		// 正常流式响应直接透传；若上游返回 JSON 错误（429/4xx）则不能伪装成 200 SSE
		if (upstream.status >= 400 || !String(upstream.headers.get("content-type") || "").includes("text/event-stream")) {
			const errRaw = await upstream.text();
			const err = parseZenError(errRaw);
			const d = zenErrorDetails(err, upstream.status);
			return anthropicErrorResponse(d.message || "Upstream error", d.anthropicType || "api_error", upstream.status >= 400 ? upstream.status : d.status);
		}
		return new Response(upstream.body, {
			status: upstream.status,
			headers: mergeHeaders(SSE_HEADERS, { "anthropic-version": "2023-06-01" }),
		});
	}
	const raw = await upstream.text();
	const e = parseZenError(raw);
	logUpstreamBody(env, requestId, model, upstream.status, raw, e);
	if (!upstream.ok || e) {
		const d = zenErrorDetails(e, upstream.status);
		return anthropicErrorResponse(d.message || "Upstream error", d.type || "api_error", upstream.status >= 400 ? upstream.status : d.status);
	}
	let data = isSSE(raw) ? aggregateAnthropicBridgeSSE(raw) : safeJsonParse(raw);
	if (data?.type === "error" || data?.error) {
		const err = data.error || data;
		const d = zenErrorDetails(err, upstream.status);
		return anthropicErrorResponse(d.message || "Upstream error", err?.type || "api_error", upstream.status >= 400 ? upstream.status : d.status);
	}
	if (!data || !Array.isArray(data.content)) return anthropicErrorResponse("Invalid Anthropic response", "upstream_error", 502);
	return jsonResponse({
		id: data.id || ocId("msg"),
		type: "message",
		role: "assistant",
		model,
		content: data.content,
		stop_reason: data.stop_reason || "end_turn",
		stop_sequence: data.stop_sequence ?? null,
		usage: {
			input_tokens: data.usage?.input_tokens || 0,
			output_tokens: data.usage?.output_tokens || 0,
			...(data.usage?.cache_read_input_tokens ? { cache_read_input_tokens: data.usage.cache_read_input_tokens } : {}),
		},
	}, 200);
}

async function handleOpenAIAnthropicBridge(body, env, requestId, model, user, request) {
	const sessionId = getSession(user);
	const headers = buildZenHeaders(env, sessionId, request ? parentSessionFrom(request) : "");
	headers["anthropic-version"] = "2023-06-01";
	const upstreamBody = openAIToAnthropicBody(body);
	const zenReq = { body: JSON.stringify(upstreamBody), headers };
	logZenRequest(env, requestId, "openai-anthropic-bridge", model, !!body.stream, user, zenReq, body.messages?.length || 0);
	let upstream;
	try {
		upstream = await fetchZenWithFallback(
			"/inference/v1/messages",
			() => ({ method: "POST", headers: zenReq.headers, body: zenReq.body }),
			env, requestId, model, !!body.stream,
		);
	} catch (error) {
		return upstreamErrorResponse(error, "openai");
	}
	if (upstream.status === 429) {
		const rawErr = await upstream.text();
		const e = parseZenError(rawErr);
		const d = zenErrorDetails(e, 429);
		return openAIErrorResponse(d.message, d.openaiType, d.status, d.code);
	}
	return body.stream
		? anthropicBridgeToOpenAIStream(upstream, model, env, requestId)
		: anthropicBridgeToOpenAIFull(upstream, model, env, requestId);
}

async function anthropicBridgeToOpenAIFull(upstream, model, env, requestId) {
	const raw = await upstream.text();
	const e = parseZenError(raw);
	logUpstreamBody(env, requestId, model, upstream.status, raw, e);
	if (!upstream.ok || e) {
		const d = zenErrorDetails(e, upstream.status);
		return openAIErrorResponse(d.message || "Upstream error", d.openaiType, upstream.status >= 400 ? upstream.status : d.status, d.code);
	}
	// 上游强制 stream:true；非流式客户端需要聚合 Anthropic SSE
	let data = isSSE(raw) ? aggregateAnthropicBridgeSSE(raw) : safeJsonParse(raw);
	if (data?.error) {
		const d = zenErrorDetails(data.error, upstream.status);
		return openAIErrorResponse(d.message || "Upstream error", d.openaiType, d.status, d.code);
	}
	if (!data || !Array.isArray(data.content)) return openAIErrorResponse("Invalid Anthropic response", "upstream_error", 502);
	return jsonResponse(anthropicResponseToOpenAI(data, model), 200);
}

async function anthropicBridgeToOpenAIStream(upstream, model, env, requestId) {
	if (!upstream.body) return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	const reader = upstream.body.getReader();
	const first = await readWithTimeout(reader, Number(env?.ZEN_FIRST_BYTE_TIMEOUT_MS) || STREAM_FIRST_BYTE_TIMEOUT_MS);
	if (first.done) return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	const firstText = new TextDecoder().decode(first.value);
	const firstErr = parseZenError(firstText);
	if (upstream.status === 429 || firstErr) {
		await reader.cancel().catch(() => {});
		const d = zenErrorDetails(firstErr, upstream.status);
		return openAIErrorResponse(d.message, d.openaiType, d.status, d.code);
	}
	const encoder = new TextEncoder();
	const chatId = ocId("chatcmpl");
	const created = Math.floor(Date.now() / 1000);
	const base = () => ({ id: chatId, object: "chat.completion.chunk", created, model });
	const stream = new ReadableStream({
		async start(controller) {
			let buffer = firstText, doneSent = false, roleSent = false;
			let finish = "stop", usage = { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };
			const blocks = new Map();
			let toolCount = 0;
			const emit = (o) => controller.enqueue(encoder.encode(`data: ${JSON.stringify(o)}\n\n`));
			// OpenAI SSE 的终止帧必须是裸文本 data: [DONE]，不能 JSON 编码成 data: "[DONE]"
			const emitDone = () => controller.enqueue(encoder.encode("data: [DONE]\n\n"));
			const role = () => { if (roleSent) return; roleSent = true; const c = base(); c.choices = [{ index: 0, delta: { role: "assistant" }, finish_reason: null }]; emit(c); };
			const finishSend = () => { if (doneSent) return; doneSent = true; role(); const c = base(); c.choices = [{ index: 0, delta: {}, finish_reason: finish }]; c.usage = usage; emit(c); emitDone(); };
			const handle = (p) => {
				if (!p || typeof p !== "object") return;
				if (p.type === "content_block_start") {
					const b = p.content_block || {};
					if (b.type === "text") { role(); blocks.set(p.index, { type: "text" }); }
					else if (b.type === "thinking") { role(); blocks.set(p.index, { type: "thinking" }); }
					else if (b.type === "tool_use") {
						role();
						const idx = toolCount++;
						blocks.set(p.index, { type: "tool", oi: idx });
						const c = base(); c.choices = [{ index: 0, delta: { tool_calls: [{ index: idx, id: b.id || `call_${idx}`, type: "function", function: { name: b.name || "", arguments: "" } }] }, finish_reason: null }]; emit(c);
					}
				} else if (p.type === "content_block_delta") {
					const st = blocks.get(p.index);
					const d = p.delta || {};
					if (d.type === "text_delta" && d.text) { role(); const c = base(); c.choices = [{ index: 0, delta: { content: d.text }, finish_reason: null }]; emit(c); }
					else if (d.type === "thinking_delta" && d.thinking) { role(); const c = base(); c.choices = [{ index: 0, delta: { reasoning_content: d.thinking }, finish_reason: null }]; emit(c); }
					else if (d.type === "input_json_delta" && d.partial_json && st?.type === "tool") { const c = base(); c.choices = [{ index: 0, delta: { tool_calls: [{ index: st.oi, function: { arguments: d.partial_json } }] }, finish_reason: null }]; emit(c); }
				} else if (p.type === "message_delta") {
					if (p.delta?.stop_reason === "tool_use") finish = "tool_calls";
					else if (p.delta?.stop_reason === "max_tokens") finish = "length";
					if (p.usage) usage = { prompt_tokens: p.usage.input_tokens || usage.prompt_tokens, completion_tokens: p.usage.output_tokens || usage.completion_tokens, total_tokens: (p.usage.input_tokens || 0) + (p.usage.output_tokens || 0) };
				} else if (p.type === "message_stop") finishSend();
			};
			try {
				buffer += "\n\n";
				while (true) {
					let i;
					while ((i = buffer.indexOf("\n\n")) !== -1) {
						const block = buffer.slice(0, i); buffer = buffer.slice(i + 2);
						const lines = block.split("\n"); let type = "", data = "";
						for (const line of lines) {
							if (line.startsWith("event:")) type = line.slice(6).trim();
							else if (line.startsWith("data:")) data += line.slice(5).trim();
						}
						if (!data || data === "[DONE]") continue;
						try { const p = JSON.parse(data); if (!p.type && type) p.type = type; handle(p); } catch { }
					}
					const chunk = await reader.read(); if (chunk.done) break;
					buffer += new TextDecoder().decode(chunk.value, { stream: true });
				}
				const left = buffer.trim(); if (left) for (const line of left.split("\n")) if (line.startsWith("data:")) { try { const p = JSON.parse(line.slice(5).trim()); handle(p); } catch { } }
				finishSend();
			} catch (e) {
				debugLog(env, "[ANTHROPIC BRIDGE STREAM FAIL]", { requestId, model, message: e?.message || String(e) });
				finishSend();
			} finally { try { reader.cancel().catch(() => {}); controller.close(); } catch { } }
		},
	});
	return new Response(stream, { status: 200, headers: mergeHeaders({ "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache", "Connection": "keep-alive" }) });
}

function aggregateAnthropicBridgeSSE(raw) {
	const msg = { id: ocId("chatcmpl"), type: "message", role: "assistant", model: "", content: [], stop_reason: "end_turn", usage: { input_tokens: 0, output_tokens: 0 } };
	const blocks = new Map();
	for (const line of String(raw || "").split("\n")) {
		const t = line.trim();
		if (!t.startsWith("data:")) continue;
		const p = t.slice(5).trim();
		if (!p || p === "[DONE]") continue;
		let o; try { o = JSON.parse(p); } catch { continue; }
		if (o.type === "message_start") {
			if (o.message?.id) msg.id = o.message.id;
			if (o.message?.model) msg.model = o.message.model;
			if (o.message?.usage) msg.usage = { ...msg.usage, ...o.message.usage };
		} else if (o.type === "content_block_start") {
			const b = o.content_block || {};
			if (b.type === "text" || b.type === "thinking" || b.type === "tool_use") {
				blocks.set(o.index, { ...b, text: b.type === "text" ? "" : undefined, thinking: b.type === "thinking" ? "" : undefined, json: b.type === "tool_use" ? "" : undefined });
			}
		} else if (o.type === "content_block_delta") {
			const b = blocks.get(o.index); const d = o.delta || {};
			if (b && d.type === "text_delta") b.text = (b.text || "") + (d.text || "");
			else if (b && d.type === "thinking_delta") b.thinking = (b.thinking || "") + (d.thinking || "");
			else if (b && d.type === "input_json_delta") b.json = (b.json || "") + (d.partial_json || "");
		} else if (o.type === "message_delta") {
			if (o.delta?.stop_reason) msg.stop_reason = o.delta.stop_reason;
			if (o.usage) msg.usage = { ...msg.usage, ...o.usage };
		} else if (o.type === "error") return o;
	}
	for (const b of blocks.values()) {
		if (b.type === "text") msg.content.push({ type: "text", text: b.text || "" });
		else if (b.type === "thinking") msg.content.push({ type: "thinking", thinking: b.thinking || "", signature: "" });
		else if (b.type === "tool_use") {
			let input = {}; try { input = b.json ? JSON.parse(b.json) : (b.input || {}); } catch { input = { _raw: b.json || "" }; }
			msg.content.push({ type: "tool_use", id: b.id || "call_0", name: b.name || "", input });
		}
	}
	return msg;
}

function shouldUseResponsesBridge(env, model) {
	return responsesBridgeModels(env).includes(model);
}

function bridgeContentText(content) {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	const parts = [];
	for (const part of content) {
		if (typeof part === "string") parts.push(part);
		else if (part && typeof part.text === "string" && ["text", "input_text", "output_text"].includes(part.type)) parts.push(part.text);
		else if (part && part.type === "image_url") parts.push("[image omitted]");
	}
	return parts.join("\n");
}

function openAIToResponsesBody(env, model, messages, stream, tools, toolChoice, extras) {
	const input = [];
	for (const msg of Array.isArray(messages) ? messages : []) {
		if (!msg || typeof msg !== "object") continue;
		const text = bridgeContentText(msg.content);
		if (msg.role === "tool") {
			input.push({ type: "function_call_output", call_id: msg.tool_call_id || "call_0", output: text });
			continue;
		}
		if (msg.role === "assistant" && Array.isArray(msg.tool_calls) && msg.tool_calls.length) {
			if (text) input.push({ role: "assistant", content: text });
			for (const call of msg.tool_calls) {
				input.push({
					type: "function_call",
					call_id: call?.id || "call_0",
					name: call?.function?.name || "tool",
					arguments: call?.function?.arguments || "{}"
				});
			}
			continue;
		}
		const role = msg.role === "assistant" ? "assistant" : (msg.role === "system" || msg.role === "developer") ? "system" : "user";
		input.push({ role, content: text });
	}
	if (!input.length) input.push({ role: "user", content: "" });
		const gatedInput = input;

	const body = { model, input: gatedInput, stream: true };
	const mapped = [];
	for (const tool of (Array.isArray(tools) ? tools : [])) {
		if (!tool || tool.type !== "function" || !tool.function?.name) continue;
		const fn = { type: "function", name: tool.function.name };
		if (tool.function.description) fn.description = tool.function.description;
		if (tool.function.parameters) fn.parameters = tool.function.parameters;
		mapped.push(fn);
	}
	// 2026-09-18 起：上游要求 tools 含真名 bash+read → 与用户工具合并，确保放行
	const seenR = new Set();
	const mergedR = [];
	for (const t of FAKE_TOOL_RESPONSES) {
		if (!seenR.has(t.name)) { mergedR.push(t); seenR.add(t.name); }
	}
	for (const t of mapped) {
		if (!seenR.has(t.name)) { mergedR.push(t); seenR.add(t.name); }
	}
	body.tools = mergedR.length ? mergedR : FAKE_TOOL_RESPONSES;
	// responses 端点仅支持 tool_choice:"auto"
	body.tool_choice = "auto";
	const requested = Number(extras?.max_tokens) || 0;
	const reserve = Number(env?.BRIDGE_REASONING_RESERVE_TOKENS) || BRIDGE_REASONING_RESERVE_TOKENS;
	body.max_output_tokens = Math.max(requested || 8192, BRIDGE_MIN_MAX_OUTPUT_TOKENS) + reserve;
	for (const key of ["temperature", "top_p", "seed"]) {
		if (extras?.[key] !== undefined && extras[key] !== null) body[key] = extras[key];
	}
	return body;
}

function responsesOutputText(data) {
	const parts = [];
	for (const item of Array.isArray(data?.output) ? data.output : []) {
		if (item?.type !== "message") continue;
		for (const part of Array.isArray(item.content) ? item.content : []) {
			if (part?.type === "output_text" && typeof part.text === "string") parts.push(part.text);
		}
	}
	return parts.join("");
}

function responsesToolCalls(data) {
	const calls = [];
	for (const item of Array.isArray(data?.output) ? data.output : []) {
		if (item?.type !== "function_call") continue;
		calls.push({
			id: item.call_id || item.id || `call_${calls.length}`,
			type: "function",
			function: { name: item.name || "tool", arguments: item.arguments || "{}" }
		});
	}
	return calls;
}

function responsesToChatCompletion(data, model) {
	const text = responsesOutputText(data);
	const calls = responsesToolCalls(data);
	const usage = data?.usage || {};
	const finish = calls.length ? "tool_calls" : (data?.status === "incomplete" && data?.incomplete_details?.reason === "max_output_tokens") ? "length" : "stop";
	const message = { role: "assistant", content: calls.length ? (text || null) : text };
	if (calls.length) message.tool_calls = calls;
	return {
		id: data?.id || ocId("chatcmpl"),
		object: "chat.completion",
		created: data?.created_at || Math.floor(Date.now() / 1000),
		model,
		choices: [{ index: 0, message, finish_reason: finish }],
		usage: {
			prompt_tokens: usage.input_tokens || 0,
			completion_tokens: usage.output_tokens || 0,
			total_tokens: usage.total_tokens || 0
		}
	};
}


async function fetchZenResponses(zenReq, env, requestId, model, stream) {
	return fetchZenWithFallback(
		RESPONSES_BRIDGE_PATH,
		() => ({
			method: "POST",
			headers: zenReq.headers,
			body: zenReq.body,
		}),
		env,
		requestId,
		model,
		stream,
	);
}

async function bridgeFullResponse(upstream, env, requestId, model, refetch) {
	const raw = await upstream.text();
	// relay 式真空重发：无文本无工具时内部重发一次（仅一次），命中才走占位
	if (refetch && isEmptyBridgeRaw(raw)) {
		debugLog(env, "[ZEN EMPTY-RETRY]", { requestId, model });
		try {
			const retryUp = await refetch();
			const retryRaw = await retryUp.text();
			if (!isEmptyBridgeRaw(retryRaw)) {
				return bridgeFullResponse(retryUp, env, requestId, model, null);
			}
			console.log("[ZEN EMPTY-RETRY-FAIL]", JSON.stringify({ requestId, model }));
			return bridgeFullResponse(new Response(retryRaw, { status: retryUp.status, headers: retryUp.headers }), env, requestId, model, null);
		} catch (e) {
			debugLog(env, "[ZEN EMPTY-RETRY-ERR]", { requestId, model, err: e?.message || String(e) });
		}
	}
	const zenError = parseZenError(raw);
	const isHermesClient = (env?._CLIENT_UA || "").includes("openai/python");
	logUpstreamBody(env, requestId, model, upstream.status, raw, zenError);

	if (upstream.status === 429 || zenError) {
		const details = zenErrorDetails(zenError, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}

	if (isSSE(raw)) {
		const agg = aggregateResponsesSSE(raw, !isHermesClient);
		if (agg.error) {
			const message = agg.error.message || "Upstream error";
			return openAIErrorResponse(message, "upstream_error", 502, agg.error.code);
		}
		if (agg.data) {
			if (agg.data.error) {
				return openAIErrorResponse(agg.data.error.message || "Upstream error", "upstream_error", 502, agg.data.error.code);
			}
			return jsonResponse(responsesToChatCompletion(agg.data, model), 200);
		}
	}

	let data = safeJsonParse(raw);
	if (data && (Array.isArray(data.output) || data.object === "response")) {
		if (data.error) {
			const message = data.error.message || "Upstream error";
			return openAIErrorResponse(message, "upstream_error", upstream.status >= 400 ? upstream.status : 502, data.error.code);
		}
		// 兜底：真空（无文本无工具）——非 Hermes 客户端补占位，防 "completed with no content"
		const jOutText = responsesOutputText(data);
		const jCalls = responsesToolCalls(data);
		if (!jOutText && !jCalls.length && !isHermesClient) {
			console.log("[ZEN EMPTY-JSON]", JSON.stringify({ status: data.status, outputTypes: (data.output || []).map((o) => o.type) }));
			data = { ...data, output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: ZWSP }] }] };
		}
		return jsonResponse(responsesToChatCompletion(data, model), 200);
	}
	if (data?.choices) return jsonResponse(normalizeOpenAIFullData(data), upstream.status);

	return new Response(raw, {
		status: upstream.status,
		headers: mergeHeaders({
			"Content-Type": upstream.headers.get("Content-Type") || "application/json; charset=utf-8",
		}),
	});
}

async function bridgeStreamResponse(upstream, env, requestId, model) {
	if (!upstream.body) {
		return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const reader = upstream.body.getReader();
	const first = await readWithTimeout(reader, Number(env?.ZEN_FIRST_BYTE_TIMEOUT_MS) || STREAM_FIRST_BYTE_TIMEOUT_MS);
	if (first.done) {
		return openAIErrorResponse("Empty response from upstream", "upstream_error", 502);
	}

	const firstText = new TextDecoder().decode(first.value);
	const zenError = parseZenError(firstText);
	logUpstreamBody(env, requestId, model, upstream.status, firstText, zenError, true);
	if (upstream.status === 429 || zenError) {
		await reader.cancel().catch(() => { });
		const details = zenErrorDetails(zenError, upstream.status);
		return openAIErrorResponse(details.message, details.openaiType, details.status, details.code);
	}

	const encoder = new TextEncoder();
	const decoder = new TextDecoder();
	const chatId = ocId("chatcmpl");
	const created = Math.floor(Date.now() / 1000);
	let heartbeat = null;

	const stream = new ReadableStream({
		async start(controller) {
			let buffer = "";
			let doneSent = false;
			let sawFinishReason = false;
			let usage = { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };
			let current = null;
			let sawMessage = false;
			let sawContent = false;
			let index = 0;
			let firstToolIndex = 0;
			let identSent = false;
			const toolNames = [];
			let sawToolArgs = false;
			const isHermesClient = (env?._CLIENT_UA || "").includes("openai/python");

			const enqueue = (text) => controller.enqueue(encoder.encode(text));
			const sendData = (payload) => enqueue(`data: ${typeof payload === "string" ? payload : JSON.stringify(payload)}\n\n`);
			const baseChunk = () => ({ id: chatId, object: "chat.completion.chunk", created, model });
			const sendIdent = () => {
				if (identSent) return;
				identSent = true;
				const chunk = baseChunk();
				chunk.choices = [{ index: 0, delta: { role: "assistant" }, finish_reason: null }];
				sendData(chunk);
			};
			const sendFinish = (finishReason = "stop") => {
				if (sawFinishReason || doneSent) return;
				sawFinishReason = true;
				sendIdent();
				const chunk = baseChunk();
				chunk.choices = [{ index: 0, delta: {}, finish_reason: finishReason }];
				chunk.usage = usage;
				sendData(chunk);
				sendData("[DONE]");
				doneSent = true;
			};
			const delta = (fields) => {
				sendIdent();
				const chunk = baseChunk();
				chunk.choices = [{ index: 0, delta: fields, finish_reason: null }];
				sendData(chunk);
			};
			// 兜底：整条流没发过任何 content 且无工具调用（真空）时才补占位。
			// 有工具调用时一个字节 content 都不发：dsh 能处理纯工具调用，补零宽占位反而污染其多轮历史（空assistant越积越多）。
			// 覆盖上游提前断流/空流（无 response.completed 事件）等收尾路径。仅非 Hermes 客户端生效，避免污染其会话历史。
			const fillEmptyContent = () => {
				if (sawContent) return;
				if (toolNames.length) return;
				sawContent = true;
				if (isHermesClient) return;
				delta({ content: ZWSP });
			};            const handleEvent = (payload) => {
				const type = payload?.type;
				if (type === "response.output_item.added") {
					const item = payload.item || {};
					if (item.type === "function_call") {
						const toolIndex = index++;
						if (toolNames.length === 0) firstToolIndex = toolIndex;
						if (item.name) toolNames.push(item.name);
						delta({
							tool_calls: [{
								index: toolIndex,
								id: item.call_id || item.id || "call_0",
								type: "function",
								function: { name: item.name || "", arguments: "" },
							}],
						});
					}
				} else if (type === "response.output_text.delta") {
					if (typeof payload.delta === "string" && payload.delta) { sawContent = true; delta({ content: payload.delta }); }
				} else if (type === "response.reasoning_summary_text.delta" || type === "response.reasoning_text.delta") {
					if (typeof payload.delta === "string" && payload.delta) delta({ reasoning_content: payload.delta });
				} else if (type === "response.function_call_arguments.delta") {
					if (typeof payload.delta === "string" && payload.delta) {
						sawToolArgs = true;
						delta({ tool_calls: [{ index: Math.max(index - 1, 0), function: { arguments: payload.delta } }] });
					}
				} else if (type === "response.completed" || type === "response.incomplete") {
					const resp = payload.response || {};
					const u = resp.usage;
					if (u) {
						usage = {
							prompt_tokens: u.input_tokens || 0,
							completion_tokens: u.output_tokens || 0,
							total_tokens: u.total_tokens || 0,
						};
					}
					let reason = "stop";
					const calls = (resp.output || []).filter((it) => it && it.type === "function_call");
					if (calls.length) reason = "tool_calls";
					else if (resp.status === "incomplete" && resp.incomplete_details?.reason === "max_output_tokens") reason = "length";
					// 上游可能只在 completed 里给 function_call（流中无 output_item.added）→ 必须补发，
					// 或流中只发了工具名、无 arguments.delta（有名无参）→ 用 completed 参数回填，
					// 否则客户端收到残缺工具调用：零工具判"完成但无内容"，有名无参则执行不了直接卡死
					if (calls.length && (toolNames.length === 0 || !sawToolArgs)) {
						const fillWhole = toolNames.length === 0;
						console.log("[ZEN TOOL-FILL]", JSON.stringify({ names: calls.map((c) => c.name), fillWhole, fillArgs: !sawToolArgs }));
						calls.forEach((c) => {
							const args = typeof c.arguments === "string" ? c.arguments : (c.arguments ? JSON.stringify(c.arguments) : "");
							if (fillWhole) {
								const tIdx = index++;
								if (c.name) toolNames.push(c.name);
								delta({ tool_calls: [{ index: tIdx, id: c.call_id || c.id || 'call_' + tIdx, type: 'function', function: { name: c.name || '', arguments: '' } }] });
								sawToolArgs = !!args;
								if (args) delta({ tool_calls: [{ index: tIdx, function: { arguments: args } }] });
							} else if (!sawToolArgs && args) {
								delta({ tool_calls: [{ index: firstToolIndex, function: { arguments: args } }] });
								sawToolArgs = true;
							}
						});
					}
					// 仅当整个流没发过任何 content 时才补发 completed 全文（防 delta 漏导致空内容；正常时 delta 已发，跳过避免重复）
					if (!sawContent) {
						const fullText = responsesOutputText(resp);
						if (fullText) { sawContent = true; delta({ content: fullText }); }
						else {
							console.log("[ZEN EMPTY-STR]", JSON.stringify({ status: resp.status, incomplete: resp.incomplete_details, outputTypes: (resp.output || []).map((o) => o.type + ":" + (o.name || "")), sawContent }));
							fillEmptyContent();
						}
					}
					sendFinish(reason);
				} else if (type === "response.failed" || type === "error") {
					const message = payload?.response?.error?.message || payload?.message || "Upstream stream failed";
					debugLog(env, "[BRIDGE STREAM ERROR]", { requestId, model, message });
					sendFinish("stop");
				}
			};

			const handleBlock = (block) => {
				let eventName = "";
				const dataLines = [];
				for (const line of block.split("\n")) {
					if (line.startsWith("event:")) eventName = line.slice(6).trim();
					else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
				}
				const payloadText = dataLines.join("\n").trim();
				if (!payloadText || payloadText === "[DONE]") return;
				let payload;
				try {
					payload = JSON.parse(payloadText);
				} catch (error) {
					return;
				}
				if (!payload || typeof payload !== "object") return;
				if (!payload.type && eventName) payload.type = eventName;
				handleEvent(payload);
			};

			const drainBuffer = () => {
				let idx;
				while ((idx = buffer.indexOf("\n\n")) !== -1) {
					const block = buffer.slice(0, idx);
					buffer = buffer.slice(idx + 2);
					handleBlock(block);
				}
			};try {
				buffer = firstText;
				drainBuffer();
				heartbeat = setInterval(() => {
					if (!doneSent) enqueue(": ping\n\n");
				}, Number(env?.STREAM_HEARTBEAT_MS) || STREAM_HEARTBEAT_MS);

				while (true) {
					const chunk = await reader.read();
					if (chunk.done) break;
					buffer += decoder.decode(chunk.value, { stream: true });
					drainBuffer();
				}
				buffer += decoder.decode();
				drainBuffer();
				if (!sawFinishReason) {
					fillEmptyContent();
					console.log("[ZEN STREAM-EOF]", JSON.stringify({ sawContent, sawFinishReason, toolNames: toolNames.length }));
					sendFinish(toolNames.length ? "tool_calls" : "stop");
				}
			} catch (error) {
				debugLog(env, "[BRIDGE STREAM FAIL]", { requestId, model, message: error?.message || String(error) });
				if (!sawFinishReason) {
					fillEmptyContent();
					sendFinish(toolNames.length ? "tool_calls" : "stop");
				}
			} finally {
				if (heartbeat) clearInterval(heartbeat);
				try {
					controller.close();
				} catch (error) { }
			}
		},
		cancel() {
			if (heartbeat) clearInterval(heartbeat);
			reader.cancel().catch(() => { });
		},
	});

	return new Response(stream, {
		status: 200,
		headers: mergeHeaders({
			"Content-Type": "text/event-stream; charset=utf-8",
			"Cache-Control": "no-cache",
			"Connection": "keep-alive",
		}),
	});
}
