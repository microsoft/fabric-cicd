// Shared helpers for fabric-cicd triage tool skills.
//
// Dependency-free (Node 20+ global fetch only) so the pure tools that import this module
// stay runnable offline with `node <tool>.mjs`. Model-calling helpers here are only exercised
// in CI where GITHUB_TOKEN + `models: read` are available.
//
// First-party GitHub Models only — inference goes to https://models.github.ai/inference.

const MODELS_ENDPOINT = process.env.GITHUB_MODELS_ENDPOINT || "https://models.github.ai/inference";
const GITHUB_API = process.env.GITHUB_API_URL || "https://api.github.com";

// Phase 6 — model tiering. Each tier lists primary first, then 429/5xx fallbacks.
export const MODEL_TIERS = {
    fast: ["openai/gpt-4.1-mini", "microsoft/phi-4"],
    // Reasoning tier uses GPT-4.1 (large context, accepts standard params) with GPT-4o as fallback.
    // Earlier choices caused real failures: openai/o4-mini (an o-series model) rejects `max_tokens`
    // (needs `max_completion_tokens`), and its silent fallback deepseek/deepseek-r1 caps requests at
    // 4000 tokens, so real triage inputs overflowed it with a 413 tokens_limit_reached.
    reasoning: ["openai/gpt-4.1", "openai/gpt-4o"],
    mid: ["openai/gpt-4.1", "openai/gpt-4o"],
    embeddings: ["openai/text-embedding-3-small"],
};

// Which tier each skill/step runs on (mirrors the `model:` pinned in each *.prompt.yml).
export const SKILL_TIERS = {
    classify: "fast",
    severity: "fast",
    critique: "fast",
    "bug-analyze": "reasoning",
    orchestrator: "reasoning",
    answer: "mid",
    "misconfig-resolve": "mid",
    "comment-draft": "mid",
};

export function getToken() {
    return process.env.GITHUB_TOKEN || process.env.GH_TOKEN || "";
}

export function getRepo() {
    // GITHUB_REPOSITORY is "owner/repo" in Actions.
    const full = process.env.GITHUB_REPOSITORY || "";
    const [owner, repo] = full.split("/");
    return { owner: owner || "microsoft", repo: repo || "fabric-cicd" };
}

// Minimal GitHub REST GET returning parsed JSON. Never throws on non-2xx — returns null so
// tools degrade gracefully in restricted environments.
export async function ghRest(path, { params } = {}) {
    const token = getToken();
    const url = new URL(path.startsWith("http") ? path : `${GITHUB_API}${path}`);
    if (params) {
        for (const [k, v] of Object.entries(params)) {
            if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
        }
    }
    const headers = {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "fabric-cicd-triage",
    };
    if (token) headers.Authorization = `Bearer ${token}`;
    try {
        const res = await fetch(url, { headers });
        if (!res.ok) return null;
        return await res.json();
    } catch {
        return null;
    }
}

// Call a GitHub Model. `tier` may be a tier name (see MODEL_TIERS) or an explicit array of
// model slugs. Falls back through the list on 413/429/5xx (payload-too-large, rate limit, server
// error) and retries a model once on a network hang / timeout before moving on. Returns the
// assistant text content.
export async function callModel(tier, messages, opts = {}) {
    const token = getToken();
    if (!token) {
        const err = new Error("No GITHUB_TOKEN available for GitHub Models inference.");
        err.code = "NO_TOKEN";
        throw err;
    }
    const models = Array.isArray(tier) ? tier : MODEL_TIERS[tier] || MODEL_TIERS.mid;
    const { maxTokens = 1000, temperature, jsonObject = false } = opts;
    // Per-attempt hard timeout so a hung connection can't stall the whole job (undici's default
    // headers timeout is ~5 min, which previously hard-failed runs with UND_ERR_HEADERS_TIMEOUT).
    const timeoutMs = Number(process.env.TRIAGE_MODEL_TIMEOUT_MS) || 90000;

    let lastErr;
    for (const model of models) {
        const body = { model, messages, max_tokens: maxTokens };
        if (typeof temperature === "number") body.temperature = temperature;
        if (jsonObject) body.response_format = { type: "json_object" };

        // Give each model one retry on a transient network hang before falling back.
        for (let attempt = 0; attempt < 2; attempt++) {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeoutMs);
            let res;
            try {
                res = await fetch(`${MODELS_ENDPOINT}/chat/completions`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${token}`,
                        "Content-Type": "application/json",
                        Accept: "application/json",
                        "User-Agent": "fabric-cicd-triage",
                    },
                    body: JSON.stringify(body),
                    signal: controller.signal,
                });
            } catch (err) {
                // Network hang / abort / DNS blip — retry the same model once, then fall through.
                const transient =
                    err.name === "AbortError" ||
                    err.code === "UND_ERR_HEADERS_TIMEOUT" ||
                    /fetch failed|timeout|network|socket|ECONN/i.test(err.message || "");
                lastErr =
                    err.name === "AbortError"
                        ? new Error(`Model ${model} timed out after ${timeoutMs}ms; trying fallback.`)
                        : err;
                if (transient && attempt === 0) continue; // retry same model
                break; // give up on this model → next in the tier
            } finally {
                clearTimeout(timer);
            }

            // 413 (payload/token limit), 429 (rate limit), 5xx (server) → try the next model.
            if (res.status === 413 || res.status === 429 || res.status >= 500) {
                lastErr = new Error(`Model ${model} returned ${res.status}; trying fallback.`);
                break; // next model — retrying the same one won't help
            }
            if (!res.ok) {
                const text = await res.text();
                // Some models (OpenAI o-series) reject `max_tokens` and require `max_completion_tokens`.
                // Swap the param on the body and retry the same model once instead of hard-failing.
                if (res.status === 400 && /max_completion_tokens/i.test(text) && "max_tokens" in body) {
                    body.max_completion_tokens = body.max_tokens;
                    delete body.max_tokens;
                    lastErr = new Error(`Model ${model} needs max_completion_tokens; retrying.`);
                    if (attempt === 0) continue; // retry same model with the corrected param
                    break; // already retried → next model
                }
                throw new Error(`Model ${model} failed (${res.status}): ${text.slice(0, 300)}`);
            }
            const data = await res.json();
            const content = data?.choices?.[0]?.message?.content ?? "";
            return { model, content };
        }
    }
    throw lastErr || new Error("All models in tier failed.");
}

// Extract the first well-formed JSON object from a model response (tolerates code fences and
// leading/trailing prose).
export function extractJson(text) {
    if (!text) return null;
    const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
    const candidate = fenced ? fenced[1] : text;
    const start = candidate.indexOf("{");
    const end = candidate.lastIndexOf("}");
    if (start === -1 || end === -1 || end < start) return null;
    const slice = candidate.slice(start, end + 1);
    try {
        return JSON.parse(slice);
    } catch {
        return null;
    }
}

// Read a single string input for a standalone tool: prefer --key=value / positional argv,
// then the named env var, then stdin.
export async function readInput({ argName, envName } = {}) {
    const argv = process.argv.slice(2);
    if (argName) {
        const flag = `--${argName}=`;
        const hit = argv.find((a) => a.startsWith(flag));
        if (hit) return hit.slice(flag.length);
    }
    const positional = argv.find((a) => !a.startsWith("--"));
    if (positional) return positional;
    if (envName && process.env[envName]) return process.env[envName];
    // Fall back to stdin (non-blocking best-effort).
    if (!process.stdin.isTTY) {
        const chunks = [];
        for await (const chunk of process.stdin) chunks.push(chunk);
        const s = Buffer.concat(chunks).toString("utf8").trim();
        if (s) return s;
    }
    return "";
}

export function printJson(obj) {
    process.stdout.write(JSON.stringify(obj, null, 2) + "\n");
}

// True when this module file is the process entry point (ESM equivalent of require.main).
export function isMain(importMetaUrl) {
    const entry = process.argv[1] ? new URL(`file://${process.argv[1]}`).href : "";
    return importMetaUrl === entry;
}
