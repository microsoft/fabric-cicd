// Orchestrator: the agentic triage pipeline.
//
// Runs a deterministic, tool-grounded pipeline over a single issue and emits one decision JSON
// object (the Phase-1 contract) that the workflow consumes to label / comment / close.
//
//   classify ──► tools (parse_error_log, verify_symbol, check_version, search_issues)
//            └─► branch: bug-analyze | answer | misconfig-resolve | (feature/dup/spam: none)
//            └─► comment-draft ──► critique (one reflection pass) ──► decision JSON
//
// First-party GitHub Models only (via lib/gh.mjs callModel). Degrades gracefully with no token
// (skips model stages, still runs offline tools) so the pipeline is testable locally:
//
//   ISSUE_TITLE="..." ISSUE_BODY="..." node .github/triage-tools/orchestrate.mjs --self-test
//
// Output: pretty JSON to stdout, and to $DECISION_OUT / --out=<path> if provided.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { callModel, extractJson, isMain, printJson, getToken, SKILL_TIERS, MODEL_TIERS } from "./lib/gh.mjs";
import { parseErrorLog } from "./parse_error_log.mjs";
import { verifySymbol } from "./verify_symbol.mjs";
import { checkVersion } from "./check_version.mjs";
import { searchIssues } from "./search_issues.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(HERE, "..", "..");
const SKILLS_DIR = join(REPO_ROOT, ".github", "prompts", "skills");
const CONTEXT_PATH = join(REPO_ROOT, ".github", "prompts", "_context.md");

let YAML = null;
async function getYaml() {
    if (YAML) return YAML;
    try {
        YAML = (await import("js-yaml")).default;
    } catch {
        // Minimal fallback is not attempted — js-yaml is a declared dependency installed in CI.
        throw new Error("js-yaml is required. Run `npm install --prefix .github/triage-tools`.");
    }
    return YAML;
}

function loadContext() {
    try {
        return readFileSync(CONTEXT_PATH, "utf8");
    } catch {
        return "";
    }
}

async function loadSkill(name) {
    const yaml = await getYaml();
    const doc = yaml.load(readFileSync(join(SKILLS_DIR, `${name}.prompt.yml`), "utf8"));
    return {
        name,
        messages: doc.messages || [],
        model: doc.model,
        params: doc.modelParameters || {},
        tier: SKILL_TIERS[name] || "mid",
    };
}

function fillTemplate(text, vars) {
    return String(text).replace(/\{\{\s*(\w+)\s*\}\}/g, (m, k) => (k in vars ? String(vars[k]) : m));
}

function renderMessages(skill, vars) {
    return skill.messages.map((msg) => ({ role: msg.role, content: fillTemplate(msg.content, vars) }));
}

// Build the model list for a skill: an explicitly pinned `model:` first, then its tier's
// remaining models as 429/5xx fallbacks.
function tierFor(skill) {
    const fallbacks = MODEL_TIERS[skill.tier] || [];
    if (skill.model) return [skill.model, ...fallbacks.filter((m) => m !== skill.model)];
    return skill.tier;
}

// Run one skill through GitHub Models. Returns { raw, json, model } or null when no token.
async function runSkill(skill, vars, { json = true } = {}) {
    if (!getToken()) return null;
    const messages = renderMessages(skill, vars);
    const { model, content } = await callModel(tierFor(skill), messages, {
        maxTokens: skill.params.max_tokens || 1000,
        temperature: skill.params.temperature,
        jsonObject: json,
    });
    return { raw: content, json: json ? extractJson(content) : null, model };
}

// ---- Signal extraction (drives which tools run) -------------------------------------------

function extractSymbols(text) {
    const s = String(text || "");
    const camel = s.match(/\b[A-Z][a-z]+(?:[A-Z][a-z0-9]+)+\b/g) || []; // DataPipeline, CopyJob…
    const flags = s.match(/\benable_[a-z0-9_]+\b/g) || []; // enable_bulk_publish…
    return [...new Set([...camel, ...flags])].slice(0, 12);
}

// ---- Resolution → workflow signals --------------------------------------------------------

function signalsFor(resolution, category, severity) {
    const r = resolution || "none";
    const needs_author_feedback = r === "needs-info";
    const add_help_wanted = r === "community";
    const needs_human_review =
        r === "potential-bug" || r === "escalate" || severity === "critical" || (category === "bug" && r !== "duplicate");
    const can_auto_close = ["answered", "misconfiguration", "redirect-docs", "duplicate"].includes(r);
    return { needs_author_feedback, add_help_wanted, needs_human_review, can_auto_close };
}

function labelsFor(category, resolution, addHelpWanted) {
    const labels = new Set();
    if (category === "bug") labels.add("bug");
    else if (category === "feature") labels.add("enhancement");
    else if (category === "question") labels.add("question");
    else if (category === "duplicate" || resolution === "duplicate") labels.add("duplicate");
    if (addHelpWanted) labels.add("help wanted");
    return [...labels];
}

// ---- Pipeline -----------------------------------------------------------------------------

export async function orchestrate(issue, { confidenceThreshold = 0.85 } = {}) {
    const context = loadContext();
    const composite = `${issue.title || ""}\n\n${issue.body || ""}`.trim();
    const stages = [];
    const modelsUsed = [];
    const record = (name, res) => {
        stages.push(name);
        if (res?.model) modelsUsed.push(`${name}:${res.model}`);
    };

    // 1) Classify
    const classifySkill = await loadSkill("classify");
    let classification = { category: "needs-info", confidence: 0, severity: "normal" };
    const cls = await runSkill(classifySkill, { input: composite });
    record("classify", cls);
    if (cls?.json) classification = { ...classification, ...cls.json };

    // 2) Tools (offline-capable; always run for grounding)
    const errorLog = parseErrorLog(composite);
    const version = await checkVersion(composite);
    const symbolNames = extractSymbols(composite);
    const symbols = symbolNames.map((n) => verifySymbol(n)).filter((s) => s.kind || s.suggestions.length);
    const dupes = await searchIssues(composite, { excludeNumber: issue.number });
    const toolFindings = { error_log: errorLog, version, symbols, duplicates: dupes };

    const toolSummary = JSON.stringify({
        error_log: { primary_exception: errorLog.primary_exception, http_status: errorLog.http_status },
        version: { reported: version.reported_version, latest: version.latest_version, is_stale: version.is_stale },
        symbols: symbols.map((s) => ({ name: s.name, exists: s.exists, kind: s.kind, suggestions: s.suggestions })),
        likely_duplicate_of: dupes.likely_duplicate_of,
    });

    // 3) Branch analysis
    const analysisInput = [
        `## Issue`,
        composite,
        ``,
        `## Tool findings`,
        toolSummary,
        ``,
        `## Codebase reference`,
        context,
    ].join("\n");

    let analysis = null;
    let analysisModel = null;
    const category = classification.category;
    if (category === "bug") {
        const skill = await loadSkill("bug-analyze");
        const res = await runSkill(skill, { input: analysisInput });
        record("bug-analyze", res);
        analysis = res?.json || null;
        analysisModel = res?.model;
    } else if (category === "question") {
        const skill = await loadSkill("answer");
        const res = await runSkill(skill, { input: analysisInput });
        record("answer", res);
        analysis = res?.json || null;
        analysisModel = res?.model;
    } else if (category === "needs-info" || category === "duplicate") {
        // Misconfiguration often masquerades as a bug report or thin question — try to resolve.
        const skill = await loadSkill("misconfig-resolve");
        const res = await runSkill(skill, { input: analysisInput });
        record("misconfig-resolve", res);
        analysis = res?.json || null;
        analysisModel = res?.model;
    }
    void analysisModel;

    // 4) Derive a resolution from category + analysis + tools
    let resolution = deriveResolution(category, classification, analysis, toolFindings);

    // 5) Draft comment
    const draftSkill = await loadSkill("comment-draft");
    const bundle = {
        category,
        confidence: classification.confidence,
        severity: classification.severity,
        resolution,
        issue_author: issue.author || "",
        analysis,
        tool_findings: {
            primary_exception: errorLog.primary_exception,
            http_status: errorLog.http_status,
            version_is_stale: version.is_stale,
            latest_version: version.latest_version,
            likely_duplicate_of: dupes.likely_duplicate_of,
            unknown_symbols: symbols.filter((s) => !s.exists).map((s) => ({ name: s.name, suggestions: s.suggestions })),
        },
    };
    const draft = await runSkill(draftSkill, { input: JSON.stringify(bundle), issue_author: issue.author || "" });
    record("comment-draft", draft);
    let commentMarkdown =
        unwrapComment(draft?.json?.comment_markdown) ||
        unwrapComment(draft?.raw) ||
        fallbackComment(category, resolution, issue);

    // 6) Critique (one reflection pass) — only refine, never expand scope
    const critiqueSkill = await loadSkill("critique");
    const critique = await runSkill(critiqueSkill, { input: commentMarkdown }, { json: false });
    record("critique", critique);
    const refined = unwrapComment(critique?.raw);
    if (refined && refined.trim().startsWith("###")) {
        commentMarkdown = refined.trim();
    }

    // 7) Assemble decision + confidence gating
    const sig = signalsFor(resolution, category, classification.severity);
    const confident = (classification.confidence || 0) >= confidenceThreshold;
    const can_auto_close = sig.can_auto_close && confident && !sig.needs_human_review && !sig.needs_author_feedback && !sig.add_help_wanted;

    return {
        category,
        confidence: classification.confidence || 0,
        severity: classification.severity || "normal",
        resolution,
        is_duplicate_of: dupes.likely_duplicate_of,
        labels: labelsFor(category, resolution, sig.add_help_wanted),
        needs_human_review: sig.needs_human_review,
        needs_author_feedback: sig.needs_author_feedback,
        add_help_wanted: sig.add_help_wanted,
        can_auto_close,
        confidence_threshold: confidenceThreshold,
        comment_markdown: commentMarkdown,
        degraded: !getToken(),
        stages,
        models_used: modelsUsed,
        tool_findings: toolFindings,
    };
}

function deriveResolution(category, classification, analysis, tools) {
    if (tools.duplicates?.likely_duplicate_of) return "duplicate";
    if (category === "question") {
        if (analysis?.resolution) return analysis.resolution; // answered | redirect-docs | needs-info
        return analysis ? "answered" : "needs-info";
    }
    if (category === "feature") return analysis?.scope === "community" ? "community" : "backlog";
    if (category === "bug") {
        if (analysis?.likely_misconfiguration) return "misconfiguration";
        return "potential-bug";
    }
    if (category === "needs-info") {
        if (analysis?.likely_misconfiguration || analysis?.fix) return "misconfiguration";
        return "needs-info";
    }
    if (category === "spam") return "none";
    return "none";
}

// Unwrap a comment that may arrive as raw markdown, a JSON string, or a {comment_markdown:...}
// object (models sometimes double-wrap). Returns clean markdown or "" if not usable.
function unwrapComment(value) {
    let v = value;
    for (let i = 0; i < 3; i++) {
        if (v == null) return "";
        if (typeof v === "object") {
            if (typeof v.comment_markdown === "string") {
                v = v.comment_markdown;
                continue;
            }
            return "";
        }
        const trimmed = String(v).trim();
        if (trimmed.startsWith("{") && trimmed.includes("comment_markdown")) {
            const parsed = extractJson(trimmed);
            if (parsed && typeof parsed.comment_markdown === "string") {
                v = parsed.comment_markdown;
                continue;
            }
        }
        return trimmed;
    }
    return typeof v === "string" ? v.trim() : "";
}

function fallbackComment(category, resolution, issue) {
    const label =
        category === "bug"
            ? "Potential Bug"
            : category === "feature"
              ? "Valuable Enhancement"
              : category === "question"
                ? "Question"
                : "Needs Author Feedback";
    return [
        `### AI Assessment: ${label}`,
        ``,
        `This issue has been triaged automatically. A maintainer will review it.`,
        ``,
        resolution === "needs-info"
            ? `**⏳ Awaiting author feedback** — @${issue.author || "author"}, please provide additional details.`
            : `**🔔 Escalated to team** — This issue requires team review and has been flagged for attention.`,
        ``,
        `---`,
        `> 💡 If this issue requires the team's attention and was not escalated, you can tag @microsoft/fabric-cicd to notify the team.`,
    ].join("\n");
}

// ---- Entry point --------------------------------------------------------------------------

function readIssueFromEnv() {
    const fileArg = process.argv.find((a) => a.startsWith("--issue-file="));
    if (fileArg) {
        return JSON.parse(readFileSync(fileArg.slice("--issue-file=".length), "utf8"));
    }
    return {
        number: process.env.ISSUE_NUMBER ? parseInt(process.env.ISSUE_NUMBER, 10) : undefined,
        title: process.env.ISSUE_TITLE || "",
        body: process.env.ISSUE_BODY || "",
        author: process.env.ISSUE_AUTHOR || "",
    };
}

if (isMain(import.meta.url)) {
    const selfTest = process.argv.includes("--self-test");
    let issue = readIssueFromEnv();
    if (selfTest && !issue.body) {
        issue = {
            number: 999,
            title: "DataPipeline fails with PublishError: dependency not published",
            body: readFileSync(join(HERE, "__fixtures__", "issue-bug.md"), "utf8"),
            author: "octocat",
        };
        if (!process.env.TRIAGE_ISSUES_FIXTURE) {
            process.env.TRIAGE_ISSUES_FIXTURE = join(HERE, "__fixtures__", "issues.json");
        }
        if (!process.env.FABRIC_LATEST_VERSION) process.env.FABRIC_LATEST_VERSION = "1.2.0";
    }
    const decision = await orchestrate(issue);
    printJson(decision);
    const outArg = process.argv.find((a) => a.startsWith("--out="));
    const outPath = outArg ? outArg.slice("--out=".length) : process.env.DECISION_OUT;
    if (outPath) writeFileSync(outPath, JSON.stringify(decision, null, 2) + "\n");
}
