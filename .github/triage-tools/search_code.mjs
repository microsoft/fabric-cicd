// Tool skill: search_code
// Grounds the assessment in the ACTUAL fabric-cicd source, not just the condensed reference.
// Given the issue's signals (exception types, verified symbols, keywords), it greps
// src/fabric_cicd/**/*.py and returns concrete pointers — file, line, a one-line snippet, and a
// GitHub blob permalink — so the triage comment can say *why* it reached its conclusion and link
// the reader straight to the relevant code.
//
// Standalone:  node .github/triage-tools/search_code.mjs --signals="ItemDependencyError,DataPipeline"
// Importable:  import { searchCode } from "./search_code.mjs";

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { printJson, isMain, getRepo } from "./lib/gh.mjs";

function resolveSrcRoot() {
    const fromArg = process.argv.find((a) => a.startsWith("--src="));
    if (fromArg) return fromArg.slice("--src=".length);
    if (process.env.FABRIC_SRC_ROOT) return process.env.FABRIC_SRC_ROOT;
    return join(process.cwd(), "src", "fabric_cicd");
}

function blobBase() {
    const server = process.env.GITHUB_SERVER_URL || "https://github.com";
    const { owner, repo } = getRepo();
    // Prefer the exact commit for a durable permalink; fall back to the default branch.
    const ref = process.env.GITHUB_SHA || "main";
    return { server, owner, repo, ref };
}

function walkPy(dir, acc = [], depth = 0) {
    if (depth > 8) return acc;
    let entries = [];
    try {
        entries = readdirSync(dir);
    } catch {
        return acc;
    }
    for (const name of entries) {
        if (name === "__pycache__" || name.startsWith(".")) continue;
        const full = join(dir, name);
        let st;
        try {
            st = statSync(full);
        } catch {
            continue;
        }
        if (st.isDirectory()) walkPy(full, acc, depth + 1);
        else if (name.endsWith(".py")) acc.push(full);
    }
    return acc;
}

// signals: { strong: string[], keywords: string[] }. strong = exception types / canonical symbols
// (matched verbatim); keywords = plain words (matched as whole-word, case-insensitive).
export function searchCode(signals = {}, { srcRoot = resolveSrcRoot(), limit = 6, perFile = 2 } = {}) {
    const strong = [...new Set(signals.strong || [])].filter((s) => String(s).length > 2);
    const keywords = [...new Set(signals.keywords || [])].filter((k) => String(k).length > 3);
    if (!strong.length && !keywords.length) return { pointers: [], searched: 0, src_root: srcRoot };

    const files = walkPy(srcRoot);
    const { server, owner, repo, ref } = blobBase();

    const strongRes = strong.map((s) => ({ term: s, re: new RegExp(s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")) }));
    const kwRes = keywords.map((k) => ({ term: k, re: new RegExp(`\\b${k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i") }));

    const perFileHits = [];
    for (const file of files) {
        let content;
        try {
            content = readFileSync(file, "utf8");
        } catch {
            continue;
        }
        const lines = content.split(/\r?\n/);
        const matchedTerms = new Set();
        const hits = [];
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            if (line.length > 400) continue;
            let strongOnLine = false;
            for (const { term, re } of strongRes) {
                if (re.test(line)) {
                    matchedTerms.add(term);
                    strongOnLine = true;
                }
            }
            let kwOnLine = false;
            for (const { term, re } of kwRes) {
                if (re.test(line)) {
                    matchedTerms.add(term);
                    kwOnLine = true;
                }
            }
            if (strongOnLine || kwOnLine) {
                // Capture a small window around the match so the comment can quote the ACTUAL code
                // (e.g. an invalid call / offending line), not just link to it. Kept tight so the
                // evidence bundle stays small; each line is length-capped.
                const from = Math.max(0, i - 2);
                const to = Math.min(lines.length - 1, i + 2);
                const code = [];
                for (let j = from; j <= to; j++) {
                    code.push({ n: j + 1, text: lines[j].replace(/\t/g, "    ").slice(0, 200), hit: j === i });
                }
                hits.push({ line: i + 1, snippet: line.trim().slice(0, 160), strong: strongOnLine, code });
            }
        }
        if (!hits.length) continue;
        // Prefer definition-ish lines (class/def/raise/=) over incidental mentions.
        hits.sort((a, b) => {
            const defA = /\b(class|def|raise)\b/.test(a.snippet) ? 1 : 0;
            const defB = /\b(class|def|raise)\b/.test(b.snippet) ? 1 : 0;
            return b.strong - a.strong || defB - defA || a.line - b.line;
        });
        const rel = relative(process.cwd(), file).split(sep).join("/");
        for (const h of hits.slice(0, perFile)) {
            perFileHits.push({
                file: rel,
                line: h.line,
                snippet: h.snippet,
                code: h.code || [],
                strong: h.strong,
                matched: [...matchedTerms],
                distinct: matchedTerms.size,
                blob_url: `${server}/${owner}/${repo}/blob/${ref}/${rel}#L${h.line}`,
            });
        }
    }

    // Rank: files matching more distinct signals first, strong matches ahead of keyword-only.
    perFileHits.sort((a, b) => b.distinct - a.distinct || b.strong - a.strong || a.file.localeCompare(b.file));
    return { pointers: perFileHits.slice(0, limit), searched: files.length, src_root: srcRoot };
}

if (isMain(import.meta.url)) {
    const hit = process.argv.find((a) => a.startsWith("--signals="));
    const raw = (hit ? hit.slice("--signals=".length) : process.env.SIGNALS || "").split(",").map((s) => s.trim()).filter(Boolean);
    const strong = raw.filter((s) => /[A-Z]/.test(s) || s.includes("_"));
    const keywords = raw.filter((s) => !strong.includes(s));
    printJson(searchCode({ strong, keywords }));
}
