// Tool skill: search_issues
// Finds the most similar existing issues (open + closed) to support duplicate detection and
// "already fixed in vX.Y" checks. Returns the top-5 candidates with a similarity score.
//
// Candidate source: TRIAGE_ISSUES_FIXTURE (path to a JSON array) when set — used for offline
// tests — otherwise the GitHub Search API for the current repo. Similarity is a lightweight
// token-overlap (Dice) score computed locally; no embeddings dependency required.
//
// Standalone:  node .github/triage-tools/search_issues.mjs --query="DataPipeline PublishError dependency"
// Importable:  import { searchIssues, similarity } from "./search_issues.mjs";

import { readFileSync } from "node:fs";
import { readInput, printJson, isMain, ghRest, getRepo } from "./lib/gh.mjs";

const STOP = new Set(
    "the a an and or of to in is on for with as at by be this that it i you we my your our error issue bug when then from into using use get got not no yes fabric cicd".split(
        " "
    )
);

function tokenize(text) {
    return new Set(
        String(text || "")
            .toLowerCase()
            .replace(/[^a-z0-9_]+/g, " ")
            .split(/\s+/)
            .filter((t) => t.length > 2 && !STOP.has(t))
    );
}

// Sørensen–Dice coefficient over token sets (0..1).
export function similarity(a, b) {
    const ta = tokenize(a);
    const tb = tokenize(b);
    if (ta.size === 0 || tb.size === 0) return 0;
    let inter = 0;
    for (const t of ta) if (tb.has(t)) inter++;
    return (2 * inter) / (ta.size + tb.size);
}

function loadFixture() {
    const path = process.env.TRIAGE_ISSUES_FIXTURE;
    if (!path) return null;
    try {
        return JSON.parse(readFileSync(path, "utf8"));
    } catch {
        return null;
    }
}

// Fetch candidate issues from the GitHub Search API using the top keywords of the query.
async function fetchCandidates(query, { excludeNumber } = {}) {
    const { owner, repo } = getRepo();
    const keywords = [...tokenize(query)].slice(0, 6).join(" ");
    const q = `repo:${owner}/${repo} is:issue ${keywords}`.trim();
    const data = await ghRest("/search/issues", { params: { q, per_page: 20, sort: "updated", order: "desc" } });
    const items = data?.items || [];
    return items
        .filter((it) => !it.pull_request && it.number !== excludeNumber)
        .map((it) => ({
            number: it.number,
            title: it.title,
            body: it.body || "",
            state: it.state,
            html_url: it.html_url,
            labels: (it.labels || []).map((l) => (typeof l === "string" ? l : l.name)),
        }));
}

export async function searchIssues(query, { excludeNumber, limit = 5, threshold = 0.2 } = {}) {
    const fixture = loadFixture();
    const candidates = fixture
        ? fixture.filter((c) => c.number !== excludeNumber)
        : await fetchCandidates(query, { excludeNumber });

    const scored = candidates
        .map((c) => ({
            number: c.number,
            title: c.title,
            state: c.state,
            html_url: c.html_url,
            labels: c.labels || [],
            similarity: Number(similarity(query, `${c.title} ${c.body}`).toFixed(3)),
        }))
        .sort((x, y) => y.similarity - x.similarity)
        .slice(0, limit);

    const best = scored[0] || null;
    const likelyDuplicate = best && best.similarity >= 0.45 ? best : null;

    return {
        query,
        source: fixture ? "fixture" : "github-search",
        candidates: scored.filter((s) => s.similarity >= threshold),
        likely_duplicate_of: likelyDuplicate ? likelyDuplicate.number : null,
        likely_duplicate: likelyDuplicate,
    };
}

if (isMain(import.meta.url)) {
    const query = await readInput({ argName: "query", envName: "ISSUE_QUERY" });
    const excludeArg = process.argv.find((a) => a.startsWith("--exclude="));
    const excludeNumber = excludeArg ? parseInt(excludeArg.slice("--exclude=".length), 10) : undefined;
    printJson(await searchIssues(query, { excludeNumber }));
}
