// Tool skill: changelog_relevance
// Answers "could a newer release actually address THIS issue?" — not just "is the version old?".
//
// Pure semver staleness is misleading: telling every user on an older build to "upgrade and try
// again" is noise when nothing in the newer releases touches their problem. This tool parses the
// project changelog (docs/changelog.md), collects entries shipped in versions *newer than the
// reported one*, and scores each against the issue's real signals (exception types, verified
// symbols, keywords). It only concludes `upgrade_may_help` when a relevant change is found, and
// returns the exact changelog entry + issue link so the comment can point at concrete evidence.
//
// Standalone:  node .github/triage-tools/changelog_relevance.mjs --reported=1.1.0 --signals="ItemDependencyError,Activator"
// Importable:  import { changelogRelevance, parseChangelog } from "./changelog_relevance.mjs";

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { printJson, isMain } from "./lib/gh.mjs";
import { compareVersions } from "./check_version.mjs";

function resolveChangelogPath() {
    const fromArg = process.argv.find((a) => a.startsWith("--changelog="));
    if (fromArg) return fromArg.slice("--changelog=".length);
    if (process.env.FABRIC_CHANGELOG_PATH) return process.env.FABRIC_CHANGELOG_PATH;
    return join(process.cwd(), "docs", "changelog.md");
}

// Sections that represent shippable behavior changes (worth an "upgrade may help" nudge).
// Documentation-only entries are parsed but never on their own justify an upgrade suggestion.
const ACTIONABLE_SECTIONS = /new items|new functionality|bug fix|optimization|breaking change/i;

// Parse docs/changelog.md into [{ version, date, sections: [{ title, entries: [{text, issue, url}] }] }].
export function parseChangelog(markdown) {
    const text = String(markdown || "");
    const versions = [];
    // Split on top-level version headers: "## [v1.2.0](...) - June 30, 2026"
    const blocks = text.split(/\n(?=##\s+\[)/);
    for (const block of blocks) {
        const header = block.match(/^##\s+\[v?(\d+\.\d+(?:\.\d+)?)\]\(([^)]*)\)(?:\s*-\s*(.+))?/m);
        if (!header) continue;
        const version = header[1];
        const versionUrl = header[2] || "";
        const date = (header[3] || "").trim();
        const sections = [];
        // Split the block into "### <section>" chunks.
        const secChunks = block.split(/\n(?=###\s+)/);
        for (const chunk of secChunks) {
            const secHead = chunk.match(/^###\s+(.+)$/m);
            if (!secHead) continue;
            const title = secHead[1].replace(/[^\p{L}\p{N} ]/gu, "").trim(); // drop emoji
            const entries = [];
            const bulletRe = /^\s*[-*]\s+(.+)$/gm;
            let m;
            while ((m = bulletRe.exec(chunk)) !== null) {
                const line = m[1].trim();
                const issueLink = line.match(/\[#(\d+)\]\(([^)]+)\)/);
                // Strip the trailing "by [author](url) ([#N](url))" attribution for a clean summary.
                const cleaned = line
                    .replace(/\s+by\s+\[[^\]]+\]\([^)]*\)/g, "")
                    .replace(/\s*\(\[#\d+\]\([^)]*\)\)\s*$/g, "")
                    .trim();
                entries.push({
                    text: cleaned,
                    issue: issueLink ? parseInt(issueLink[1], 10) : null,
                    url: issueLink ? issueLink[2] : versionUrl,
                });
            }
            if (entries.length) sections.push({ title, entries });
        }
        versions.push({ version, date, url: versionUrl, sections });
    }
    return versions;
}

function tokenize(text) {
    return new Set(
        String(text || "")
            .toLowerCase()
            .replace(/[^a-z0-9_]+/g, " ")
            .split(/\s+/)
            .filter((t) => t.length > 2)
    );
}

// Keywords too generic to connect an issue to a specific changelog entry. Matching on these
// alone is exactly what produced the old "upgrade and review" noise (e.g. the issue text and a
// "remove version check on import" entry both contain "version"/"call"). Only STRONG signals
// (exception types, verified symbols) or specific keywords may justify an upgrade nudge.
const GENERIC_KEYWORDS = new Set(
    ("version versions call calls library libraries import imports network startup noise install installs pip pypi " +
        "python latest release releases update updates upgrade upgrades change changes fix fixes issue issues error errors " +
        "message messages support add added adds remove removed removes improve improved add feature api apis request requests " +
        "response file files folder name names value values default option options behavior behaviour").split(" ")
);

// Score one changelog entry against the issue signals.
//   strong signals (exception types, verified symbols) are decisive; specific (non-generic)
//   keywords need >= 2 to count. Generic keywords never make an entry relevant on their own.
function scoreEntry(entry, sectionTitle, { strong, keywords }) {
    const hay = entry.text.toLowerCase();
    const hayTokens = tokenize(entry.text);
    const strongHits = strong.filter((s) => hay.includes(String(s).toLowerCase()));
    const specificKeywords = keywords.filter((k) => !GENERIC_KEYWORDS.has(String(k).toLowerCase()));
    const keywordHits = specificKeywords.filter((k) => hayTokens.has(String(k).toLowerCase()));
    const sectionBoost = ACTIONABLE_SECTIONS.test(sectionTitle) ? 1 : 0;
    const relevant = strongHits.length >= 1 || keywordHits.length >= 2;
    const score = strongHits.length * 3 + keywordHits.length + sectionBoost;
    return { relevant: relevant && sectionBoost > 0, score, strongHits, keywordHits };
}

// signals: { strong: string[], keywords: string[] } — strong = exception types / canonical symbols.
export function changelogRelevance(markdown, reportedVersion, signals = {}, { latestVersion, limit = 4 } = {}) {
    const strong = [...new Set(signals.strong || [])].filter(Boolean);
    const keywords = [...new Set(signals.keywords || [])].filter(Boolean);
    const versions = parseChangelog(markdown);

    if (!reportedVersion) {
        return { reported_version: null, newer_versions: [], relevant_entries: [], upgrade_may_help: false, reason: "no version reported" };
    }

    const newer = versions.filter((v) => {
        if (compareVersions(v.version, reportedVersion) <= 0) return false;
        if (latestVersion && compareVersions(v.version, latestVersion) > 0) return false;
        return true;
    });

    const relevant = [];
    for (const v of newer) {
        for (const sec of v.sections) {
            for (const entry of sec.entries) {
                const s = scoreEntry(entry, sec.title, { strong, keywords });
                if (s.relevant) {
                    relevant.push({
                        version: v.version,
                        version_url: v.url,
                        section: sec.title,
                        text: entry.text,
                        issue: entry.issue,
                        url: entry.url,
                        score: s.score,
                        matched: [...s.strongHits, ...s.keywordHits],
                    });
                }
            }
        }
    }
    relevant.sort((a, b) => b.score - a.score || compareVersions(a.version, b.version));
    const top = relevant.slice(0, limit);

    return {
        reported_version: reportedVersion,
        latest_version: latestVersion || (versions[0] ? versions[0].version : null),
        newer_versions: newer.map((v) => v.version),
        relevant_entries: top,
        upgrade_may_help: top.length > 0,
        // Only when a change genuinely relates to the issue do we recommend the newest version that carries a fix.
        suggested_version: top.length ? top[0].version : null,
    };
}

// Convenience: load the changelog from disk and score in one call (used by the orchestrator).
export function changelogRelevanceFromFile(reportedVersion, signals, opts = {}) {
    let md = "";
    try {
        md = readFileSync(resolveChangelogPath(), "utf8");
    } catch {
        return { reported_version: reportedVersion || null, newer_versions: [], relevant_entries: [], upgrade_may_help: false, reason: "changelog not found" };
    }
    return changelogRelevance(md, reportedVersion, signals, opts);
}

// A compact digest of what actually changed in the most recent release(s) — grounding for the
// analysis stage so it can reason about whether an issue reflects recently changed behavior.
export function recentChanges(markdown, { versions = 1, perVersion = 8 } = {}) {
    const parsed = parseChangelog(markdown).slice(0, versions);
    return parsed.map((v) => ({
        version: v.version,
        date: v.date,
        changes: v.sections
            .filter((s) => ACTIONABLE_SECTIONS.test(s.title))
            .flatMap((s) => s.entries.map((e) => ({ section: s.title, text: e.text, issue: e.issue, url: e.url })))
            .slice(0, perVersion),
    }));
}

export function recentChangesFromFile(opts = {}) {
    try {
        return recentChanges(readFileSync(resolveChangelogPath(), "utf8"), opts);
    } catch {
        return [];
    }
}

if (isMain(import.meta.url)) {
    const arg = (name) => {
        const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
        return hit ? hit.slice(name.length + 3) : undefined;
    };
    const reported = arg("reported") || process.env.REPORTED_VERSION || null;
    const latest = arg("latest") || process.env.FABRIC_LATEST_VERSION || undefined;
    const rawSignals = (arg("signals") || "").split(",").map((s) => s.trim()).filter(Boolean);
    // Treat CamelCase / snake_case tokens as strong signals, plain words as keywords.
    const strong = rawSignals.filter((s) => /[A-Z]/.test(s) || s.includes("_"));
    const keywords = rawSignals.filter((s) => !strong.includes(s));
    printJson(changelogRelevanceFromFile(reported, { strong, keywords }, { latestVersion: latest }));
}
