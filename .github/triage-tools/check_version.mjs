// Tool skill: check_version
// Parses the fabric-cicd version the user reported, compares it to the latest release, and
// flags whether they are running a stale build ("may already be fixed in vX.Y.Z").
//
// Latest version resolution order: FABRIC_LATEST_VERSION env → PyPI → local constants.py.
//
// Standalone:  node .github/triage-tools/check_version.mjs --version=1.1.0
//              node .github/triage-tools/check_version.mjs "I'm on Library Version: 1.0.9"
// Importable:  import { checkVersion, extractVersion } from "./check_version.mjs";

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { readInput, printJson, isMain } from "./lib/gh.mjs";

const PYPI_URL = "https://pypi.org/pypi/fabric-cicd/json";

// Pull the first plausible semver from free text, honoring "Library Version: x.y.z" hints.
export function extractVersion(text) {
    const raw = String(text || "");
    const labeled = raw.match(/(?:library\s*version|fabric[\s_-]?cicd|version)\D{0,4}(\d+\.\d+(?:\.\d+)?)/i);
    if (labeled) return labeled[1];
    const any = raw.match(/\b(\d+\.\d+\.\d+)\b/);
    return any ? any[1] : null;
}

function parseParts(v) {
    return String(v || "")
        .split(".")
        .map((n) => parseInt(n, 10) || 0);
}

// Returns 1 if a>b, -1 if a<b, 0 if equal.
export function compareVersions(a, b) {
    const pa = parseParts(a);
    const pb = parseParts(b);
    const len = Math.max(pa.length, pb.length);
    for (let i = 0; i < len; i++) {
        const da = pa[i] || 0;
        const db = pb[i] || 0;
        if (da > db) return 1;
        if (da < db) return -1;
    }
    return 0;
}

function localVersion() {
    try {
        const source = readFileSync(join(process.cwd(), "src", "fabric_cicd", "constants.py"), "utf8");
        const m = source.match(/^VERSION\s*=\s*"([^"]+)"/m);
        return m ? m[1] : null;
    } catch {
        return null;
    }
}

async function latestVersion() {
    if (process.env.FABRIC_LATEST_VERSION) return { version: process.env.FABRIC_LATEST_VERSION, source: "env" };
    try {
        const res = await fetch(PYPI_URL, { headers: { "User-Agent": "fabric-cicd-triage" } });
        if (res.ok) {
            const data = await res.json();
            if (data?.info?.version) return { version: data.info.version, source: "pypi" };
        }
    } catch {
        // fall through to local
    }
    const local = localVersion();
    if (local) return { version: local, source: "constants.py" };
    return { version: null, source: "unknown" };
}

export async function checkVersion(input) {
    const reported = extractVersion(input);
    const { version: latest, source } = await latestVersion();

    let isStale = null;
    let behindBy = null;
    if (reported && latest) {
        const cmp = compareVersions(reported, latest);
        isStale = cmp < 0;
        if (isStale) {
            const rp = parseParts(reported);
            const lp = parseParts(latest);
            behindBy = { major: (lp[0] || 0) - (rp[0] || 0), minor: (lp[1] || 0) - (rp[1] || 0), patch: (lp[2] || 0) - (rp[2] || 0) };
        }
    }

    return {
        reported_version: reported,
        latest_version: latest,
        latest_source: source,
        is_stale: isStale,
        behind_by: behindBy,
        recommend_upgrade: Boolean(isStale),
    };
}

if (isMain(import.meta.url)) {
    const input = await readInput({ argName: "version", envName: "ISSUE_BODY" });
    printJson(await checkVersion(input));
}
