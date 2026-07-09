// Tool skill: verify_symbol
// Confirms that a user-named ItemType or FeatureFlag actually exists in
// src/fabric_cicd/constants.py, preventing the triage system from hallucinating API surface.
//
// Standalone:  node .github/triage-tools/verify_symbol.mjs "enable_lakehouse_unpublish"
//              node .github/triage-tools/verify_symbol.mjs --name=Notebook
// Importable:  import { verifySymbol, loadSymbols } from "./verify_symbol.mjs";

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { readInput, printJson, isMain } from "./lib/gh.mjs";

function resolveConstantsPath() {
    const fromArg = process.argv.find((a) => a.startsWith("--constants="));
    if (fromArg) return fromArg.slice("--constants=".length);
    if (process.env.FABRIC_CONSTANTS_PATH) return process.env.FABRIC_CONSTANTS_PATH;
    return join(process.cwd(), "src", "fabric_cicd", "constants.py");
}

// Parse the string values of a `class <Name>(str, Enum):` block from constants.py.
function parseEnumValues(source, className) {
    const classRe = new RegExp(`class\\s+${className}\\s*\\([^)]*\\)\\s*:`, "m");
    const m = classRe.exec(source);
    if (!m) return [];
    const rest = source.slice(m.index + m[0].length);
    // Stop at the next top-level `class ` or a top-level assignment (e.g. SERIAL_ITEM_...).
    const stop = rest.search(/\n(?:class\s+\w+|[A-Z_]+[A-Z0-9_]*\s*[:=])/);
    const block = stop === -1 ? rest : rest.slice(0, stop);
    const values = [];
    const valueRe = /=\s*"([^"]+)"/g;
    let v;
    while ((v = valueRe.exec(block)) !== null) values.push(v[1]);
    return values;
}

export function loadSymbols(constantsPath = resolveConstantsPath()) {
    const source = readFileSync(constantsPath, "utf8");
    return {
        itemTypes: parseEnumValues(source, "ItemType"),
        featureFlags: parseEnumValues(source, "FeatureFlag"),
        envVars: parseEnumValues(source, "EnvVar"),
    };
}

function normalize(s) {
    return String(s || "").trim().toLowerCase().replace(/[\s_-]/g, "");
}

// Levenshtein edit distance for typo-tolerant suggestions.
function editDistance(a, b) {
    const m = a.length;
    const n = b.length;
    if (m === 0) return n;
    if (n === 0) return m;
    let prev = Array.from({ length: n + 1 }, (_, j) => j);
    let curr = new Array(n + 1);
    for (let i = 1; i <= m; i++) {
        curr[0] = i;
        for (let j = 1; j <= n; j++) {
            const cost = a[i - 1] === b[j - 1] ? 0 : 1;
            curr[j] = Math.min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost);
        }
        [prev, curr] = [curr, prev];
    }
    return prev[n];
}

export function verifySymbol(name, constantsPath) {
    const symbols = loadSymbols(constantsPath);
    const target = normalize(name);
    const check = (list, kind) => {
        for (const value of list) {
            if (normalize(value) === target) return { exists: true, kind, canonical: value };
        }
        return null;
    };
    const hit = check(symbols.itemTypes, "ItemType") || check(symbols.featureFlags, "FeatureFlag") || check(symbols.envVars, "EnvVar");

    // Near-miss suggestions (substring first, then edit-distance) to help the model recover a typo.
    const pool = [...symbols.itemTypes, ...symbols.featureFlags, ...symbols.envVars];
    let suggestions = [];
    if (target) {
        const substr = pool.filter((v) => normalize(v).includes(target) || target.includes(normalize(v)));
        const fuzzy = pool
            .map((v) => ({ v, d: editDistance(target, normalize(v)) }))
            .filter((x) => x.d > 0 && x.d <= Math.max(2, Math.floor(target.length / 4)))
            .sort((a, b) => a.d - b.d)
            .map((x) => x.v);
        suggestions = [...new Set([...substr, ...fuzzy])].slice(0, 5);
    }

    return {
        name,
        exists: Boolean(hit),
        kind: hit ? hit.kind : null,
        canonical: hit ? hit.canonical : null,
        suggestions: hit ? [] : suggestions,
        known_item_types: symbols.itemTypes,
        known_feature_flags: symbols.featureFlags,
    };
}

if (isMain(import.meta.url)) {
    const name = await readInput({ argName: "name", envName: "SYMBOL_NAME" });
    try {
        printJson(verifySymbol(name));
    } catch (e) {
        printJson({ name, exists: false, error: e.message });
        process.exitCode = 1;
    }
}
