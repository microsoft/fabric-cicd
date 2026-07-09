// Golden-set evaluation for the classify skill.
//
// Runs the classify skill over .github/evals/triage-golden-set.jsonl and reports category
// accuracy. Exits non-zero when accuracy drops below THRESHOLD (default 0.80) so CI catches
// prompt regressions. Requires GITHUB_TOKEN with `models: read`.
//
//   node .github/evals/run-golden.mjs           # full set, threshold 0.80
//   THRESHOLD=0.9 node .github/evals/run-golden.mjs

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { callModel, extractJson, getToken } from "../triage-tools/lib/gh.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const GOLDEN = join(HERE, "triage-golden-set.jsonl");
const CLASSIFY = join(HERE, "..", "prompts", "skills", "classify.prompt.yml");
const THRESHOLD = Number(process.env.THRESHOLD || "0.80");

async function loadClassify() {
    const yaml = (await importYaml()).default;
    const doc = yaml.load(readFileSync(CLASSIFY, "utf8"));
    return { messages: doc.messages || [], model: doc.model, params: doc.modelParameters || {} };
}

async function importYaml() {
    try {
        return await import("js-yaml");
    } catch {
        // Fall back to the copy installed under triage-tools when run from the repo root
        // without NODE_PATH set (e.g. local `node .github/evals/run-golden.mjs`).
        const local = join(HERE, "..", "triage-tools", "node_modules", "js-yaml", "index.js");
        return await import(`file://${local}`);
    }
}

function render(messages, input) {
    return messages.map((m) => ({ role: m.role, content: String(m.content).replace(/\{\{\s*input\s*\}\}/g, input) }));
}

async function main() {
    if (!getToken()) {
        console.error("No GITHUB_TOKEN — cannot run model evals. Skipping (soft pass).");
        process.exit(0);
    }
    const skill = await loadClassify();
    const rows = readFileSync(GOLDEN, "utf8")
        .split(/\r?\n/)
        .filter(Boolean)
        .map((l) => JSON.parse(l));

    let correct = 0;
    const failures = [];
    for (const row of rows) {
        let got = "error";
        try {
            const { content } = await callModel([skill.model, "microsoft/phi-4"], render(skill.messages, row.input), {
                maxTokens: skill.params.max_tokens || 200,
                temperature: 0,
                jsonObject: true,
            });
            got = extractJson(content)?.category || "unparsed";
        } catch (e) {
            got = `error:${e.message.slice(0, 40)}`;
        }
        if (got === row.expected_category) correct++;
        else failures.push(`  ✗ ${row.id}: expected ${row.expected_category}, got ${got}`);
    }

    const accuracy = correct / rows.length;
    console.log(`\nClassify golden-set accuracy: ${correct}/${rows.length} = ${(accuracy * 100).toFixed(1)}%`);
    if (failures.length) console.log("Mismatches:\n" + failures.join("\n"));
    if (accuracy < THRESHOLD) {
        console.error(`\nFAIL: accuracy ${(accuracy * 100).toFixed(1)}% < threshold ${(THRESHOLD * 100).toFixed(0)}%`);
        process.exit(1);
    }
    console.log(`PASS: accuracy meets threshold ${(THRESHOLD * 100).toFixed(0)}%`);
}

await main();
