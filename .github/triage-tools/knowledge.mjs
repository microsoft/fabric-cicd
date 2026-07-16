// Tool skill: knowledge
// A tiny, curated, CITABLE knowledge base of authoritative fabric-cicd / Microsoft Fabric domain
// facts. It exists so the triage reasons correctly about known *by-design* behaviors and about what
// an item *definition* does (and does not) contain — instead of guessing and mislabeling expected
// behavior as a bug, or answering a feasibility question with only generic doc links.
//
// Each fact is grounded: `code` points at the real fabric-cicd source that implements the behavior
// and `docs` link the official Microsoft Learn pages the fact is drawn from. Facts are matched to an
// issue with a cheap keyword test; selected facts are injected into (a) the analysis input (so
// bug-analyze / answer / misconfig-resolve treat them as ground truth) and (b) the comment-draft
// evidence bundle (so even the feature path, which runs no analysis skill, can still cite them).
//
// Standalone:  node .github/triage-tools/knowledge.mjs --text="renaming a semantic model creates a new one"
// Importable:  import { selectKnowledge } from "./knowledge.mjs";

import { printJson, isMain, getRepo } from "./lib/gh.mjs";

// GitHub blob permalink for a file in the running repo, matching search_code.mjs's scheme so
// knowledge links resolve in whatever repo the workflow runs in (including a fork dry-run).
function blob(path, line) {
    const server = process.env.GITHUB_SERVER_URL || "https://github.com";
    const { owner, repo } = getRepo();
    const ref = process.env.GITHUB_SHA || "main";
    return `${server}/${owner}/${repo}/blob/${ref}/${path}${line ? `#L${line}` : ""}`;
}

// A fact matches when `test` (run against the lower-cased issue text) returns true. Keep `test`
// tight so a fact only fires when the issue is genuinely about that behavior.
const FACTS = [
    {
        id: "rename-creates-new-item",
        // by_design => the reported behavior is expected/documented, so the orchestrator resolves it
        // as "works-as-designed" (answer + workaround) instead of escalating it as a potential bug.
        by_design: true,
        test: (t) => /\brenam(e|ed|es|ing)\b/.test(t),
        title: "Renaming an item creates a NEW item on deploy — expected behavior, not a defect",
        fact:
            "fabric-cicd identifies repository items by type + display name (it keys them as " +
            "`repository_items[item_type][item_name]`, read straight from each item's folder). Renaming " +
            "an item changes that name, so on the next publish the library sees an item it has never " +
            "deployed, creates a brand-new one, and leaves the previously-named item behind as an orphan. " +
            "This is by design: the library has no rename/move operation and does not track a stable " +
            "identity across a name change (Fabric's own Git integration tracks identity by logical ID, " +
            "but fabric-cicd matches by name+type). The stale item can be removed with " +
            "`unpublish_all_orphan_items(workspace)`.",
        code: [
            { label: "items keyed by type + name — fabric_workspace.py", path: "src/fabric_cicd/fabric_workspace.py", line: 404 },
            { label: "existing GUID only reused when the name still matches — fabric_workspace.py", path: "src/fabric_cicd/fabric_workspace.py", line: 827 },
            { label: "unpublish_all_orphan_items — publish.py", path: "src/fabric_cicd/publish.py", line: 299 },
        ],
        docs: [
            { label: "Resolve logical ID conflicts (Fabric item identity)", url: "https://learn.microsoft.com/en-us/fabric/cicd/git-integration/conflict-resolution" },
            { label: "Manage orphan items — fabric-cicd docs", url: "https://microsoft.github.io/fabric-cicd/latest/" },
        ],
    },
    {
        id: "tags-not-in-item-definition",
        // Not by_design: this is a legitimate enhancement request — the fact just makes the answer
        // accurate about *why* it isn't supported today and *how* it would have to be implemented.
        test: (t) => /\btags?\b/.test(t) && /\b(item|items|apply|applied|carry|carried|forward|govern|metadata|catalog|classif|label|workspace)\b/.test(t),
        title: "Item tags are governance metadata — not part of an item definition",
        fact:
            "A Fabric item definition is the structural metadata that describes how an item is " +
            "constructed (its `.platform` system file plus the item's payload parts). Tags are governance " +
            "metadata applied to items and workspaces through a separate tags API, and are NOT included in " +
            "the item definition. fabric-cicd deploys item *definitions*, so it does not — and cannot via " +
            "definitions — carry tags across environments today (there is no tag handling anywhere in " +
            "`src/fabric_cicd`). Supporting tags would be a real enhancement requiring the library to call " +
            "the Fabric tags API (apply/remove) as a post-publish step, not something available through the " +
            "item definition.",
        code: [],
        docs: [
            { label: "Item definition overview (what a definition contains)", url: "https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/item-definition-overview" },
            { label: "Tags in Microsoft Fabric (tag APIs)", url: "https://learn.microsoft.com/en-us/fabric/governance/tags-overview" },
            { label: "Apply tags to items and workspaces", url: "https://learn.microsoft.com/en-us/fabric/governance/tags-apply" },
        ],
    },
];

// Return the facts relevant to an issue, each with its code + doc links flattened into a single
// `links` array of { label, url } (code pointers first). Shape is stable for prompt injection.
export function selectKnowledge(text) {
    const t = String(text || "").toLowerCase();
    return FACTS.filter((f) => {
        try {
            return f.test(t);
        } catch {
            return false;
        }
    }).map((f) => ({
        id: f.id,
        by_design: !!f.by_design,
        title: f.title,
        fact: f.fact,
        links: [
            ...(f.code || []).map((c) => ({ label: c.label, url: blob(c.path, c.line) })),
            ...(f.docs || []).map((d) => ({ label: d.label, url: d.url })),
        ],
    }));
}

if (isMain(import.meta.url)) {
    const arg = process.argv.find((a) => a.startsWith("--text="));
    const text = arg ? arg.slice("--text=".length) : `${process.env.ISSUE_TITLE || ""} ${process.env.ISSUE_BODY || ""}`;
    printJson(selectKnowledge(text));
}
