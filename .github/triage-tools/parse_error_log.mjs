// Tool skill: parse_error_log
// Extracts the real exception type(s), message, traceback, HTTP status and request id from a
// pasted `fabric_cicd.error.log` or Python traceback so downstream analysis anchors to the
// actual failure instead of the user's prose.
//
// Standalone:  node .github/triage-tools/parse_error_log.mjs --file=path/to/log.txt
//              cat log.txt | node .github/triage-tools/parse_error_log.mjs
// Importable:  import { parseErrorLog } from "./parse_error_log.mjs";

import { readFileSync } from "node:fs";
import { readInput, printJson, isMain } from "./lib/gh.mjs";

// fabric-cicd custom exceptions plus common Python/Azure ones worth surfacing.
const KNOWN_EXCEPTIONS = [
    "ParsingError",
    "InputError",
    "TokenError",
    "InvokeError",
    "ItemDependencyError",
    "FileTypeError",
    "ParameterFileError",
    "FailedPublishedItemStatusError",
    "PublishError",
    "ConfigValidationError",
    "CredentialUnavailableError",
    "ClientAuthenticationError",
];

export function parseErrorLog(text) {
    const raw = String(text || "");
    const lines = raw.split(/\r?\n/);

    // Any `SomeError: message` occurrence.
    const exceptions = [];
    const seen = new Set();
    const excRe = /(?:^|[\s.>])([A-Z][A-Za-z0-9_]*(?:Error|Exception|Warning))\s*:\s*(.+?)\s*$/;
    for (const line of lines) {
        const m = excRe.exec(line);
        if (m) {
            const key = `${m[1]}::${m[2]}`;
            if (!seen.has(key)) {
                seen.add(key);
                exceptions.push({ type: m[1], message: m[2].slice(0, 500), known: KNOWN_EXCEPTIONS.includes(m[1]) });
            }
        }
    }

    // HTTP status (e.g. "status code 429", "HTTP 404", "response: 403").
    let httpStatus = null;
    const httpRe = /(?:status(?:\s*code)?|http|response)\D{0,8}(\d{3})/i;
    for (const line of lines) {
        const m = httpRe.exec(line);
        if (m) {
            const code = parseInt(m[1], 10);
            if (code >= 100 && code < 600) {
                httpStatus = code;
                break;
            }
        }
    }

    // Fabric/Power BI request id (RequestId / x-ms-request-id / activity id GUIDs).
    let requestId = null;
    const reqRe = /(?:request[\s_-]?id|activity[\s_-]?id|x-ms-request-id)\D{0,4}([0-9a-fA-F-]{8,})/i;
    for (const line of lines) {
        const m = reqRe.exec(line);
        if (m) {
            requestId = m[1];
            break;
        }
    }

    // Last traceback block, if present.
    let traceback = null;
    const tbStart = raw.lastIndexOf("Traceback (most recent call last)");
    if (tbStart !== -1) {
        traceback = raw.slice(tbStart).split(/\n\s*\n/)[0].slice(0, 2000);
    }

    const primary = exceptions.length ? exceptions[exceptions.length - 1] : null;

    return {
        has_error: exceptions.length > 0 || httpStatus !== null,
        primary_exception: primary,
        exceptions,
        http_status: httpStatus,
        request_id: requestId,
        traceback,
    };
}

if (isMain(import.meta.url)) {
    const fileArg = process.argv.find((a) => a.startsWith("--file="));
    let text;
    if (fileArg) {
        text = readFileSync(fileArg.slice("--file=".length), "utf8");
    } else {
        text = await readInput({ argName: "text", envName: "ERROR_LOG" });
    }
    printJson(parseErrorLog(text));
}
