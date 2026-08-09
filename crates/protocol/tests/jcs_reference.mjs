// Independent ECMAScript reference for the AFWP-C14N-1 conformance test.
// It intentionally uses only Node's standard library.
import { createHash } from "node:crypto";

function canonicalize(value) {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("non-finite JSON number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalize).join(",")}]`;
  }
  if (typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalize(value[key])}`)
      .join(",")}}`;
  }
  throw new Error(`unsupported value: ${typeof value}`);
}

function packageHash(document) {
  const signingValue = structuredClone(document);
  delete signingValue.package_hash;
  return `sha256:${createHash("sha256").update(canonicalize(signingValue), "utf8").digest("hex")}`;
}

let input = "";
for await (const chunk of process.stdin) input += chunk;
process.stdout.write(JSON.stringify(JSON.parse(input).map(packageHash)));
