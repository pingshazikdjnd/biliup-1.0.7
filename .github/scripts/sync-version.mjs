import { existsSync, readFileSync, writeFileSync } from "node:fs";

function extractSection(markdown, version) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const targetHeading = `## ${version}`;
  let fenceChar = "";
  let fenceLen = 0;
  let capturing = false;
  const captured = [];

  for (const line of lines) {
    const fenceMatch = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
    if (fenceChar) {
      if (
        fenceMatch &&
        fenceMatch[1][0] === fenceChar &&
        fenceMatch[1].length >= fenceLen &&
        fenceMatch[2].trim() === ""
      ) {
        fenceChar = "";
        fenceLen = 0;
      }
      if (capturing) captured.push(line);
      continue;
    }

    if (fenceMatch) {
      fenceChar = fenceMatch[1][0];
      fenceLen = fenceMatch[1].length;
      if (capturing) captured.push(line);
      continue;
    }

    if (!capturing) {
      if (line.trimEnd() === targetHeading) {
        capturing = true;
      }
      continue;
    }

    if (/^## /.test(line)) {
      break;
    }
    captured.push(line);
  }

  return captured.join("\n").trim();
}

function writePreservingEol(filePath, transform) {
  const raw = readFileSync(filePath, "utf8");
  const eol = raw.includes("\r\n") ? "\r\n" : "\n";
  const normalized = raw.replace(/\r\n/g, "\n");
  let next = transform(normalized);
  if (eol === "\r\n") {
    next = next.replace(/\n/g, "\r\n");
  }
  writeFileSync(filePath, next, "utf8");
}

function updateJsonVersion(filePath, version) {
  writePreservingEol(filePath, (norm) => {
    const data = JSON.parse(norm);
    data.version = version;
    return `${JSON.stringify(data, null, 2)}\n`;
  });
}

function updateNpmLockVersion(filePath, version) {
  if (!existsSync(filePath)) return;
  writePreservingEol(filePath, (norm) => {
    const lock = JSON.parse(norm);
    lock.version = version;
    if (lock.packages && lock.packages[""]) {
      lock.packages[""].version = version;
    }
    return `${JSON.stringify(lock, null, 2)}\n`;
  });
}

function updateCargoPackageVersion(filePath, version) {
  writePreservingEol(filePath, (norm) => {
    let replaced = false;
    const updated = norm.replace(
      /^(version\s*=\s*")[^"]+(")/m,
      (_, prefix, suffix) => {
        replaced = true;
        return `${prefix}${version}${suffix}`;
      },
    );
    if (!replaced) {
      throw new Error(`Could not find package version in ${filePath}`);
    }
    return updated;
  });
}

function updateCargoLockPackageVersion(norm, pkgName, version) {
  const pattern = new RegExp(
    `(^\\[\\[package\\]\\]\\nname = "${pkgName}"\\nversion = ")[^"]+(")`,
    "m",
  );
  let replaced = false;
  const updated = norm.replace(pattern, (_, prefix, suffix) => {
    replaced = true;
    return `${prefix}${version}${suffix}`;
  });
  if (!replaced) {
    throw new Error(`Could not find [[package]] ${pkgName} in Cargo.lock`);
  }
  return updated;
}

const rootPkg = JSON.parse(readFileSync("package.json", "utf8"));
const version = rootPkg.version?.trim();
if (!version) {
  throw new Error("Root package.json is missing version");
}

updateCargoPackageVersion("crates/stream-gears/Cargo.toml", version);
updateCargoPackageVersion("tauri-app/src-tauri/Cargo.toml", version);
updateJsonVersion("tauri-app/src-tauri/tauri.conf.json", version);
updateJsonVersion("tauri-app/package.json", version);
updateNpmLockVersion("tauri-app/package-lock.json", version);

writePreservingEol("Cargo.lock", (norm) => {
  let updated = updateCargoLockPackageVersion(norm, "stream-gears", version);
  updated = updateCargoLockPackageVersion(updated, "tauri-app", version);
  return updated;
});

const rootChangelog = readFileSync("CHANGELOG.md", "utf8");
const body = extractSection(rootChangelog, version);
if (body) {
  const docsChangelogPath = "docs/content/docs/guide/CHANGELOG.md";
  writePreservingEol(docsChangelogPath, (norm) => {
    const headingRegex = new RegExp(`^## ${version.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "m");
    if (headingRegex.test(norm)) {
      return norm;
    }
    const anchorMatch = norm.match(/^## \d/m);
    if (!anchorMatch || anchorMatch.index === undefined) {
      throw new Error(`Could not find version heading anchor in ${docsChangelogPath}`);
    }
    const insertion = `## ${version}\n\n${body}\n\n`;
    return (
      norm.slice(0, anchorMatch.index) +
      insertion +
      norm.slice(anchorMatch.index)
    );
  });
}

console.log(`Synced biliup version ${version} across Cargo/Tauri/package manifests.`);
