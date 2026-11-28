import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function extractSection(markdown, version) {
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

export function formatDateUtc8(now = new Date()) {
  return new Date(now.getTime() + 8 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

function classifyAndFormatBullet(section, rawFirst, details) {
  const withoutHash = rawFirst.replace(/^[0-9a-f]{7,40}:\s+/i, "").trim();
  if (!withoutHash) {
    return null;
  }

  let kind = "fix";
  if (
    section === "Major Changes" ||
    withoutHash.startsWith("⚠️") ||
    /\bBREAKING CHANGE\b/i.test(withoutHash) ||
    /^[a-z]+(?:\([^)]*\))?!:/i.test(withoutHash)
  ) {
    kind = "warn";
  } else if (
    section === "Minor Changes" ||
    withoutHash.startsWith("💡") ||
    /^feat(?:\([^)]*\))?:/i.test(withoutHash)
  ) {
    kind = "feat";
  }

  let title = withoutHash.replace(/^(?:💡|🔧|⚠️?)\s*/u, "").trim();
  title = title
    .replace(
      /^(?:feat|fix|perf|revert|refactor|docs|chore|ci|test|style)(?:\([^)]*\))?!?:\s*/i,
      "",
    )
    .trim();
  if (!title) {
    title = withoutHash;
  }

  const detail = details.join(" ").trim();
  return detail ? { kind, title, detail } : { kind, title };
}

export function parseChangelogSection(markdownBody) {
  if (!markdownBody || typeof markdownBody !== "string") {
    return { items: [], sawDependencyBullet: false, sawUnparsedContent: false };
  }
  const lines = markdownBody.replace(/\r\n/g, "\n").split("\n");
  let fenceChar = "";
  let fenceLen = 0;
  let currentSection = "";
  let skippingUpdatedDeps = false;
  let currentBullet = null;
  let sawDependencyBullet = false;
  let sawUnparsedContent = false;
  const items = [];

  const flushBullet = () => {
    if (!currentBullet) return;
    const item = classifyAndFormatBullet(
      currentBullet.section,
      currentBullet.rawFirst,
      currentBullet.details,
    );
    if (item) {
      items.push(item);
    }
    currentBullet = null;
  };

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
      if (currentBullet && !skippingUpdatedDeps) {
        const trimmed = line.trim();
        if (trimmed) currentBullet.details.push(trimmed);
      } else if (!skippingUpdatedDeps && line.trim()) {
        sawUnparsedContent = true;
      }
      continue;
    }

    if (fenceMatch) {
      fenceChar = fenceMatch[1][0];
      fenceLen = fenceMatch[1].length;
      if (currentBullet && !skippingUpdatedDeps) {
        const trimmed = line.trim();
        if (trimmed) currentBullet.details.push(trimmed);
      } else if (!skippingUpdatedDeps) {
        sawUnparsedContent = true;
      }
      continue;
    }

    const headingMatch = line.match(/^###\s+(.+)$/);
    if (headingMatch) {
      flushBullet();
      skippingUpdatedDeps = false;
      currentSection = headingMatch[1].trim();
      continue;
    }

    if (skippingUpdatedDeps) {
      if (/^[ \t]+/.test(line) || line.trim() === "") {
        continue;
      }
      skippingUpdatedDeps = false;
    }

    const bulletMatch = line.match(/^[-*]\s+(.*)$/);
    if (bulletMatch) {
      flushBullet();
      const rawText = bulletMatch[1].trim();
      if (/^Updated dependencies\b/i.test(rawText)) {
        skippingUpdatedDeps = true;
        sawDependencyBullet = true;
        continue;
      }
      if (
        /^(?:@[a-z0-9-~][a-z0-9-._~]*\/)?[a-z0-9-~][a-z0-9-._~]*@\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/i.test(
          rawText,
        )
      ) {
        sawDependencyBullet = true;
        continue;
      }
      currentBullet = {
        section: currentSection,
        rawFirst: rawText,
        details: [],
      };
      continue;
    }

    if (currentBullet) {
      if (/^[ \t]+/.test(line)) {
        const trimmed = line.trim();
        if (trimmed) {
          currentBullet.details.push(trimmed);
        }
        continue;
      }
      if (line.trim() === "") {
        continue;
      }
      flushBullet();
    }

    const trimmedLine = line.trim();
    if (trimmedLine !== "" && trimmedLine !== "No changes in this release.") {
      sawUnparsedContent = true;
    }
  }

  flushBullet();
  return { items, sawDependencyBullet, sawUnparsedContent };
}

export function parseChangelogSectionToWebItems(markdownBody) {
  return parseChangelogSection(markdownBody).items;
}

export function readWebChangelogFile(filePath) {
  if (!existsSync(filePath)) {
    return { eol: "\n", entries: [] };
  }
  const raw = readFileSync(filePath, "utf8");
  const eol = raw.includes("\r\n") ? "\r\n" : "\n";
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`Expected JSON array in ${filePath}`);
  }
  return { eol, entries: parsed };
}

export function syncWebChangelog(filePath, version, body, { now } = {}) {
  const { items, sawDependencyBullet, sawUnparsedContent } =
    parseChangelogSection(body);
  if (items.length === 0) {
    const status = sawUnparsedContent
      ? "unparsed"
      : sawDependencyBullet
        ? "skipped-deps-only"
        : "empty";
    return { updated: false, status, itemCount: 0 };
  }

  const { eol, entries } = readWebChangelogFile(filePath);
  const tag = `v${version}`;
  const existingIdx = entries.findIndex((entry) => entry && entry.version === version);
  let nextEntries;
  let itemCount = items.length;
  if (existingIdx !== -1) {
    const prev = entries[existingIdx];
    const prevKindsByTitle = new Map();
    if (Array.isArray(prev.items)) {
      for (const prevItem of prev.items) {
        if (prevItem && typeof prevItem.title === "string" && prevItem.kind) {
          prevKindsByTitle.set(prevItem.title, prevItem.kind);
        }
      }
    }
    const mergedItems = items.map((item) => {
      const prevKind = prevKindsByTitle.get(item.title);
      return prevKind ? { ...item, kind: prevKind } : item;
    });
    itemCount = mergedItems.length;
    const updated = {
      ...prev,
      version,
      tag,
      date: prev.date || formatDateUtc8(now),
      items: mergedItems,
    };
    nextEntries = entries.map((entry, idx) => (idx === existingIdx ? updated : entry));
  } else {
    nextEntries = [
      {
        version,
        tag,
        date: formatDateUtc8(now),
        items,
      },
      ...entries,
    ];
  }

  mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
  let output = `${JSON.stringify(nextEntries, null, 2)}\n`;
  if (eol === "\r\n") {
    output = output.replace(/\n/g, "\r\n");
  }
  writeFileSync(filePath, output, "utf8");
  return { updated: true, status: "updated", itemCount };
}

function main() {
  const rootPkg = JSON.parse(readFileSync("package.json", "utf8"));
  const version = rootPkg.version?.trim();
  if (!version) {
    throw new Error("Root package.json is missing version");
  }

  const rootChangelog = readFileSync("CHANGELOG.md", "utf8");
  const body = extractSection(rootChangelog, version);
  const webChangelogPath = "app/changelog.generated.json";
  // 在修改任何版本文件前先校验内置更新日志 JSON，避免半截写入
  readWebChangelogFile(webChangelogPath);

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

  let webSync = null;
  if (body) {
    const docsChangelogPath = "docs/content/docs/guide/CHANGELOG.md";
    writePreservingEol(docsChangelogPath, (norm) => {
      const headingRegex = new RegExp(
        `^## ${version.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`,
        "m",
      );
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
    webSync = syncWebChangelog(webChangelogPath, version, body);
    if (webSync.status === "unparsed") {
      console.warn(
        `::warning::Release ${version} has non-empty CHANGELOG content that could not be parsed into WebUI changelog items; ${webChangelogPath} was not updated.`,
      );
    } else if (webSync.status === "skipped-deps-only") {
      console.log(
        `::notice::Skipping ${webChangelogPath} update for ${version} (dependency-only release).`,
      );
    }
  }

  console.log(
    `Synced biliup version ${version} across Cargo/Tauri/package manifests${webSync?.updated ? " and WebUI changelog" : ""}.`,
  );
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  main();
}
