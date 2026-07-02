import { readdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { transform } from "esbuild";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const outputDir = path.resolve(scriptDir, "..", "ts-dist");

async function collectJavaScriptFiles(directory) {
  const entries = await readdir(directory);
  const files = [];

  for (const entry of entries) {
    const entryPath = path.join(directory, entry);
    const entryStats = await stat(entryPath);
    if (entryStats.isDirectory()) {
      files.push(...await collectJavaScriptFiles(entryPath));
      continue;
    }
    if (entry.endsWith(".js")) {
      files.push(entryPath);
    }
  }

  return files;
}

for (const filePath of await collectJavaScriptFiles(outputDir)) {
  const source = await readFile(filePath, "utf8");
  const result = await transform(source, {
    format: "esm",
    legalComments: "none",
    minifyIdentifiers: false,
    minifySyntax: true,
    minifyWhitespace: true,
    sourcefile: path.relative(outputDir, filePath),
    target: "es2022",
  });
  await writeFile(filePath, result.code);
}