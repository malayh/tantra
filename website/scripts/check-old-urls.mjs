import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const base = "/tantra";
const dist = fileURLToPath(new URL("../dist/", import.meta.url));
const urls = JSON.parse(readFileSync(new URL("./legacy-urls.json", import.meta.url), "utf8"));
const refresh = /<meta(?=[^>]*\shttp-equiv=["']?refresh["']?)[^>]*\scontent=["']?\d+\s*;\s*url=([^"'>\s]+)/i;

const pageFor = (path) => `${dist}${path.replace(/^\//, "")}index.html`;
const failures = [];

for (const url of urls) {
  const page = pageFor(url);
  if (!existsSync(page)) {
    failures.push(`${url}: missing ${page}`);
    continue;
  }
  const target = readFileSync(page, "utf8").match(refresh)?.[1];
  if (!target) continue;
  const path = new URL(target, `https://example.test${base}${url}`).pathname;
  if (!path.startsWith(`${base}/`) || !existsSync(pageFor(path.slice(base.length)))) {
    failures.push(`${url}: redirect target ${target} does not exist`);
  }
}

if (failures.length) {
  console.error(`Old URL check failed for ${failures.length} of ${urls.length} URLs:`);
  for (const failure of failures) console.error(`  ${failure}`);
  process.exit(1);
}

console.log(`Old URL check passed for ${urls.length} URLs.`);
