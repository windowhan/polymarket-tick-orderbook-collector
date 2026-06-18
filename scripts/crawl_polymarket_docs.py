#!/usr/bin/env python3
"""Mirror official Polymarket docs Markdown pages into docs/polymarket-docs.

This script intentionally uses the official `llms.txt` index as the source of
truth. It does not HTML-crawl, translate, summarize, or reinterpret the source
content; it only adds a small frontmatter block for local traceability.
"""

from __future__ import annotations

import json
import re
import subprocess
import shutil
import sys
import time
from tempfile import TemporaryDirectory
from urllib.parse import unquote
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

BASE_URL = "https://docs.polymarket.com"
LLMS_URL = f"{BASE_URL}/llms.txt"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
OUT_DIR = Path("docs/polymarket-docs")
REQUEST_DELAY_SECS = 0.20


@dataclass(frozen=True)
class DocPage:
    """One Markdown document discovered from the official Polymarket llms.txt."""

    title: str
    url: str
    description: str

    @property
    def relative_path(self) -> Path:
        """Map a source URL path to the local mirror path below OUT_DIR.

        The Polymarket docs include a root `/index.md` page. On the default
        macOS case-insensitive filesystem, that path collides with this mirror's
        generated `INDEX.md` table of contents. Store the source root index under
        `_root/index.md` so both files can coexist while preserving the source
        URL in frontmatter and manifest metadata.
        """
        parsed = urlparse(self.url)
        if parsed.scheme != "https" or parsed.netloc != "docs.polymarket.com":
            raise ValueError(f"Unexpected Polymarket docs URL: {self.url}")

        path = unquote(parsed.path).lstrip("/")
        candidate = Path(path)
        if (
            not path.endswith(".md")
            or candidate.is_absolute()
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise ValueError(f"Unsafe Polymarket docs path: {self.url}")

        if path == "index.md":
            path = "_root/index.md"
        return Path(path)


def curl_text(url: str) -> str:
    """Fetch UTF-8 text with curl and retries.

    The project uses this small script for documentation mirroring only. Curl is
    used instead of Python's urllib so the script follows the same TLS behavior
    as the shell commands used during investigation on macOS.
    """
    result = subprocess.run(
        [
            "curl",
            "-fsSL",
            "--retry",
            "3",
            "--retry-delay",
            "1",
            "--connect-timeout",
            "10",
            "--max-time",
            "60",
            url,
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def parse_llms(text: str) -> list[DocPage]:
    """Extract official Markdown page links from Polymarket llms.txt."""
    pages: list[DocPage] = []
    pattern = re.compile(
        r"^- \[(?P<title>[^\]]+)\]"
        r"\((?P<url>https://docs\.polymarket\.com/[^)]+?\.md)\)"
        r"(?:: (?P<desc>.*))?$"
    )
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        pages.append(
            DocPage(
                title=match.group("title"),
                url=match.group("url"),
                description=match.group("desc") or "",
            )
        )
    return pages


def quote_yaml(value: str) -> str:
    """Return a conservative double-quoted YAML scalar."""
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def frontmatter(page: DocPage, crawled_at: str) -> str:
    """Build local traceability metadata for a mirrored document."""
    return (
        "---\n"
        f"title: {quote_yaml(page.title)}\n"
        f"source_url: {page.url}\n"
        f"crawled_at: {crawled_at}\n"
        f"description: {quote_yaml(page.description)}\n"
        "---\n\n"
    )


def write_index(pages: list[dict[str, str]], crawled_at: str, output_dir: Path) -> None:
    """Write a local Markdown index linking every successfully mirrored document."""
    lines = [
        "# Polymarket Docs Mirror",
        "",
        f"- Source index: {LLMS_URL}",
        f"- Source sitemap: {SITEMAP_URL}",
        f"- Crawled at: {crawled_at}",
        f"- Page count: {len(pages)}",
        "- Scope: official `https://docs.polymarket.com/` Markdown pages listed in `llms.txt`.",
        "- Content policy: source Markdown is mirrored verbatim after this repository-local frontmatter.",
        "",
        "## Pages",
        "",
    ]
    for page in pages:
        rel = page["path"]
        desc = f" — {page['description']}" if page["description"] else ""
        lines.append(f"- [{page['title']}]({rel}){desc}")
    (output_dir / "INDEX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    """Download the official Polymarket docs Markdown mirror."""
    crawled_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    OUT_DIR.parent.mkdir(parents=True, exist_ok=True)

    llms = curl_text(LLMS_URL)
    pages = parse_llms(llms)
    if not pages:
        raise RuntimeError(f"No Markdown pages found in {LLMS_URL}")

    with TemporaryDirectory(prefix="polymarket-docs-", dir=str(OUT_DIR.parent)) as tmp:
        staging_dir = Path(tmp)
        (staging_dir / "llms.txt").write_text(llms, encoding="utf-8")

        sitemap_error = None
        try:
            (staging_dir / "sitemap.xml").write_text(curl_text(SITEMAP_URL), encoding="utf-8")
        except subprocess.CalledProcessError as exc:
            sitemap_error = exc.stderr.strip()
            print(f"warning: failed to fetch sitemap: {sitemap_error}", file=sys.stderr)

        manifest_pages = []
        failures = []
        for idx, page in enumerate(pages, start=1):
            try:
                rel_path = page.relative_path
                target = staging_dir / rel_path
                if not target.resolve().is_relative_to(staging_dir.resolve()):
                    raise ValueError(f"Resolved path escapes staging dir: {page.url}")
                body = curl_text(page.url)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(frontmatter(page, crawled_at) + body, encoding="utf-8")
                manifest_pages.append(
                    {
                        "title": page.title,
                        "url": page.url,
                        "path": target.relative_to(staging_dir).as_posix(),
                        "description": page.description,
                    }
                )
                print(f"[{idx:03d}/{len(pages):03d}] ok {page.url}")
            except (subprocess.CalledProcessError, ValueError) as exc:
                error = str(exc).strip()
                failures.append({"url": page.url, "error": error})
                print(f"[{idx:03d}/{len(pages):03d}] FAIL {page.url}: {error}", file=sys.stderr)
            time.sleep(REQUEST_DELAY_SECS)

        write_index(manifest_pages, crawled_at, staging_dir)
        (staging_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "source_index": LLMS_URL,
                    "source_sitemap": SITEMAP_URL,
                    "crawled_at": crawled_at,
                    "page_count": len(pages),
                    "downloaded_count": len(manifest_pages),
                    "failure_count": len(failures),
                    "sitemap_error": sitemap_error,
                    "pages": manifest_pages,
                    "failures": failures,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        if failures:
            return 2

        if OUT_DIR.exists():
            shutil.rmtree(OUT_DIR)
        shutil.move(str(staging_dir), str(OUT_DIR))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
