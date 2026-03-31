#!/usr/bin/env python3
"""
CV Builder: Markdown sections → LaTeX → PDF via tectonic.

Converts markdown files in sections/ to LaTeX fragments using a custom
CV-aware converter, injects them into a LaTeX template with config from
cv.toml, and compiles to PDF with tectonic.

Markdown conventions:
    ### Title | Location | Date   →  \\textbf{Title} \\hfill Location \\textbar\\ Date
    ### Title | Right              →  \\textbf{Title} \\hfill Right
    ### Title                      →  \\textbf{Title}
    **bold text**                  →  \\textbf{bold text}
    *italic text*                  →  \\textit{italic text}
    [text](url)                    →  \\href{url}{text}
    `code`                         →  \\verb|code|
    - list item                    →  \\begin{itemize} ... \\end{itemize}
    Text | Right-aligned           →  Text \\hfill Right-aligned
    & (ampersand)                  →  \\& (auto-escaped)
"""

import os
import re
import sys
import subprocess
import tomllib
from pathlib import Path


# ---------------------------------------------------------------------------
# Markdown → LaTeX converter
# ---------------------------------------------------------------------------

def escape_latex(text: str) -> str:
    """Escape LaTeX special characters that commonly appear in CV text."""
    text = text.replace('&', '\\&')
    return text


def convert_inline(text: str) -> str:
    """Convert inline markdown formatting to LaTeX."""
    # Bold: **text** → \textbf{text}  (process before italic)
    text = re.sub(r'\*\*(.+?)\*\*', r'\\textbf{\1}', text)
    # Italic: *text* → \textit{text}
    text = re.sub(r'(?<!\*)\*([^*]+?)\*(?!\*)', r'\\textit{\1}', text)
    # Links: [text](url) → \href{url}{text}
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'\\href{\2}{\1}', text)
    # Inline code: `text` → \verb|text|
    text = re.sub(r'`(.+?)`', r'\\verb|\1|', text)
    return text


def convert_section(text: str) -> str:
    """Convert a markdown section to LaTeX fragment."""
    lines = text.split('\n')
    output = []
    i = 0

    while i < len(lines):
        line = lines[i].rstrip()

        # Blank line → empty line (paragraph break)
        if not line:
            output.append('')
            i += 1
            continue

        # Raw LaTeX passthrough (lines starting with \)
        if line.startswith('\\'):
            output.append(line)
            i += 1
            continue

        # H3 header: ### Title | Location | Date
        if line.startswith('### '):
            content = line[4:]
            parts = [p.strip() for p in content.split('|')]
            parts = [convert_inline(escape_latex(p)) for p in parts]

            if len(parts) == 3:
                output.append(
                    f'\\textbf{{{parts[0]}}} \\hfill {parts[1]} '
                    f'\\textbar\\ {parts[2]}'
                )
            elif len(parts) == 2:
                output.append(f'\\textbf{{{parts[0]}}} \\hfill {parts[1]}')
            else:
                output.append(f'\\textbf{{{parts[0]}}}')
            i += 1
            continue

        # List items: group consecutive "- " lines into \begin{itemize}
        if line.startswith('- '):
            output.append(
                '\\begin{itemize}'
                '[noitemsep, topsep=0pt, partopsep=0pt, parsep=0pt]'
            )
            while i < len(lines) and lines[i].rstrip().startswith('- '):
                item = lines[i].rstrip()[2:]
                item = convert_inline(escape_latex(item))
                output.append(f'    \\item {item}')
                i += 1
            output.append('\\end{itemize}')
            continue

        # Plain text line with | separator (e.g., education dates)
        if '|' in line and not line.startswith('**'):
            parts = [p.strip() for p in line.split('|')]
            if len(parts) == 2:
                left = convert_inline(escape_latex(parts[0]))
                right = escape_latex(parts[1])
                output.append(f'{left} \\hfill {right}')
                i += 1
                continue

        # Regular line — escape and convert inline formatting
        line = escape_latex(line)
        line = convert_inline(line)
        output.append(line)
        i += 1

    return '\n'.join(output)


# ---------------------------------------------------------------------------
# Build pipeline
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    """Load cv.toml configuration."""
    with open(config_path, 'rb') as f:
        return tomllib.load(f)


def build_sections(config: dict, sections_dir: Path) -> str:
    """Convert all markdown sections to LaTeX and assemble them."""
    section_order = config.get('sections', {}).get('order', [])
    section_configs = config.get('sections', {})
    parts = []

    for i, name in enumerate(section_order):
        md_path = sections_dir / f'{name}.md'
        if not md_path.exists():
            print(f'  ⚠ Section not found: {md_path}', file=sys.stderr)
            continue

        section_cfg = section_configs.get(name, {})
        title = section_cfg.get('title', name.title())
        pagebreak = section_cfg.get('pagebreak_before', False)

        md_text = md_path.read_text()
        latex_fragment = convert_section(md_text)

        section_latex = ''

        # Page break before this section if configured
        if pagebreak:
            section_latex += '\\newpage\n\n'

        # Spacing between sections (not before the first one)
        if i > 0 and not pagebreak:
            section_latex += '\\vspace{8pt}\n\n'

        # Section header
        section_latex += (
            '\\begin{center}\n'
            f'    \\textbf{{{title}}}\n'
            '    \\hrulefill\n'
            '\\end{center}\n'
        )

        # Section content
        section_latex += latex_fragment

        parts.append(section_latex)

    return '\n'.join(parts)


def fill_template(
    template_path: Path,
    config: dict,
    sections_latex: str,
) -> str:
    """Fill the LaTeX template with config values and section content."""
    template = template_path.read_text()

    # Replace %CONFIG:field% markers with profile values
    profile = config.get('profile', {})
    for key, value in profile.items():
        marker = f'%CONFIG:{key}%'
        template = template.replace(marker, str(value))

    # Replace %SECTIONS% with assembled section content
    template = template.replace('%SECTIONS%', sections_latex)

    return template


def compile_pdf(tex_path: Path, output_dir: Path) -> bool:
    """Compile LaTeX to PDF using tectonic."""
    # Ensure tectonic is available
    tectonic = os.environ.get('TECTONIC_PATH', 'tectonic')

    # Check common locations if not in PATH
    if tectonic == 'tectonic':
        local_bin = Path.home() / '.local' / 'bin' / 'tectonic'
        if local_bin.exists():
            tectonic = str(local_bin)

    cmd = [tectonic, str(tex_path), '-o', str(output_dir)]
    print(f'  → {" ".join(cmd)}')

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f'  ✗ tectonic failed:\n{result.stderr}', file=sys.stderr)
        return False

    return True


def main():
    # Resolve paths relative to the project root (where cv.toml lives)
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / 'cv.toml'
    sections_dir = project_root / 'sections'
    build_dir = project_root / '.build'
    output_dir = project_root / 'output'

    # Allow overriding config path via CLI argument (skip flags)
    positional_args = [a for a in sys.argv[1:] if not a.startswith('--')]
    if positional_args:
        config_path = Path(positional_args[0])
        project_root = config_path.parent

    # Load config
    if not config_path.exists():
        print(f'✗ Config not found: {config_path}', file=sys.stderr)
        sys.exit(1)

    print(f'📄 Loading config: {config_path}')
    config = load_config(config_path)

    # Determine template
    template_name = config.get('build', {}).get('template', 'default')
    template_path = project_root / 'templates' / f'{template_name}.tex.template'
    if not template_path.exists():
        print(f'✗ Template not found: {template_path}', file=sys.stderr)
        sys.exit(1)

    # Determine output path
    output_pdf = config.get('build', {}).get('output', 'output/cv.pdf')
    output_path = project_root / output_pdf
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build
    build_dir.mkdir(parents=True, exist_ok=True)
    tex_path = build_dir / 'cv.tex'

    print('🔨 Converting markdown sections to LaTeX...')
    sections_latex = build_sections(config, sections_dir)

    print('📋 Filling template...')
    full_latex = fill_template(template_path, config, sections_latex)
    tex_path.write_text(full_latex)
    print(f'  → {tex_path}')

    # Check for --dry-run flag
    if '--dry-run' in sys.argv:
        print(f'✅ Dry run complete. LaTeX written to: {tex_path}')
        return

    print('📄 Compiling PDF with tectonic...')
    if compile_pdf(tex_path, output_path.parent):
        # Rename if output filename differs from cv.pdf
        compiled = output_path.parent / 'cv.pdf'
        if compiled != output_path and compiled.exists():
            compiled.rename(output_path)
        print(f'✅ Done: {output_path}')
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
