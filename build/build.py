#!/usr/bin/env python3
"""
CV Builder: Markdown sections → LaTeX → PDF via tectonic.

    Convert markdown files in sections/ to LaTeX fragments using a custom
    CV-aware converter targeting the Steve Nguyen (my_template) style, injects
    them into a LaTeX template with config from cv.toml, and compiles to PDF
    with tectonic.

    hint.md is reference notes only — it is NOT compiled. Copy content from
    hint.md into sections/*.md, then run `make`.

Section-specific conversion:
    summary   → plain paragraph text
    education → \\resumeEducationHeading / \\resumeSubheading
    skills    → key-value list (\\textbf{Key:} values)
    experience→ \\resumeSubheading with \\resumeItem bullets
    projects  → \\resumeProjectHeading with \\resumeItem bullets

Markdown conventions:
    ### Title | Location | Date   →  section-specific heading
    **bold text**                  →  \\textbf{bold text}
    *italic text*                  →  \\textit{italic text}
    [text](url)                    →  \\href{url}{text}
    `code`                         →  \\texttt{code}
    - list item                    →  \\resumeItem{...}
    & (ampersand)                  →  \\& (auto-escaped)
"""

import os
import re
import sys
import subprocess
import tomllib
from pathlib import Path


# ---------------------------------------------------------------------------
# Markdown → LaTeX converter (arasgungore style)
# ---------------------------------------------------------------------------

def escape_latex(text: str) -> str:
    """Escape LaTeX special characters that commonly appear in CV text.

    Note: This should be called AFTER convert_inline() to avoid escaping
    characters inside LaTeX commands. However, since we process links
    separately, we handle & and # here but NOT underscores (those are
    handled contextually).
    """
    text = text.replace('&', '\\&')
    text = text.replace('#', '\\#')
    # Normalize markdown-escaped underscores back to plain before escaping
    text = text.replace('\\_', '_')
    text = text.replace('_', '\\_')
    text = text.replace('~', '{\\raise.17ex\\hbox{$\\scriptstyle\\sim$}}')
    return text


def _escape_text_only(text: str) -> str:
    """Escape LaTeX special chars in text segments only (not in URLs)."""
    text = text.replace('%', '\\%')
    text = text.replace('&', '\\&')
    text = text.replace('#', '\\#')
    text = text.replace('\\_', '_')
    text = text.replace('_', '\\_')
    text = text.replace('~', '{\\raise.17ex\\hbox{$\\scriptstyle\\sim$}}')
    # Convert Unicode dashes to LaTeX equivalents (em-dash before en-dash)
    text = text.replace('\u2014', '---')
    text = text.replace('\u2013', '--')
    return text


def convert_inline(text: str) -> str:
    """Convert inline markdown formatting to LaTeX.

    Processes links first (to protect URLs from escaping), then extracts
    and protects those, escapes remaining text, and applies bold/italic/code.
    """
    # Step 1: Extract and protect markdown links [text](url)
    link_placeholders = {}
    counter = [0]

    def _replace_link(m):
        link_text = m.group(1)
        url = m.group(2)
        key = f'@@LINK{counter[0]}@@'
        counter[0] += 1
        # Escape only the display text, not the URL
        escaped_text = _escape_text_only(link_text)
        # Apply bold/italic to the link text
        escaped_text = re.sub(r'\*\*(.+?)\*\*', r'\\textbf{\1}', escaped_text)
        escaped_text = re.sub(
            r'(?<!\*)\*([^*]+?)\*(?!\*)', r'\\textit{\1}', escaped_text
        )
        link_placeholders[key] = f'\\href{{{url}}}{{\\color{{blue}}{escaped_text}}}'
        return key

    text = re.sub(r'\[(.+?)\]\((.+?)\)', _replace_link, text)

    # Step 2: Extract and protect inline code `text`
    code_placeholders = {}
    code_counter = [0]

    def _replace_code(m):
        code_text = m.group(1)
        key = f'@@CODE{code_counter[0]}@@'
        code_counter[0] += 1
        code_placeholders[key] = f'\\texttt{{{code_text}}}'
        return key

    text = re.sub(r'`(.+?)`', _replace_code, text)

    # Step 3: Escape remaining text
    text = _escape_text_only(text)

    # Step 4: Convert bold and italic
    text = re.sub(r'\*\*(.+?)\*\*', r'\\textbf{\1}', text)
    text = re.sub(r'(?<!\*)\*([^*]+?)\*(?!\*)', r'\\textit{\1}', text)

    # Step 5: Restore placeholders
    for key, val in link_placeholders.items():
        text = text.replace(key, val)
    for key, val in code_placeholders.items():
        text = text.replace(key, val)

    return text


def _strip_link_md(text: str) -> tuple[str, str | None]:
    """Extract title and optional URL from markdown link syntax.

    Returns (title, url) where url may be None.
    Example: '[My Project](https://...)' → ('My Project', 'https://...')
             'Plain Title' → ('Plain Title', None)
    """
    m = re.match(r'^\[(.+?)\]\((.+?)\)$', text.strip())
    if m:
        return m.group(1), m.group(2)
    return text.strip(), None


# ---------------------------------------------------------------------------
# Section-specific converters
# ---------------------------------------------------------------------------

def convert_summary(text: str) -> str:
    """Convert summary section — plain paragraph text."""
    lines = text.strip().split('\n')
    output = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        line = convert_inline(line)
        output.append(line)
    return (
        '\\vspace{2pt}\n'
        '\\resumeSubHeadingListStart\n'
        '  \\small{\\item{\n'
        '    ' + ' '.join(output) + '\n'
        '  }}\n'
        '\\resumeSubHeadingListEnd'
    )


def _is_tech_line(text: str) -> str | None:
    """Return tech list if line is an @tech marker, else None."""
    if text.startswith('@tech '):
        return text[6:].strip()
    return None


def _emit_bullets(output: list, bullets: list) -> None:
    """Emit resumeItem bullets, with optional trailing @tech → resumeTechUsedItem."""
    regular = []
    tech = None
    for b in bullets:
        t = _is_tech_line(b)
        if t is not None:
            tech = convert_inline(t)
        else:
            regular.append(b)

    if regular:
        output.append('        \\resumeItemListStart')
        for b in regular:
            output.append(f'            \\resumeItem{{{b}}}')
        if tech:
            output.append(f'            \\resumeTechUsedItem{{{tech}}}')
        output.append('        \\resumeItemListEnd')
    elif tech:
        output.append('        \\resumeItemListStart')
        output.append(f'            \\resumeTechUsedItem{{{tech}}}')
        output.append('        \\resumeItemListEnd')


def convert_education(text: str) -> str:
    """Convert education section.

    Expected markdown format:
        ### University Name | Location
        Degree, GPA | Date Range
        *Additional note*
    """
    lines = [l.rstrip() for l in text.strip().split('\n') if l.strip()]
    output = ['\\vspace{3pt}', '\\resumeSubHeadingListStart']

    i = 0
    while i < len(lines):
        line = lines[i]

        if line.startswith('### '):
            content = line[4:]
            parts = [p.strip() for p in content.split('|')]
            uni_name = _escape_text_only(parts[0])
            location = _escape_text_only(parts[1]) if len(parts) > 1 else ''

            degree_line = ''
            date_line = ''
            note_line = ''

            if i + 1 < len(lines) and not lines[i + 1].startswith('### '):
                i += 1
                next_line = lines[i]
                if next_line.startswith('*') and next_line.endswith('*'):
                    note_line = next_line
                elif '|' in next_line:
                    deg_parts = [p.strip() for p in next_line.split('|')]
                    degree_line = convert_inline(deg_parts[0])
                    date_line = _escape_text_only(deg_parts[1]) if len(deg_parts) > 1 else ''
                    date_line = date_line.replace('--', '\\textbf{--}')
                else:
                    degree_line = convert_inline(next_line)

            if i + 1 < len(lines) and not lines[i + 1].startswith('### '):
                peek = lines[i + 1]
                if peek.startswith('*') and peek.endswith('*'):
                    i += 1
                    note_line = peek

            note_text = ''
            if note_line:
                note_text = convert_inline(note_line.strip('*').strip())

            output.append('')
            output.append('    \\resumeEducationHeading')
            output.append(f'      {{{uni_name}}}{{{location}}}')
            output.append(f'      {{{degree_line}}}{{{date_line}}}')
            output.append(f'      {{{note_text}}}')

        i += 1

    output.append('')
    output.append('\\resumeSubHeadingListEnd')
    return '\n'.join(output)


def convert_skills(text: str) -> str:
    """Convert skills section — key: value pairs.

    Expected markdown format:
        **Key:** value1, value2, ...
    """
    lines = [l.rstrip() for l in text.strip().split('\n') if l.strip()]
    output = ['\\vspace{2pt}', '\\resumeSubHeadingListStart', '  \\small{\\item{']

    for idx, line in enumerate(lines):
        line = convert_inline(line)
        if idx < len(lines) - 1:
            output.append(f'      {line} \\\\ \\vspace{{3pt}}')
        else:
            output.append(f'      {line}')

    output.append('  }}')
    output.append('\\resumeSubHeadingListEnd')
    return '\n'.join(output)


def convert_experience(text: str) -> str:
    """Convert experience section.

    Expected markdown format:
        ### Company | Location | Date
        **Role Title**
        - Bullet point 1
        - Bullet point 2
    """
    lines = [l.rstrip() for l in text.split('\n')]
    output = ['\\vspace{3pt}', '\\resumeSubHeadingListStart']

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        if not line:
            i += 1
            continue

        if line.startswith('### '):
            content = line[4:]
            parts = [p.strip() for p in content.split('|')]
            company = _escape_text_only(parts[0])
            location = _escape_text_only(parts[1]) if len(parts) > 1 else ''
            date = parts[2].strip() if len(parts) > 2 else ''
            # Replace -- with \textbf{--}
            date = date.replace('--', '\\textbf{--}')

            # Collect all roles under this company heading
            i += 1
            first_role = True

            while i < len(lines):
                line = lines[i].rstrip()

                # Next company heading — break
                if line.startswith('### '):
                    break

                # Role title: **Role Name**
                if line.startswith('**') and line.endswith('**'):
                    role = line.strip('*').strip()
                    role = _escape_text_only(role)

                    if first_role:
                        output.append('')
                        output.append('    \\resumeSubheading')
                        output.append(f'      {{{company}}}{{{location}}}')
                        output.append(f'      {{{role}}}{{{date}}}')
                        first_role = False
                    else:
                        output.append('')
                        output.append('    \\resumeSubSubheading')
                        output.append(f'      {{{role}}}{{}}')

                    # Collect bullet points for this role
                    i += 1
                    bullets = []
                    while i < len(lines):
                        bline = lines[i].rstrip()
                        if bline.startswith('- '):
                            item_text = convert_inline(bline[2:])
                            bullets.append(item_text)
                            i += 1
                        elif bline.startswith('@tech '):
                            bullets.append(bline)
                            i += 1
                        elif bline == '':
                            i += 1
                            # Check if next non-empty line is a role or heading
                            if i < len(lines) and (
                                lines[i].rstrip().startswith('**')
                                or lines[i].rstrip().startswith('### ')
                            ):
                                break
                            continue
                        else:
                            break

                    if bullets:
                        _emit_bullets(output, bullets)

                    continue

                # Skip empty lines between roles
                if not line:
                    i += 1
                    continue

                i += 1

            continue

        i += 1

    output.append('')
    output.append('  \\resumeSubHeadingListEnd')
    return '\n'.join(output)


def convert_projects(text: str) -> str:
    """Convert projects section.

    Expected markdown format:
        ### [Project Name](url)
        - **Stack**: tech1, tech2
        - Bullet point 1
        - Bullet point 2
    """
    lines = [l.rstrip() for l in text.split('\n')]
    output = ['\\vspace{3pt}', '\\resumeSubHeadingListStart']

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        if not line:
            i += 1
            continue

        if line.startswith('### '):
            content = line[4:].strip()
            title, url = _strip_link_md(content)
            title = _escape_text_only(title)

            if url:
                heading = (
                    f'\\textbf{{{title}}} $|$ '
                    f'\\emph{{\\href{{{url}}}{{\\color{{blue}}GitHub}}}}'
                )
            else:
                heading = f'\\textbf{{{title}}}'

            output.append('')
            output.append('      \\resumeProjectHeading')
            output.append(f'        {{{heading}}}{{}}')

            # Collect bullet points
            i += 1
            bullets = []
            while i < len(lines):
                bline = lines[i].rstrip()
                if bline.startswith('- '):
                    item_text = convert_inline(bline[2:])
                    bullets.append(item_text)
                    i += 1
                elif bline.startswith('@tech '):
                    bullets.append(bline)
                    i += 1
                elif bline == '':
                    i += 1
                    # If next line is a new project heading, break
                    if i < len(lines) and lines[i].rstrip().startswith('### '):
                        break
                    continue
                else:
                    break

            if bullets:
                _emit_bullets(output, bullets)

            continue

        i += 1

    output.append('')
    output.append('    \\resumeSubHeadingListEnd')
    return '\n'.join(output)


# Map of section names to their converter functions
SECTION_CONVERTERS = {
    'summary': convert_summary,
    'education': convert_education,
    'skills': convert_skills,
    'experience': convert_experience,
    'projects': convert_projects,
}


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

    for name in section_order:
        md_path = sections_dir / f'{name}.md'
        if not md_path.exists():
            print(f'  ⚠ Section not found: {md_path}', file=sys.stderr)
            continue

        section_cfg = section_configs.get(name, {})
        title = section_cfg.get('title', name.title())
        pagebreak = section_cfg.get('pagebreak_before', False)

        md_text = md_path.read_text()

        # Use section-specific converter if available
        converter = SECTION_CONVERTERS.get(name)
        if converter:
            latex_fragment = converter(md_text)
        else:
            # Fallback: treat as summary-style text
            latex_fragment = convert_summary(md_text)

        section_latex = ''

        # Page break before this section if configured
        if pagebreak:
            section_latex += '\\newpage\n\n'

        # Section header
        section_latex += f'\\section{{{title}}}\n'

        # Section content
        section_latex += latex_fragment

        parts.append(section_latex)

    return '\n\n\n'.join(parts)


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
