#!/usr/bin/env python3
"""Check repository guidance links/structure and the work-item form, without network access."""

import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

try:
    import yaml
except ImportError:
    sys.exit('Install PyYAML 6 in your Python environment, then rerun this command.')

ROOT = Path(__file__).resolve().parent.parent
MAIN_DOCUMENT = 'https://github.com/ginsys/parley/blob/main/'
FIELDS = {
    'work-type': ('dropdown', 'Work type', True),
    'objective': ('textarea', 'Objective and design references', True),
    'scope': ('textarea', 'Scope and exclusions', True),
    'deliverables': ('textarea', 'Deliverables', True),
    'acceptance': ('textarea', 'Acceptance criteria', True),
    'verification': ('textarea', 'Verification method', True),
    'alternatives': ('textarea', 'Alternatives and decision enabled', False),
    'outcome-links': ('textarea', 'Outcome links', False),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_text(path, root=ROOT):
    """Read repository UTF-8 without exposing checkout paths in file errors."""
    try:
        return path.read_text(encoding='utf-8')
    except OSError as error:
        raise ValueError(f'{path.relative_to(root)}: {error.strerror}') from None
    except UnicodeError:
        raise ValueError(f'{path.relative_to(root)}: invalid UTF-8') from None


def prose(path, root=ROOT):
    """Exclude fenced examples from link checks; verify fences and whitespace."""
    lines = []
    fence = None
    label = path.relative_to(root)
    for number, line in enumerate(read_text(path, root).splitlines(), 1):
        require(line == line.rstrip(), f'{label}:{number}: trailing whitespace')
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})(.*)$', line)
        if marker:
            run, suffix = marker.groups()
            if fence is None:
                fence = run
            elif run[0] == fence[0] and len(run) >= len(fence) and not suffix.strip():
                fence = None
        elif fence is None:
            lines.append(line)
    require(fence is None, f'{label}: unclosed code fence')
    return '\n'.join(lines)


def anchors(text):
    result = set()
    for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', text, re.MULTILINE):
        slug = re.sub(r'[^\w -]', '', heading.lower()).replace(' ', '-')
        candidate = slug
        suffix = 0
        while candidate in result:
            suffix += 1
            candidate = f'{slug}-{suffix}'
        result.add(candidate)
    return result


def check_markdown(path, root=ROOT):
    label = path.relative_to(root)
    for destination in re.findall(r'\[[^\]\n]+\]\(([^)\s]+)\)', prose(path, root)):
        if destination.startswith(MAIN_DOCUMENT):
            parts = urlsplit(destination[len(MAIN_DOCUMENT):])
            target = root / unquote(parts.path)
        else:
            parts = urlsplit(destination)
            if parts.scheme or parts.netloc:
                continue
            target = path.parent / unquote(parts.path) if parts.path else path
        target = target.resolve()
        require(target.is_relative_to(root.resolve()), f'{label}: link escapes repository: {destination}')
        require(target.exists(), f'{label}: missing link target: {destination}')
        if parts.fragment and target.suffix == '.md':
            require(unquote(parts.fragment) in anchors(prose(target, root)), f'{label}: missing anchor: {destination}')


def check_form(root=ROOT):
    templates = root / '.github/ISSUE_TEMPLATE'
    form = yaml.safe_load(read_text(templates / 'work-item.yml', root))
    require(isinstance(form, dict) and form.get('name') and form.get('description'), 'Form needs a name and description')
    elements = form.get('body')
    require(isinstance(elements, list), 'Form body must be a list')
    fields = {}
    for element in elements:
        require(isinstance(element, dict), 'Form elements must be mappings')
        if element.get('type') == 'markdown':
            require(element.get('attributes', {}).get('value'), 'Markdown element needs content')
            continue
        key = element.get('id')
        require(key in FIELDS and key not in fields, f'Unknown or duplicate form field: {key}')
        fields[key] = element
    require(fields.keys() == FIELDS.keys(), 'Missing required form sections')
    for key, (kind, label, required) in FIELDS.items():
        field = fields[key]
        require(field.get('type') == kind and field.get('attributes', {}).get('label') == label,
                f'{key}: incorrect type or section label')
        require(field.get('validations', {}).get('required') is required, f'{key}: incorrect requiredness')
    require(fields['work-type']['attributes'].get('options') ==
            ['Investigation', 'Decision', 'Specification', 'Implementation', 'Validation'], 'Incorrect work types')
    require(yaml.safe_load(read_text(templates / 'config.yml', root)) == {'blank_issues_enabled': False},
            'Blank issues must remain disabled')


def main():
    documents = [ROOT / name for name in ('README.md', 'CONTRIBUTING.md', 'AGENTS.md', 'CLAUDE.md')]
    documents = sorted(set(documents) | set((ROOT / 'docs').rglob('*.md')) if (ROOT / 'docs').is_dir() else set(documents))
    for document in documents:
        check_markdown(document)
    check_form()
    require(read_text(ROOT / 'CLAUDE.md') == '@AGENTS.md\n', 'CLAUDE.md must delegate to AGENTS.md')
    required = sum(field[2] for field in FIELDS.values())
    print(f'PASS: {len(documents)} guidance/specification documents; local links/anchors, fences, whitespace; '
          f'issue-form YAML and {len(FIELDS)} fields ({required} required, {len(FIELDS) - required} optional); '
          'blank issues disabled; CLAUDE delegation. '
          'External URLs, live tracker state and design semantics require separate review.')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, yaml.YAMLError) as error:
        sys.exit(str(error))
