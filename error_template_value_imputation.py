import json
from dataclasses import dataclass
from html import escape
from typing import Any, Dict, List, Sequence


@dataclass
class VariableToken:
    name: str


@dataclass
class SectionToken:
    name: str
    tokens: List[Any]
    inverted: bool = False


Token = Any


def parse_template(template: str) -> List[Token]:
    tokens: List[Token] = []
    stack: List[List[Token]] = [tokens]
    section_stack: List[SectionToken] = []
    idx = 0

    while idx < len(template):
        start = template.find("{{", idx)
        if start == -1:
            stack[-1].append(template[idx:])
            break

        if start > idx:
            stack[-1].append(template[idx:start])

        end = template.find("}}", start)
        if end == -1:
            raise ValueError("Unclosed tag in template")

        tag_content = template[start + 2 : end].strip()
        idx = end + 2

        if not tag_content:
            continue

        marker = tag_content[0]
        if marker in ("#", "^"):
            name = tag_content[1:].strip()
            section = SectionToken(name=name, tokens=[], inverted=marker == "^")
            stack[-1].append(section)
            stack.append(section.tokens)
            section_stack.append(section)
        elif marker == "/":
            name = tag_content[1:].strip()
            if not section_stack or section_stack[-1].name != name:
                raise ValueError(f"Mismatched closing tag: {name}")
            section_stack.pop()
            stack.pop()
        else:
            stack[-1].append(VariableToken(name=tag_content))

    if section_stack:
        unclosed = ", ".join(section.name for section in section_stack)
        raise ValueError(f"Unclosed sections: {unclosed}")

    return tokens


def is_truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    if isinstance(value, str):
        return len(value) > 0
    return bool(value)


def resolve_name(name: str, context_stack: Sequence[Any]) -> Any:
    if name == ".":
        return context_stack[-1]

    parts = name.split(".")
    for context in reversed(context_stack):
        value = context
        matched = True
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            elif not isinstance(
                value, (str, bytes, int, float, bool, list, tuple, set, dict)
            ) and hasattr(value, part):
                value = getattr(value, part)
            else:
                matched = False
                break
        if matched:
            return value
    return None


def render_tokens(tokens: Sequence[Token], context_stack: List[Any]) -> str:
    rendered: List[str] = []

    for token in tokens:
        if isinstance(token, str):
            rendered.append(token)
        elif isinstance(token, VariableToken):
            value = resolve_name(token.name, context_stack)
            rendered.append("" if value is None else escape(str(value)))
        elif isinstance(token, SectionToken):
            value = resolve_name(token.name, context_stack)
            if token.inverted:
                if not is_truthy(value):
                    rendered.append(render_tokens(token.tokens, context_stack.copy()))
            else:
                if isinstance(value, (list, tuple)):
                    for item in value:
                        rendered.append(render_tokens(token.tokens, context_stack + [item]))
                elif isinstance(value, dict):
                    rendered.append(render_tokens(token.tokens, context_stack + [value]))
                elif is_truthy(value):
                    rendered.append(render_tokens(token.tokens, context_stack + [value]))
        else:
            raise TypeError(f"Unknown token type: {token}")

    return "".join(rendered)


def render(template: str, data: Dict[str, Any]) -> str:
    tokens = parse_template(template)
    return render_tokens(tokens, [data])


def main(input: dict) -> Dict[str, str]:
    template = input.get("template", "")
    data = input.get("data", {})
    data["system_name"] = "Workato"
    data["job_id"] = data.get("job_url", "unknown").split("/")[-1]
    data["recipe_id"] = data.get("recipe_url", "unknown").split("/")[-1]
    rendered_html = render(template, data)
    return {'error_body': rendered_html}


if __name__ == '__main__':
    with open('data/error_template_v1.html', 'r') as t, open('data/error_message_data.json', 'r') as d:
        template = t.read()
        data = json.load(d)
    error_body = main(template, data)

    with open('data/error_message_v1_imputed.html', 'w') as f:
        f.write(error_body['error_body'])
    print(error_body)
    pass
