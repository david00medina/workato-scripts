import json
from typing import Dict

import pystache


def main(template: str, data: dict) -> Dict[str, str]:
    data["system_name"] = "Workato"
    data["job_id"] = data.get("job_url", "unknown").split("/")[-1]
    data["recipe_id"] = data.get("recipe_url", "unknown").split("/")[-1]
    rendered_html = pystache.render(template, data)
    return { 'error_body':  rendered_html }


if __name__ == '__main__':
    with open('data/error_template_v1.html', 'r') as t, open('data/error_message_data.json', 'r') as d:
        template = t.read()
        data = json.load(d)
    error_body = main(template, data)

    with open('data/error_message_v1_imputed.html', 'w') as f:
        f.write(error_body['error_body'])
    print(error_body)
    pass