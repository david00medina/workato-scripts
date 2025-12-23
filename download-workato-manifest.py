#!/usr/bin/env python3
"""
Export Workato packages (manifest -> package -> download) with cleanup prompts.

Flow (per selected project):
 1) Create export manifest (assets from --assets-file or auto_generate_assets via --folder-id).
 2) Export package from that manifest (poll until completed).
 3) Download the package zip.
 4) Show package JSON and prompt to delete it.
 5) Show manifest JSON and prompt to delete it.

Docs used: https://docs.workato.com/en/workato-api/recipe-lifecycle-management.html

Requirements:
  - Python 3.8+
  - requests (pip install requests)
"""

import argparse
import getpass
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import requests
    from requests import Session
except ImportError:  # pragma: no cover - dependency hint
    print("The 'requests' package is required. Install it with: pip install requests", file=sys.stderr)
    sys.exit(1)


DEFAULT_BASE_URL = "https://www.workato.com"

# Recipe lifecycle management endpoints (from official docs)
EXPORT_MANIFEST_CREATE = "/api/export_manifests"
EXPORT_MANIFEST_VIEW = "/api/export_manifests/{manifest_id}"
EXPORT_MANIFEST_DELETE = "/api/export_manifests/{manifest_id}"
PACKAGE_EXPORT = "/api/packages/export/{manifest_id}"
PACKAGE_VIEW = "/api/packages/{package_id}"
PACKAGE_DOWNLOAD = "/api/packages/{package_id}/download"
PACKAGE_DELETE = "/api/packages/{package_id}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a Workato export manifest, export & download its package, then optionally delete both."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("WORKATO_BASE_URL", DEFAULT_BASE_URL),
        help=f"Workato base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("WORKATO_API_TOKEN") or os.getenv("WORKATO_TOKEN"),
        help="Workato API token. Falls back to env WORKATO_API_TOKEN or WORKATO_TOKEN.",
    )
    parser.add_argument(
        "--output-dir",
        default="manifests",
        help="Directory where package zip files are saved (default: %(default)s).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Projects page size when listing (default: %(default)s).",
    )
    parser.add_argument(
        "--project-id",
        action="append",
        dest="project_ids",
        help="Project ID(s) to process without prompting. May be repeated.",
    )
    parser.add_argument(
        "--manifest-name",
        help="Override export manifest name. Defaults to '<project>-manifest-<timestamp>'.",
    )
    parser.add_argument(
        "--assets-file",
        type=Path,
        help=(
            "Path to JSON file containing export_manifest.assets array. "
            "When provided, assets are used as-is."
        ),
    )
    parser.add_argument(
        "--folder-id",
        type=int,
        help=(
            "Folder ID for auto_generate_assets. When assets-file is not provided, "
            "the manifest is auto-built from this folder."
        ),
    )
    parser.add_argument(
        "--include-tags",
        action="store_true",
        help="Set include_tags=true on the manifest (propagates into package).",
    )
    parser.add_argument(
        "--include-test-cases",
        action="store_true",
        help="Set include_test_cases=true when auto-generating assets.",
    )
    parser.add_argument(
        "--include-data",
        action="store_true",
        help="Set include_data=true when auto-generating assets.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=3,
        help="Seconds between polling package status during export (default: %(default)s).",
    )
    parser.add_argument(
        "--poll-timeout",
        type=int,
        default=180,
        help="Seconds before giving up waiting for export to finish (default: %(default)s).",
    )
    parser.add_argument(
        "--output-zip-name",
        help="Override downloaded package filename (defaults to server-provided).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompts for deletions.",
    )
    return parser.parse_args()


def ensure_token(token: Optional[str]) -> str:
    if token:
        return token.strip()
    entered = getpass.getpass("Enter Workato API token: ").strip()
    if not entered:
        print("A Workato API token is required.", file=sys.stderr)
        sys.exit(1)
    return entered


def build_session(token: str) -> Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


def normalize_base(url: str) -> str:
    return url.rstrip("/")


def fallback_base_urls(user_base: str) -> List[str]:
    # Try user-specified base first, then common host variants to dodge 404s
    candidates = [normalize_base(user_base or DEFAULT_BASE_URL)]
    for alt in ("https://www.workato.com", "https://app.workato.com", "https://www.workato.eu"):
        alt_norm = normalize_base(alt)
        if alt_norm not in candidates:
            candidates.append(alt_norm)
    return candidates


def extract_projects(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        for key in ("projects", "data", "items", "records", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [p for p in value if isinstance(p, dict)]
    raise ValueError("Unable to extract projects from API response.")


def fetch_projects(session: Session, base_url: str, page_size: int) -> List[Dict[str, Any]]:
    projects: List[Dict[str, Any]] = []
    page = 1
    base_url = base_url.rstrip("/")
    while True:
        url = f"{base_url}/api/projects"
        params = {"page": page, "per_page": page_size}
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 404 and page == 1:
            raise RuntimeError(f"Projects endpoint not found at {url}")
        resp.raise_for_status()
        batch = extract_projects(resp.json())
        if not batch:
            break
        projects.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return projects


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-")
    return cleaned or "project"


def parse_selection(selection: str, total: int) -> List[int]:
    if selection.lower() in {"a", "all"}:
        return list(range(total))

    picked: List[int] = []
    for part in selection.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            if start_str.isdigit() and end_str.isdigit():
                start, end = int(start_str), int(end_str)
                picked.extend(range(min(start, end), max(start, end) + 1))
            continue
        if part.isdigit():
            picked.append(int(part))
    # Convert to zero-based and deduplicate while preserving order
    seen = set()
    normalized: List[int] = []
    for idx in picked:
        zero_based = idx - 1
        if 0 <= zero_based < total and zero_based not in seen:
            seen.add(zero_based)
            normalized.append(zero_based)
    return normalized


def prompt_project_selection(projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not projects:
        raise RuntimeError("No projects found in the workspace.")

    print("Projects in workspace:")
    for i, project in enumerate(projects, start=1):
        name = project.get("name") or project.get("title") or "(no name)"
        pid = project.get("id") or project.get("project_id")
        print(f"[{i}] {name} (id={pid})")

    selection = input("Select project numbers (e.g. 1,3-4 or 'a' for all): ").strip()
    selected_indexes = parse_selection(selection, len(projects))
    if not selected_indexes:
        print("No valid selection made. Exiting.", file=sys.stderr)
        sys.exit(1)
    return [projects[i] for i in selected_indexes]


def find_projects_by_id(
    project_ids: Iterable[str], projects: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    lookup = {str(p.get("id") or p.get("project_id")): p for p in projects}
    selected: List[Dict[str, Any]] = []
    for pid in project_ids:
        key = str(pid)
        project = lookup.get(key)
        if project:
            selected.append(project)
        else:
            selected.append({"id": key, "name": f"project-{key}"})
    return selected


def load_assets_from_file(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict) and "assets" in data:
        assets = data["assets"]
    else:
        assets = data
    if not isinstance(assets, list):
        raise ValueError("assets-file must contain a list or an object with 'assets'.")
    return assets


def prompt_folder_id(project: Dict[str, Any]) -> Optional[int]:
    project_name = project.get("name") or project.get("title") or "(no name)"
    pid = project.get("id") or project.get("project_id") or ""
    project_folder_raw = project.get("folder_id")
    proj_folder_id = int(project_folder_raw) if project_folder_raw is not None else 0
    entered = input(
        f"Enter folder_id to auto-generate assets for project '{project_name}' "
        f"(project id={pid}, project folder_id={proj_folder_id}). "
        f"Press Enter to use project folder_id ({proj_folder_id}): "
    ).strip()
    if not entered:
        return proj_folder_id
    if not entered.isdigit():
        raise ValueError("folder_id must be numeric.")
    return int(entered)


def build_manifest_payload(
    project_name: str,
    args: argparse.Namespace,
    folder_id: Optional[int],
) -> Dict[str, Any]:
    manifest_name = args.manifest_name or f"{slugify(project_name)}-manifest-{int(time.time())}"
    payload: Dict[str, Any] = {
        "export_manifest": {
            "name": manifest_name,
            "include_tags": bool(args.include_tags),
        }
    }

    if args.assets_file:
        payload["export_manifest"]["assets"] = load_assets_from_file(args.assets_file)
    else:
        if folder_id is None:
            folder_id = 0  # default root folder if project folder_id unavailable
        payload["export_manifest"]["auto_generate_assets"] = True
        payload["export_manifest"]["folder_id"] = folder_id
        if args.include_test_cases:
            payload["export_manifest"]["include_test_cases"] = True
        if args.include_data:
            payload["export_manifest"]["include_data"] = True
    return payload


def create_export_manifest(
    session: Session, base_url: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{EXPORT_MANIFEST_CREATE}"
    resp = session.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    result = data.get("result") or data
    if "id" not in result:
        raise RuntimeError(f"Unexpected manifest response: {data}")
    return result


def export_package(session: Session, base_url: str, manifest_id: Any) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{PACKAGE_EXPORT.format(manifest_id=manifest_id)}"
    resp = session.post(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_package(session: Session, base_url: str, package_id: Any) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{PACKAGE_VIEW.format(package_id=package_id)}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def wait_for_package(
    session: Session,
    base_url: str,
    package_id: Any,
    poll_interval: int,
    poll_timeout: int,
) -> Dict[str, Any]:
    deadline = time.time() + poll_timeout
    last = get_package(session, base_url, package_id)
    while time.time() < deadline:
        status = (last.get("status") or "").lower()
        if status in {"completed", "failed"}:
            return last
        time.sleep(max(1, poll_interval))
        last = get_package(session, base_url, package_id)
    raise TimeoutError(f"Package {package_id} did not complete within {poll_timeout}s")


def download_package_zip(
    session: Session,
    base_url: str,
    package: Dict[str, Any],
    output_dir: Path,
    override_name: Optional[str] = None,
) -> Path:
    package_id = package.get("id")
    if not package_id:
        raise RuntimeError(f"Package payload missing id: {package}")

    download_endpoint = f"{base_url.rstrip('/')}{PACKAGE_DOWNLOAD.format(package_id=package_id)}"

    def _fetch(url: str) -> requests.Response:
        resp = session.get(url, timeout=60, stream=True, allow_redirects=True)
        resp.raise_for_status()
        return resp

    try:
        resp = _fetch(download_endpoint)
    except requests.HTTPError as exc:
        # Retry once on client errors that can stem from short-lived redirects
        if exc.response is not None and exc.response.status_code in {400, 403}:
            resp = _fetch(download_endpoint)
        else:
            raise

    filename = override_name or f"package-{package_id}.zip"
    cd = resp.headers.get("Content-Disposition")
    if not override_name and cd:
        match = re.search(r'filename=\"?([^\";]+)\"?', cd)
        if match:
            filename = match.group(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    with output_path.open("wb") as fh:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)
    return output_path


def view_manifest(session: Session, base_url: str, manifest_id: Any) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{EXPORT_MANIFEST_VIEW.format(manifest_id=manifest_id)}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("result") or data


def delete_package(session: Session, base_url: str, package_id: Any) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{PACKAGE_DELETE.format(package_id=package_id)}"
    resp = session.delete(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def delete_manifest(session: Session, base_url: str, manifest_id: Any) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{EXPORT_MANIFEST_DELETE.format(manifest_id=manifest_id)}"
    resp = session.delete(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def prompt_yes_no(message: str, default_no: bool = True, assume_yes: bool = False) -> bool:
    if assume_yes:
        return True
    suffix = " [y/N]: " if default_no else " [Y/n]: "
    choice = input(f"{message}{suffix}").strip().lower()
    if not choice:
        return not default_no
    return choice in {"y", "yes"}


def main() -> None:
    args = parse_args()
    token = ensure_token(args.token)
    session = build_session(token)
    projects = fetch_projects(session, args.base_url, args.page_size)

    if args.project_ids:
        selected_projects = find_projects_by_id(args.project_ids, projects)
    else:
        selected_projects = prompt_project_selection(projects)

    output_dir = Path(args.output_dir)

    for project in selected_projects:
        project_name = project.get("name") or project.get("title") or project.get("id")
        try:
            folder_id = args.folder_id
            if not args.assets_file and folder_id is None:
                if args.yes:
                    project_folder_raw = project.get("folder_id")
                    folder_id = int(project_folder_raw) if project_folder_raw is not None else 0
                    print(f"Using project folder_id={folder_id} for '{project_name}' (auto-selected).")
                else:
                    folder_id = prompt_folder_id(project)
            manifest_payload = build_manifest_payload(project_name or "project", args, folder_id)
        except Exception as exc:  # noqa: BLE001
            print(f"Skipping project '{project_name}': {exc}", file=sys.stderr)
            continue

        manifest = None
        working_base = None
        for candidate_base in fallback_base_urls(args.base_url):
            try:
                manifest = create_export_manifest(session, candidate_base, manifest_payload)
                working_base = candidate_base
                break
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    # Try the next base URL on hard 404s (path not found)
                    continue
                raise
        if manifest is None or working_base is None:
            print(
                f"Failed to create export manifest for '{project_name}': "
                f"No base URL worked (tried {fallback_base_urls(args.base_url)}).",
                file=sys.stderr,
            )
            continue
        manifest_id = manifest["id"]
        print(f"Created export manifest {manifest_id} for project '{project_name}' (base: {working_base}).")

        try:
            package_meta = export_package(session, working_base, manifest_id)
            package_id = package_meta.get("id")
            if not package_id:
                raise RuntimeError(f"Unexpected package export response: {package_meta}")
            print(f"Started package export {package_id} from manifest {manifest_id} for '{project_name}'.")
            package_final = wait_for_package(
                session,
                working_base,
                package_id,
                args.poll_interval,
                args.poll_timeout,
            )
            status = package_final.get("status")
            print(f"Package {package_id} status: {status}")
            if status != "completed":
                raise RuntimeError(f"Package export failed or incomplete: {package_final}")
            zip_path = download_package_zip(
                session,
                working_base,
                package_final,
                output_dir=output_dir,
                override_name=args.output_zip_name,
            )
            print(f"Downloaded package {package_id} to {zip_path}")
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to export/download package for '{project_name}': {exc}", file=sys.stderr)
            continue

        # Confirm and delete package
        try:
            package_latest = get_package(session, working_base, package_id)
            package_json = json.dumps(package_latest, indent=2)
            print(f"Package details (review before delete):\n{package_json}")
            if prompt_yes_no(
                f"Delete package {package_id} now?",
                default_no=True,
                assume_yes=args.yes,
            ):
                delete_resp = delete_package(session, working_base, package_id)
                print(f"Deleted package {package_id}: {delete_resp}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error while deleting package {package_id}: {exc}", file=sys.stderr)

        # Confirm and delete manifest
        try:
            manifest_view = view_manifest(session, working_base, manifest_id)
            manifest_json = json.dumps(manifest_view, indent=2)
            print(f"Manifest details (review before delete):\n{manifest_json}")
            if prompt_yes_no(
                f"Delete export manifest {manifest_id} now?",
                default_no=True,
                assume_yes=args.yes,
            ):
                delete_resp = delete_manifest(session, working_base, manifest_id)
                print(f"Deleted manifest {manifest_id}: {delete_resp}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error while deleting manifest {manifest_id}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
