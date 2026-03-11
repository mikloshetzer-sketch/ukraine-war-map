import json
import os
import zipfile
from pathlib import Path
from datetime import datetime, timezone

import requests

TOKEN_URL = "https://public-api.wordpress.com/oauth2/token"
TOKEN_INFO_URL = "https://public-api.wordpress.com/oauth2/token-info"
ME_URL = "https://public-api.wordpress.com/rest/v1.1/me"

OUT_DIR = Path("wpcom_token_bundle")
OUT_ZIP = Path("wpcom_token_bundle.zip")


def get_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def exchange_code(
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    authorization_code: str,
) -> dict:
    payload = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "client_secret": client_secret,
        "code": authorization_code,
        "grant_type": "authorization_code",
    }

    resp = requests.post(TOKEN_URL, data=payload, timeout=60)
    if resp.status_code >= 300:
        raise SystemExit(f"Token exchange failed ({resp.status_code}): {resp.text}")

    data = resp.json()

    if "access_token" not in data:
        raise SystemExit(f"No access_token in response: {json.dumps(data, ensure_ascii=False)}")

    return data


def get_token_info(access_token: str) -> dict:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(TOKEN_INFO_URL, headers=headers, timeout=60)

    if resp.status_code >= 300:
        raise SystemExit(f"Token info failed ({resp.status_code}): {resp.text}")

    return resp.json()


def get_me(access_token: str) -> dict:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(ME_URL, headers=headers, timeout=60)

    if resp.status_code >= 300:
        raise SystemExit(f"/me lookup failed ({resp.status_code}): {resp.text}")

    return resp.json()


def write_bundle(
    token_response: dict,
    token_info: dict,
    me_info: dict,
    site: str,
) -> None:
    if OUT_DIR.exists():
        for item in OUT_DIR.iterdir():
            if item.is_file():
                item.unlink()
    else:
        OUT_DIR.mkdir(parents=True, exist_ok=True)

    access_token = token_response.get("access_token", "")
    refresh_token = token_response.get("refresh_token", "")

    metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "site": site,
        "token_type": token_response.get("token_type"),
        "blog_id": token_response.get("blog_id"),
        "blog_url": token_response.get("blog_url"),
        "scope": token_response.get("scope"),
        "token_info": token_info,
        "me": {
            "ID": me_info.get("ID"),
            "username": me_info.get("username"),
            "email": me_info.get("email"),
            "display_name": me_info.get("display_name"),
            "primary_blog": me_info.get("primary_blog"),
        },
    }

    token_json = {
        "WPCOM_ACCESS_TOKEN": access_token,
        "WPCOM_REFRESH_TOKEN": refresh_token,
        "WPCOM_SITE": site,
    }

    (OUT_DIR / "token.json").write_text(
        json.dumps(token_json, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    (OUT_DIR / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    readme = f"""WordPress.com OAuth token bundle

Generated at: {metadata["generated_at_utc"]}
Site: {site}

Files:
- token.json      -> contains WPCOM_ACCESS_TOKEN
- metadata.json   -> token/account metadata

Next step:
1. Download this artifact ZIP from GitHub Actions
2. Open token.json
3. Copy the value of WPCOM_ACCESS_TOKEN
4. Save it as a GitHub Actions secret:
   WPCOM_ACCESS_TOKEN
5. Make sure WPCOM_SITE is also set to:
   {site}
"""
    (OUT_DIR / "README.txt").write_text(readme, encoding="utf-8")

    if OUT_ZIP.exists():
        OUT_ZIP.unlink()

    with zipfile.ZipFile(OUT_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in OUT_DIR.iterdir():
            zf.write(file_path, arcname=file_path.name)


def main() -> None:
    client_id = get_env("WPCOM_CLIENT_ID")
    client_secret = get_env("WPCOM_CLIENT_SECRET")
    redirect_uri = get_env("WPCOM_REDIRECT_URI")
    site = get_env("WPCOM_SITE")
    authorization_code = get_env("WPCOM_AUTHORIZATION_CODE")

    token_response = exchange_code(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        authorization_code=authorization_code,
    )

    access_token = token_response["access_token"]

    token_info = get_token_info(access_token)
    me_info = get_me(access_token)

    write_bundle(
        token_response=token_response,
        token_info=token_info,
        me_info=me_info,
        site=site,
    )

    print("WordPress.com token exchange completed.")
    print(f"ZIP written: {OUT_ZIP}")
    print(f"Site: {site}")
    print(f"Token scope: {token_response.get('scope')}")
    print(f"Blog URL: {token_response.get('blog_url')}")


if __name__ == "__main__":
    main()
