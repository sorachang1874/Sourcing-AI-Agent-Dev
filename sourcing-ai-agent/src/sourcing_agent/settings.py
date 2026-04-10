from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path


@dataclass(frozen=True, slots=True)
class QwenSettings:
    enabled: bool
    api_key: str = ""
    base_url: str = "https://dashscope.aliyuncs.com/api/v2/apps/protocols/compatible-mode/v1"
    model: str = "qwen3.5-plus"
    timeout_seconds: int = 45


@dataclass(frozen=True, slots=True)
class ModelProviderSettings:
    enabled: bool = False
    provider_name: str = ""
    api_key: str = ""
    base_url: str = ""
    model: str = ""
    api_style: str = "openai_chat_completions"
    timeout_seconds: int = 45


@dataclass(frozen=True, slots=True)
class SemanticProviderSettings:
    enabled: bool
    api_key: str = ""
    embedding_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    embedding_model: str = "text-embedding-v4"
    embedding_dimensions: int = 1024
    embedding_timeout_seconds: int = 45
    rerank_base_url: str = "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank"
    rerank_model: str = "gte-rerank-v2"
    media_rerank_model: str = "qwen3-vl-rerank"
    rerank_timeout_seconds: int = 45
    max_documents_per_call: int = 25


@dataclass(frozen=True, slots=True)
class SearchProviderSettings:
    enabled: bool = True
    provider_order: tuple[str, ...] = (
        "dataforseo_google_organic",
        "serper_google",
        "google_browser",
        "bing_html",
        "duckduckgo_html",
    )
    timeout_seconds: int = 30
    max_results_per_call: int = 10
    enable_dataforseo_google_organic: bool = True
    dataforseo_login: str = ""
    dataforseo_password: str = ""
    dataforseo_base_url: str = "https://api.dataforseo.com"
    dataforseo_default_location_name: str = "United States"
    dataforseo_default_language_name: str = "English"
    dataforseo_default_device: str = "desktop"
    dataforseo_default_os: str = "windows"
    dataforseo_default_depth: int = 10
    enable_bing_html: bool = True
    enable_duckduckgo_html: bool = True
    serper_api_key: str = ""
    serper_base_url: str = "https://google.serper.dev/search"
    enable_google_browser: bool = True
    google_browser_npx_package: str = "playwright@1.59.1"
    google_browser_node_modules_dir: str = "/tmp/sourcing-playwright-node/node_modules"
    google_browser_script_path: str = ""
    google_browser_npm_cache_dir: str = "/tmp/.npm-cache"
    google_browser_browsers_path: str = "/tmp/playwright-browsers"
    google_browser_headless: bool = True
    google_browser_locale: str = "en-US"


@dataclass(frozen=True, slots=True)
class ObjectStorageSettings:
    enabled: bool = False
    provider: str = "filesystem"
    bucket: str = ""
    prefix: str = "sourcing-ai-agent-dev"
    endpoint_url: str = ""
    region: str = "us-east-1"
    access_key_id: str = ""
    secret_access_key: str = ""
    timeout_seconds: int = 60
    force_path_style: bool = True
    local_dir: str = ""
    max_workers: int = 8


@dataclass(frozen=True, slots=True)
class HarvestActorSettings:
    enabled: bool = False
    api_token: str = ""
    actor_id: str = ""
    timeout_seconds: int = 180
    max_total_charge_usd: float = 1.0
    max_paid_items: int = 25
    default_mode: str = "full"
    collect_email: bool = False


@dataclass(frozen=True, slots=True)
class HarvestSettings:
    profile_scraper: HarvestActorSettings = field(default_factory=HarvestActorSettings)
    profile_search: HarvestActorSettings = field(default_factory=HarvestActorSettings)
    company_employees: HarvestActorSettings = field(default_factory=HarvestActorSettings)


@dataclass(frozen=True, slots=True)
class AppSettings:
    project_root: Path
    runtime_dir: Path
    secrets_file: Path
    jobs_dir: Path
    company_assets_dir: Path
    db_path: Path
    qwen: QwenSettings
    semantic: SemanticProviderSettings
    harvest: HarvestSettings = field(default_factory=HarvestSettings)
    search: SearchProviderSettings = field(default_factory=SearchProviderSettings)
    object_storage: ObjectStorageSettings = field(default_factory=ObjectStorageSettings)
    model_provider: ModelProviderSettings = field(default_factory=ModelProviderSettings)


def load_settings(project_root: str | Path) -> AppSettings:
    root = Path(project_root)
    runtime_dir = root / "runtime"
    secret_dir = runtime_dir / "secrets"
    secret_file = secret_dir / "providers.local.json"
    secret_payload = _load_json_file(secret_file)

    qwen_payload = secret_payload.get("qwen", {})
    model_provider_payload = secret_payload.get("model_provider", {})
    semantic_payload = secret_payload.get("semantic", {})
    search_payload = secret_payload.get("search_provider", {})
    object_storage_payload = secret_payload.get("object_storage", {})
    harvest_payload = secret_payload.get("harvest", {})
    profile_scraper_payload = harvest_payload.get("profile_scraper", {})
    profile_search_payload = harvest_payload.get("profile_search", {})
    company_employees_payload = harvest_payload.get("company_employees", {})
    api_key = os.getenv("DASHSCOPE_API_KEY") or str(qwen_payload.get("api_key", "")).strip()
    base_url = os.getenv("DASHSCOPE_BASE_URL") or str(
        qwen_payload.get("base_url", "https://dashscope.aliyuncs.com/api/v2/apps/protocols/compatible-mode/v1")
    ).strip()
    model = os.getenv("DASHSCOPE_MODEL") or str(qwen_payload.get("model", "qwen3.5-plus")).strip()
    timeout = os.getenv("DASHSCOPE_TIMEOUT_SECONDS") or qwen_payload.get("timeout_seconds", 45)

    try:
        timeout_seconds = int(timeout)
    except (TypeError, ValueError):
        timeout_seconds = 45

    model_provider_api_key = os.getenv("MODEL_PROVIDER_API_KEY") or str(model_provider_payload.get("api_key", "")).strip()
    model_provider_base_url = os.getenv("MODEL_PROVIDER_BASE_URL") or str(model_provider_payload.get("base_url", "")).strip()
    model_provider_model = os.getenv("MODEL_PROVIDER_MODEL") or str(model_provider_payload.get("model", "")).strip()
    model_provider_name = os.getenv("MODEL_PROVIDER_NAME") or str(model_provider_payload.get("provider_name", "")).strip()
    model_provider_api_style = os.getenv("MODEL_PROVIDER_API_STYLE") or str(
        model_provider_payload.get("api_style", "openai_chat_completions")
    ).strip()
    model_provider_timeout = os.getenv("MODEL_PROVIDER_TIMEOUT_SECONDS") or model_provider_payload.get("timeout_seconds", 45)
    try:
        model_provider_timeout_seconds = int(model_provider_timeout)
    except (TypeError, ValueError):
        model_provider_timeout_seconds = 45

    semantic_api_key = os.getenv("DASHSCOPE_API_KEY") or str(semantic_payload.get("api_key", "")).strip() or api_key
    embedding_base_url = os.getenv("DASHSCOPE_EMBEDDING_BASE_URL") or str(
        semantic_payload.get("embedding_base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    ).strip()
    embedding_model = os.getenv("DASHSCOPE_EMBEDDING_MODEL") or str(
        semantic_payload.get("embedding_model", "text-embedding-v4")
    ).strip()
    embedding_dimensions = os.getenv("DASHSCOPE_EMBEDDING_DIMENSIONS") or semantic_payload.get("embedding_dimensions", 1024)
    embedding_timeout = os.getenv("DASHSCOPE_EMBEDDING_TIMEOUT_SECONDS") or semantic_payload.get("embedding_timeout_seconds", 45)
    rerank_base_url = os.getenv("DASHSCOPE_RERANK_BASE_URL") or str(
        semantic_payload.get("rerank_base_url", "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank")
    ).strip()
    rerank_model = os.getenv("DASHSCOPE_RERANK_MODEL") or str(
        semantic_payload.get("rerank_model", "gte-rerank-v2")
    ).strip()
    media_rerank_model = os.getenv("DASHSCOPE_MEDIA_RERANK_MODEL") or str(
        semantic_payload.get("media_rerank_model", "qwen3-vl-rerank")
    ).strip()
    rerank_timeout = os.getenv("DASHSCOPE_RERANK_TIMEOUT_SECONDS") or semantic_payload.get("rerank_timeout_seconds", 45)
    max_documents_per_call = os.getenv("DASHSCOPE_RERANK_MAX_DOCUMENTS") or semantic_payload.get("max_documents_per_call", 25)
    try:
        embedding_dimensions_value = int(embedding_dimensions)
    except (TypeError, ValueError):
        embedding_dimensions_value = 1024
    try:
        embedding_timeout_seconds = int(embedding_timeout)
    except (TypeError, ValueError):
        embedding_timeout_seconds = 45
    try:
        rerank_timeout_seconds = int(rerank_timeout)
    except (TypeError, ValueError):
        rerank_timeout_seconds = 45
    try:
        max_documents_value = int(max_documents_per_call)
    except (TypeError, ValueError):
        max_documents_value = 25

    search_provider_order = os.getenv("SEARCH_PROVIDER_ORDER") or search_payload.get(
        "provider_order",
        ["dataforseo_google_organic", "serper_google", "google_browser", "bing_html", "duckduckgo_html"],
    )
    if isinstance(search_provider_order, str):
        provider_order = tuple(item.strip() for item in search_provider_order.split(",") if item.strip())
    elif isinstance(search_provider_order, list):
        provider_order = tuple(str(item).strip() for item in search_provider_order if str(item).strip())
    else:
        provider_order = ("dataforseo_google_organic", "serper_google", "bing_html", "duckduckgo_html")
    search_timeout = os.getenv("SEARCH_PROVIDER_TIMEOUT_SECONDS") or search_payload.get("timeout_seconds", 30)
    search_max_results = os.getenv("SEARCH_PROVIDER_MAX_RESULTS") or search_payload.get("max_results_per_call", 10)
    try:
        search_timeout_seconds = int(search_timeout)
    except (TypeError, ValueError):
        search_timeout_seconds = 30
    try:
        search_max_results_value = int(search_max_results)
    except (TypeError, ValueError):
        search_max_results_value = 10
    serper_api_key = os.getenv("SERPER_API_KEY") or str(search_payload.get("serper_api_key", "")).strip()
    serper_base_url = os.getenv("SERPER_BASE_URL") or str(
        search_payload.get("serper_base_url", "https://google.serper.dev/search")
    ).strip()
    enable_dataforseo_google_organic = _coerce_bool(
        os.getenv("SEARCH_PROVIDER_ENABLE_DATAFORSEO_GOOGLE_ORGANIC"),
        default=bool(search_payload.get("enable_dataforseo_google_organic", True)),
    )
    dataforseo_login = os.getenv("DATAFORSEO_LOGIN") or str(search_payload.get("dataforseo_login", "")).strip()
    dataforseo_password = os.getenv("DATAFORSEO_PASSWORD") or str(search_payload.get("dataforseo_password", "")).strip()
    dataforseo_base_url = os.getenv("DATAFORSEO_BASE_URL") or str(
        search_payload.get("dataforseo_base_url", "https://api.dataforseo.com")
    ).strip()
    dataforseo_default_location_name = os.getenv("DATAFORSEO_LOCATION_NAME") or str(
        search_payload.get("dataforseo_default_location_name", "United States")
    ).strip()
    dataforseo_default_language_name = os.getenv("DATAFORSEO_LANGUAGE_NAME") or str(
        search_payload.get("dataforseo_default_language_name", "English")
    ).strip()
    dataforseo_default_device = os.getenv("DATAFORSEO_DEVICE") or str(
        search_payload.get("dataforseo_default_device", "desktop")
    ).strip()
    dataforseo_default_os = os.getenv("DATAFORSEO_OS") or str(
        search_payload.get("dataforseo_default_os", "windows")
    ).strip()
    dataforseo_default_depth_raw = os.getenv("DATAFORSEO_DEPTH") or search_payload.get("dataforseo_default_depth", 10)
    try:
        dataforseo_default_depth = int(dataforseo_default_depth_raw)
    except (TypeError, ValueError):
        dataforseo_default_depth = 10
    enable_bing_html = _coerce_bool(
        os.getenv("SEARCH_PROVIDER_ENABLE_BING_HTML"),
        default=bool(search_payload.get("enable_bing_html", True)),
    )
    enable_duckduckgo_html = bool(search_payload.get("enable_duckduckgo_html", True))
    enable_google_browser = _coerce_bool(
        os.getenv("SEARCH_PROVIDER_ENABLE_GOOGLE_BROWSER"),
        default=bool(search_payload.get("enable_google_browser", True)),
    )
    google_browser_npx_package = os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_NPX_PACKAGE") or str(
        search_payload.get("google_browser_npx_package", "playwright@1.59.1")
    ).strip()
    google_browser_node_modules_dir = os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_NODE_MODULES_DIR") or str(
        search_payload.get("google_browser_node_modules_dir", str(runtime_dir / "vendor" / "playwright" / "node_modules"))
    ).strip()
    google_browser_script_path = os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_SCRIPT_PATH") or str(
        search_payload.get("google_browser_script_path", "")
    ).strip()
    google_browser_npm_cache_dir = os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_NPM_CACHE_DIR") or str(
        search_payload.get("google_browser_npm_cache_dir", str(runtime_dir / "vendor" / "npm-cache"))
    ).strip()
    google_browser_browsers_path = os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_BROWSERS_PATH") or str(
        search_payload.get("google_browser_browsers_path", str(runtime_dir / "vendor" / "playwright-browsers"))
    ).strip()
    google_browser_headless = _coerce_bool(
        os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_HEADLESS"),
        default=bool(search_payload.get("google_browser_headless", True)),
    )
    google_browser_locale = os.getenv("SEARCH_PROVIDER_GOOGLE_BROWSER_LOCALE") or str(
        search_payload.get("google_browser_locale", "en-US")
    ).strip()

    object_storage_provider = os.getenv("OBJECT_STORAGE_PROVIDER") or str(
        object_storage_payload.get("provider", "filesystem")
    ).strip()
    object_storage_bucket = os.getenv("OBJECT_STORAGE_BUCKET") or str(object_storage_payload.get("bucket", "")).strip()
    object_storage_prefix = os.getenv("OBJECT_STORAGE_PREFIX") or str(
        object_storage_payload.get("prefix", "sourcing-ai-agent-dev")
    ).strip()
    object_storage_endpoint = os.getenv("OBJECT_STORAGE_ENDPOINT_URL") or str(
        object_storage_payload.get("endpoint_url", "")
    ).strip()
    object_storage_region = os.getenv("OBJECT_STORAGE_REGION") or str(
        object_storage_payload.get("region", "us-east-1")
    ).strip()
    object_storage_access_key_id = os.getenv("OBJECT_STORAGE_ACCESS_KEY_ID") or str(
        object_storage_payload.get("access_key_id", "")
    ).strip()
    object_storage_secret_access_key = os.getenv("OBJECT_STORAGE_SECRET_ACCESS_KEY") or str(
        object_storage_payload.get("secret_access_key", "")
    ).strip()
    object_storage_local_dir = os.getenv("OBJECT_STORAGE_LOCAL_DIR") or str(
        object_storage_payload.get("local_dir", "")
    ).strip()
    if not object_storage_local_dir and (object_storage_provider or "filesystem").strip().lower() in {"", "filesystem"}:
        object_storage_local_dir = str(runtime_dir / "object_store")
    object_storage_timeout = os.getenv("OBJECT_STORAGE_TIMEOUT_SECONDS") or object_storage_payload.get("timeout_seconds", 60)
    try:
        object_storage_timeout_seconds = int(object_storage_timeout)
    except (TypeError, ValueError):
        object_storage_timeout_seconds = 60
    object_storage_max_workers = os.getenv("OBJECT_STORAGE_MAX_WORKERS") or object_storage_payload.get("max_workers", 8)
    try:
        object_storage_max_workers_value = int(object_storage_max_workers)
    except (TypeError, ValueError):
        object_storage_max_workers_value = 8
    force_path_style_raw = os.getenv("OBJECT_STORAGE_FORCE_PATH_STYLE")
    if force_path_style_raw is None:
        object_storage_force_path_style = bool(object_storage_payload.get("force_path_style", True))
    else:
        object_storage_force_path_style = force_path_style_raw.strip().lower() not in {"0", "false", "no", ""}

    shared_harvest_token = os.getenv("APIFY_API_TOKEN") or str(profile_scraper_payload.get("api_token", "")).strip()
    if not shared_harvest_token:
        shared_harvest_token = str(profile_search_payload.get("api_token", "")).strip()
    if not shared_harvest_token:
        shared_harvest_token = str(company_employees_payload.get("api_token", "")).strip()

    def _harvest_actor_settings(
        payload: dict,
        *,
        actor_id_default: str,
        timeout_env: str,
        charge_env: str,
        items_env: str,
        default_charge: float,
        default_items: int,
        default_mode: str,
        default_collect_email: bool = False,
    ) -> HarvestActorSettings:
        api_token = shared_harvest_token or str(payload.get("api_token", "")).strip()
        actor_id = str(payload.get("actor_id", actor_id_default)).strip()
        timeout = os.getenv(timeout_env) or payload.get("timeout_seconds", 180)
        max_charge = os.getenv(charge_env) or payload.get("max_total_charge_usd", default_charge)
        max_paid_items = os.getenv(items_env) or payload.get("max_paid_items", default_items)
        collect_email_raw = payload.get("collect_email", default_collect_email)
        try:
            timeout_seconds = int(timeout)
        except (TypeError, ValueError):
            timeout_seconds = 180
        try:
            max_total_charge_usd = float(max_charge)
        except (TypeError, ValueError):
            max_total_charge_usd = default_charge
        try:
            max_items = int(max_paid_items)
        except (TypeError, ValueError):
            max_items = default_items
        if isinstance(collect_email_raw, str):
            collect_email = collect_email_raw.strip().lower() not in {"", "0", "false", "no"}
        else:
            collect_email = bool(collect_email_raw)
        return HarvestActorSettings(
            enabled=bool(api_token),
            api_token=api_token,
            actor_id=actor_id,
            timeout_seconds=timeout_seconds,
            max_total_charge_usd=max_total_charge_usd,
            max_paid_items=max_items,
            default_mode=str(payload.get("default_mode", default_mode)).strip() or default_mode,
            collect_email=collect_email,
        )

    return AppSettings(
        project_root=root,
        runtime_dir=runtime_dir,
        secrets_file=secret_file,
        jobs_dir=runtime_dir / "jobs",
        company_assets_dir=runtime_dir / "company_assets",
        db_path=runtime_dir / "sourcing_agent.db",
        qwen=QwenSettings(
            enabled=bool(api_key),
            api_key=api_key,
            base_url=base_url.rstrip("/"),
            model=model,
            timeout_seconds=timeout_seconds,
        ),
        semantic=SemanticProviderSettings(
            enabled=bool(semantic_api_key),
            api_key=semantic_api_key,
            embedding_base_url=embedding_base_url.rstrip("/"),
            embedding_model=embedding_model,
            embedding_dimensions=embedding_dimensions_value,
            embedding_timeout_seconds=embedding_timeout_seconds,
            rerank_base_url=rerank_base_url.rstrip("/"),
            rerank_model=rerank_model,
            media_rerank_model=media_rerank_model,
            rerank_timeout_seconds=rerank_timeout_seconds,
            max_documents_per_call=max(1, min(max_documents_value, 100)),
        ),
        search=SearchProviderSettings(
            enabled=True,
            provider_order=provider_order or ("duckduckgo_html",),
            timeout_seconds=search_timeout_seconds,
            max_results_per_call=max(1, min(search_max_results_value, 25)),
            enable_dataforseo_google_organic=enable_dataforseo_google_organic,
            dataforseo_login=dataforseo_login,
            dataforseo_password=dataforseo_password,
            dataforseo_base_url=dataforseo_base_url.rstrip("/"),
            dataforseo_default_location_name=dataforseo_default_location_name or "United States",
            dataforseo_default_language_name=dataforseo_default_language_name or "English",
            dataforseo_default_device=dataforseo_default_device or "desktop",
            dataforseo_default_os=dataforseo_default_os or "windows",
            dataforseo_default_depth=max(1, min(dataforseo_default_depth, 100)),
            enable_bing_html=enable_bing_html,
            enable_duckduckgo_html=enable_duckduckgo_html,
            serper_api_key=serper_api_key,
            serper_base_url=serper_base_url.rstrip("/"),
            enable_google_browser=enable_google_browser,
            google_browser_npx_package=google_browser_npx_package,
            google_browser_node_modules_dir=google_browser_node_modules_dir,
            google_browser_script_path=google_browser_script_path,
            google_browser_npm_cache_dir=google_browser_npm_cache_dir,
            google_browser_browsers_path=google_browser_browsers_path,
            google_browser_headless=google_browser_headless,
            google_browser_locale=google_browser_locale,
        ),
        object_storage=ObjectStorageSettings(
            enabled=bool(
                object_storage_provider.lower() in {"", "filesystem"}
                or object_storage_local_dir
                or (object_storage_provider.lower() in {"s3", "s3_compatible", "oss_s3"} and object_storage_bucket and object_storage_endpoint)
            ),
            provider=object_storage_provider or "filesystem",
            bucket=object_storage_bucket,
            prefix=object_storage_prefix or "sourcing-ai-agent-dev",
            endpoint_url=object_storage_endpoint.rstrip("/"),
            region=object_storage_region or "us-east-1",
            access_key_id=object_storage_access_key_id,
            secret_access_key=object_storage_secret_access_key,
            timeout_seconds=object_storage_timeout_seconds,
            force_path_style=object_storage_force_path_style,
            local_dir=object_storage_local_dir,
            max_workers=max(1, min(object_storage_max_workers_value, 64)),
        ),
        harvest=HarvestSettings(
            profile_scraper=_harvest_actor_settings(
                profile_scraper_payload,
                actor_id_default="LpVuK3Zozwuipa5bp",
                timeout_env="HARVEST_PROFILE_TIMEOUT_SECONDS",
                charge_env="HARVEST_PROFILE_MAX_TOTAL_CHARGE_USD",
                items_env="HARVEST_PROFILE_MAX_PAID_ITEMS",
                default_charge=5.0,
                default_items=25,
                default_mode="full",
                default_collect_email=True,
            ),
            profile_search=_harvest_actor_settings(
                profile_search_payload,
                actor_id_default="M2FMdjRVeF1HPGFcc",
                timeout_env="HARVEST_SEARCH_TIMEOUT_SECONDS",
                charge_env="HARVEST_SEARCH_MAX_TOTAL_CHARGE_USD",
                items_env="HARVEST_SEARCH_MAX_PAID_ITEMS",
                default_charge=1.0,
                default_items=50,
                default_mode="short",
            ),
            company_employees=_harvest_actor_settings(
                company_employees_payload,
                actor_id_default="Vb6LZkh4EqRlR0Ka9",
                timeout_env="HARVEST_COMPANY_TIMEOUT_SECONDS",
                charge_env="HARVEST_COMPANY_MAX_TOTAL_CHARGE_USD",
                items_env="HARVEST_COMPANY_MAX_PAID_ITEMS",
                default_charge=5.0,
                default_items=200,
                default_mode="short",
            ),
        ),
        model_provider=ModelProviderSettings(
            enabled=bool(model_provider_api_key and model_provider_base_url and model_provider_model),
            provider_name=model_provider_name or "openai_compatible",
            api_key=model_provider_api_key,
            base_url=model_provider_base_url.rstrip("/"),
            model=model_provider_model,
            api_style=model_provider_api_style or "openai_chat_completions",
            timeout_seconds=model_provider_timeout_seconds,
        ),
    )


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def _coerce_bool(value: object, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default
