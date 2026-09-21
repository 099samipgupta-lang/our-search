import os

from index_storage.remote import RemoteIndexStorage
from index_storage.supabase import SupabaseIndexStorage


def create_index_storage():
    mode = os.environ.get(
        "OUR_SEARCH_STORAGE_MODE",
        "supabase",
    ).strip().lower()

    if mode == "remote":
        base_url = os.environ.get(
            "OUR_SEARCH_STORAGE_URL"
        )

        api_key = os.environ.get(
            "OUR_SEARCH_STORAGE_API_KEY"
        )

        if not base_url:
            raise RuntimeError(
                "OUR_SEARCH_STORAGE_URL is required "
                "when OUR_SEARCH_STORAGE_MODE=remote"
            )

        if not api_key:
            raise RuntimeError(
                "OUR_SEARCH_STORAGE_API_KEY is required "
                "when OUR_SEARCH_STORAGE_MODE=remote"
            )

        timeout = int(
            os.environ.get(
                "OUR_SEARCH_STORAGE_TIMEOUT",
                "30",
            )
        )

        return RemoteIndexStorage(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
        )

    if mode == "supabase":
        project_url = os.environ.get(
            "SUPABASE_URL"
        )

        api_key = os.environ.get(
            "SUPABASE_KEY"
        )

        bucket = os.environ.get(
            "SUPABASE_BUCKET",
            "Videos",
        )

        if not project_url:
            raise RuntimeError(
                "SUPABASE_URL is required"
            )

        if not api_key:
            raise RuntimeError(
                "SUPABASE_KEY is required"
            )

        return SupabaseIndexStorage(
            project_url=project_url,
            api_key=api_key,
            bucket=bucket,
        )

    raise RuntimeError(
        f"Unsupported OUR_SEARCH_STORAGE_MODE: {mode}"
    )
