import base64
import json
import pathlib
from uuid import UUID

import polars_cloud as pc

from settings import Settings

settings = Settings()


def reuse_compute_context(filename: str, log_reuse: bool) -> pc.ComputeContext | None:
    with pathlib.Path(filename).open("r", encoding="utf8") as r:
        context_args = json.load(r)

    required_keys = ["workspace_id", "compute_id"]
    for key in required_keys:
        assert key in context_args, f"Key {key} not in {filename}"
    if log_reuse:
        print(f"Reusing existing compute context: {context_args['compute_id']}")
    context_args = {key: UUID(context_args.get(key)) for key in required_keys}
    try:
        ctx = pc.ComputeContext.connect(**context_args)
        ctx.start(wait=True)
        assert ctx.get_status() == pc.ComputeContextStatus.RUNNING
    except RuntimeError as e:
        print(f"Cannot reuse existing compute context: {e.args}")
        return None
    return ctx


def get_compute_context_args() -> dict[str, str | int]:
    return {
        key: value
        for key, value in {
            "cpus": settings.run.polars_cloud_cpus,
            "memory": settings.run.polars_cloud_memory,
            "instance_type": settings.run.polars_cloud_instance_type,
            "cluster_size": settings.run.polars_cloud_cluster_size,
            "workspace": settings.run.polars_cloud_workspace,
        }.items()
        if value is not None
    }


def get_compute_context_filename(context_args: dict[str, str | int]) -> str:
    hash = base64.b64encode(str(context_args).encode("utf-8")).decode()
    return f".polars-cloud-compute-context-{hash}.json"


def get_compute_context(
    *,
    create_if_no_reuse: bool = True,
    log_create: bool = False,
    log_reuse: bool = False,
) -> pc.ComputeContext:
    context_args = get_compute_context_args()
    context_filename = get_compute_context_filename(context_args)
    if pathlib.Path(context_filename).is_file():
        ctx = reuse_compute_context(context_filename, log_reuse)
        if ctx:
            return ctx

    # start new compute context
    if not create_if_no_reuse:
        msg = "Cannot reuse compute context"
        raise RuntimeError(msg)
    if log_create:
        print(f"Starting new compute context: {context_args}")
    ctx = pc.ComputeContext(**context_args)  # type: ignore[arg-type]
    ctx.start(wait=True)
    assert ctx.get_status() == pc.ComputeContextStatus.RUNNING
    context_args = {
        "workspace_id": str(ctx.workspace.id),
        "compute_id": str(ctx._compute_id),
    }
    with pathlib.Path(context_filename).open("w", encoding="utf8") as w:
        json.dump(context_args, w)
    return ctx


def stop_compute_context(ctx: pc.ComputeContext) -> None:
    ctx.stop(wait=True)
    context_args = get_compute_context_args()
    context_filename = get_compute_context_filename(context_args)
    pathlib.Path(context_filename).unlink(missing_ok=True)
