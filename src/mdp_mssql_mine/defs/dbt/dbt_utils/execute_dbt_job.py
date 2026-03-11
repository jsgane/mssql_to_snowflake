import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any

from dagster import AssetExecutionContext

import inspect


def execute_dbt(
    context: AssetExecutionContext,
    dbt_project_name: str,
    dbt_args: str = "run",
    dbt_project_dir: Path | None = None,
) -> Dict[str, Any]:
    """
    Execute dbt either locally or in Snowflake based on environment.

    Args:
        context: Dagster asset execution context
        dbt_project_name: Name of the dbt project (for Snowflake execution)
        dbt_args: dbt command arguments (default: "run")
        dbt_project_dir: Path to dbt project directory. If None, auto-detects from caller's location.

    Returns:
        Dictionary with execution metadata

    Environment Variables:
        DAGSTER_ENV: Set to "dev" for local execution, anything else for Snowflake execution
    """
    is_dev = os.getenv("DAGSTER_ENV", "prod") == "dev"

    if is_dev:
        context.log.info("🔧 Running dbt LOCALLY (dev mode)")
        return execute_dbt_locally(context, dbt_args, dbt_project_dir)
    else:
        context.log.info("☁️  Running dbt in SNOWFLAKE (prod mode)")
        return execute_dbt_in_snowflake(context, dbt_project_name, dbt_args)


def execute_dbt_locally(
    context: AssetExecutionContext,
    dbt_args: str = "run",
    dbt_project_dir: Path | None = None,
) -> Dict[str, Any]:
    """
    Execute dbt locally using dbt CLI.

    Args:
        context: Dagster asset execution context
        dbt_args: dbt command arguments
        dbt_project_dir: Path to dbt project directory

    Returns:
        Dictionary with execution results

    Raises:
        Exception: If dbt command fails
    """
    # Auto-detect dbt project directory if not provided
    if dbt_project_dir is None:

        caller_file = Path(inspect.stack()[2].filename)
        dbt_project_dir = caller_file.parent / "project"

    dbt_project_dir = Path(dbt_project_dir)

    # Get dbt executable from current Python environment
    dbt_path = Path(sys.executable).parent / "dbt"

    if not dbt_path.exists():
        raise FileNotFoundError(
            f"dbt executable not found at {dbt_path}. "
            f"Make sure dbt-snowflake is installed in your environment."
        )

    command = [str(dbt_path)] + dbt_args.split() + ["--target", "dev"]

    context.log.info(f"📁 dbt project dir: {dbt_project_dir}")
    context.log.info(f"▶️  Executing: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
            check=True,
        )

        context.log.info("✅ dbt execution completed successfully")
        for line in result.stdout.split("\n"):
            if line.strip():
                context.log.info(line)

        return {
            "execution_mode": "local",
            "dbt_args": dbt_args,
            "status": "success",
            "project_dir": str(dbt_project_dir),
        }

    except subprocess.CalledProcessError as e:
        context.log.error(f"❌ dbt execution failed with exit code {e.returncode}")
        context.log.error("STDOUT:")
        for line in e.stdout.split("\n"):
            if line.strip():
                context.log.error(line)
        context.log.error("STDERR:")
        for line in e.stderr.split("\n"):
            if line.strip():
                context.log.error(line)
        raise Exception(f"dbt command failed: {e.stderr}")


def execute_dbt_in_snowflake(
    context: AssetExecutionContext,
    dbt_project_name: str,
    dbt_args: str = "run",
) -> Dict[str, Any]:
    """
    Execute dbt project natively in Snowflake via EXECUTE DBT PROJECT.

    Args:
        context: Dagster asset execution context (must have 'snowflake' resource)
        dbt_project_name: Name of the dbt project in Snowflake
        dbt_args: dbt command arguments

    Returns:
        Dictionary with execution results

    Raises:
        Exception: If Snowflake execution fails
    """
    snowflake_resource = context.resources.snowflake
    context.log.info("🔷 Connecting to Snowflake...")

    with snowflake_resource.get_connection() as conn:
        cursor = conn.cursor()

        try:
            sql = f"EXECUTE DBT PROJECT {dbt_project_name} ARGS = '{dbt_args}'"
            context.log.info(f"▶️  Executing: {sql}")
            cursor.execute(sql)
            context.log.info("✅ dbt execution completed successfully")

            return {
                "execution_mode": "snowflake",
                "dbt_project": dbt_project_name,
                "dbt_args": dbt_args,
                "status": "success",
            }

        except Exception as e:
            context.log.error(f"❌ dbt execution failed: {e}")
            raise
        finally:
            cursor.close()