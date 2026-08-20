# Bergschacht Custom Resources

A [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) monorepo for CDK Custom Resource Lambda functions used by the [Bergschacht](https://github.com/feuerstein-org/bergschacht) CDK infrastructure. See the [sample-python-monorepo](https://github.com/feuerstein-org/sample-python-monorepo) to get an understanding of how the deployment works.

## Workspace Structure

```
bergschacht-custom-resources/
├── pyproject.toml                  # Workspace root (defines uv members)
├── seilbahn.toml                   # Packages & artifacts the CI/CD pipeline builds
├── packages/
│   └── s3tables-table-manager/
│       ├── pyproject.toml          # version = "0.1.0"
│       └── src/s3tables_table_manager/
│           ├── __init__.py         # Re-exports handler
│           ├── types.py            # Iceberg type & transform parsing
│           ├── models.py           # Pydantic models (schema definition)
│           ├── evolution.py        # Schema / partition / sort evolution
│           └── handler.py          # Lambda entry point
└── .github/
    └── workflows/
        ├── deploy.yml
        └── test.yml
```

## Packages

### s3tables-table-manager

CDK Custom Resource that **owns the full Iceberg table lifecycle** on S3 Tables: it creates tables on `Create`, evolves schema / partition spec / sort order on `Update`, and drops the table on `Delete`. The CDK schema definition is the source of truth - columns, partition specs, and sort orders are fully reconciled using [PyIceberg](https://py.iceberg.apache.org/).

**Lambda handler:** `s3tables_table_manager.handler.handler`

## Adding a New Custom Resource

1. Create a new directory under `packages/`:

   ```
   packages/my-custom-resource/
   ├── pyproject.toml
   └── src/my_custom_resource/
       ├── __init__.py
       └── handler.py
   ```

2. Give it a `pyproject.toml` (this is where its version lives):

   ```toml
   [project]
   name = "my-custom-resource"
   version = "0.1.0"
   dependencies = ["boto3"]
   ```

3. Declare it in the root `seilbahn.toml`:

   ```toml
   [packages.my-custom-resource]
   path = "packages/my-custom-resource"
   artifacts.my-custom-resource = { type = "lambda", build = "mise run build-lambda my-custom-resource" }
   ```

## Development

```bash
mise run install        # Install all dependencies
mise run test           # Run tests for all packages
mise run test s3tables-table-manager  # Run tests for a specific package
mise run lint           # Check linting
mise run lint-fix       # Auto-fix lint issues
```

## CI/CD Setup

Same setup as `sample-python-repo` - see [the CDK repo README](https://github.com/feuerstein-org/bergschacht) for details.

### Required Secrets

| Secret                     | Description                                        |
| -------------------------- | -------------------------------------------------- |
| `CDK_REPO_APP_ID`          | GitHub App ID for triggering workflows in CDK repo |
| `CDK_REPO_APP_PRIVATE_KEY` | GitHub App private key for authentication          |

### Required Variables

| Variable                | Description                               |
| ----------------------- | ----------------------------------------- |
| `AWS_REGION`            | AWS region for deployment                 |
| `CICD_ACCOUNT_ID`       | AWS account ID for the CI/CD account      |
| `CDK_REPO_OWNER`        | GitHub org/owner of the CDK repo          |
| `CDK_REPO_NAME`         | Name of the CDK repository                |
| `LAMBDA_S3_BUCKET_NAME` | S3 bucket for Lambda deployment artifacts |
