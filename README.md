# Bergschacht Custom Resources

A [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) monorepo for CDK Custom Resource Lambda functions used by the [Bergschacht](https://github.com/feuerstein-org/bergschacht-public) CDK infrastructure.

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

> Note: Currently publishing goes through Seilbhan but I plan to open source as much as possible directly on npm instead.

## Development

```bash
mise run install        # Install all dependencies
mise run test           # Run tests for all packages
mise run test s3tables-table-manager  # Run tests for a specific package
mise run lint           # Check linting
mise run lint-fix       # Auto-fix lint issues
```

## CI/CD Setup

Uses [Seilbahn public workflows](https://github.com/feuerstein-org/seilbahn#required-configuration-in-the-consumer-repo). Tests require no secrets. Deployment runs only in private repositories, configure the following settings there.

For your own deployment, clone this repository and push it to a new private GitHub repository. Connect it to a private Bergschacht repository using `CDK_REPO_OWNER` and `CDK_REPO_NAME`, and configure the settings below in your private copy. Seilbahn then publishes the Lambda artifact and updates that Bergschacht repository's version manifest.

### Required Secrets

| Secret                     | Description                                        |
| -------------------------- | -------------------------------------------------- |
| `CDK_REPO_APP_PRIVATE_KEY` | GitHub App private key for authentication          |

### Required Variables

| Variable                | Description                               |
| ----------------------- | ----------------------------------------- |
| `AWS_REGION`            | AWS region for deployment                 |
| `CICD_ACCOUNT_ID`       | AWS account ID for the CI/CD account      |
| `CDK_REPO_OWNER`        | GitHub org/owner of the CDK repo          |
| `CDK_REPO_NAME`         | Name of the CDK repository                |
| `CDK_REPO_CLIENT_ID`    | GitHub App client ID (`prod` environment) |
| `LAMBDA_S3_BUCKET_NAME` | S3 bucket for Lambda deployment artifacts |
