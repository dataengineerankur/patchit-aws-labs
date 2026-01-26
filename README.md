## Patchit AWS Labs

AWS Glue + CloudWatch lab with controlled failure drills for PATCHIT.

### Variables (set at runtime)

- `WORKSPACE_ROOT="<local folder where repos will live>"`
- `GITHUB_ORIGIN="<optional: remote github org/repo base>"`
- `PATCHIT_CMD="<command to invoke Patchit locally>"`
- `OUTPUT_DIR="<local folder for evidence packs and reports>"`
- `AWS_PROFILE` or `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET_DATA_LANDING`
- `GLUE_ROLE_ARN` (or created via Terraform)

---

### Architecture

Glue job:
1. Reads S3 landing data.
2. Validates schema + row counts.
3. Writes partitioned Parquet to curated bucket.
4. Emits a small manifest for verification.

Terraform deploys S3 buckets, IAM role, Glue jobs (ingest + quality), and log group.

---

### Setup

Create a local `.env` (do not commit):

```bash
cat > .env <<'EOF'
AWS_PROFILE=
AWS_REGION=
S3_BUCKET_DATA_LANDING=
GLUE_ROLE_ARN=
PATCHIT_CMD=
OUTPUT_DIR=
EOF
```

---

### Terraform deploy (do NOT apply without credentials)

```bash
cd infra/terraform
terraform init
terraform plan \
  -var "aws_region=${AWS_REGION}" \
  -var "s3_bucket_data_landing=${S3_BUCKET_DATA_LANDING}" \
  -var "glue_role_arn=${GLUE_ROLE_ARN}"
```

Apply (only after explicit confirmation + creds):

```bash
terraform apply \
  -var "aws_region=${AWS_REGION}" \
  -var "s3_bucket_data_landing=${S3_BUCKET_DATA_LANDING}" \
  -var "glue_role_arn=${GLUE_ROLE_ARN}"
```

Destroy:

```bash
terraform destroy \
  -var "aws_region=${AWS_REGION}" \
  -var "s3_bucket_data_landing=${S3_BUCKET_DATA_LANDING}" \
  -var "glue_role_arn=${GLUE_ROLE_ARN}"
```

---

### Failure drills

Drills are defined in `drills/drills.yaml`.

Run one drill:

```bash
OUTPUT_DIR="<path>" PATCHIT_CMD="<patchit command>" \
./scripts/run_drill.sh AWS1_glue_schema_drift
```

Run all enabled drills:

```bash
OUTPUT_DIR="<path>" PATCHIT_CMD="<patchit command>" \
./scripts/run_all_drills.sh
```

---

### How PATCHIT is invoked

```bash
${PATCHIT_CMD} \
  --repo "$(pwd)" \
  --platform aws \
  --logs "$LOG_PATH" \
  --mode pr_only \
  --evidence_out "$EVIDENCE_PATH"
```

---

### Cost controls / cleanup

- Use smallest job size and small input data.
- Disable schedules by default.
- Always `terraform destroy` after validation.

---

### How to onboard a real company later

- Emit Glue failures from EventBridge into a secure webhook/Lambda.
- Store AWS secrets in IAM roles/SSM, not local files.
- Enforce PR-only remediation.

---

### How to run and test (end-to-end)

1) Local smoke test (no cloud required):

```bash
python jobs/glue_job.py
```

2) Run a drill:

```bash
OUTPUT_DIR="$(pwd)/evidence" PATCHIT_CMD="<your patchit cmd>" \
./scripts/run_drill.sh AWS1_glue_schema_drift
```

3) Review outputs:
- Evidence: `evidence/AWS1_glue_schema_drift/evidence_pack.json`
- Logs: `evidence/AWS1_glue_schema_drift/logs/`

4) Run all enabled drills:

```bash
OUTPUT_DIR="$(pwd)/evidence" PATCHIT_CMD="<your patchit cmd>" \
./scripts/run_all_drills.sh
```
