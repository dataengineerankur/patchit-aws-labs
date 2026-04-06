
# PATCHIT: add missing IAM permissions
resource "aws_iam_role_policy" "patchit_fix" {
  name = "patchit-auto-fix-policy"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket",
                  "glue:*", "logs:*", "cloudwatch:*"]
      Resource = "*"
    }]
  })
}
