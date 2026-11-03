#!/usr/bin/env bash
# Deploy the omics-sra CloudFormation stacks with the AWS CLI.
#
#   infra/deploy.sh bootstrap                 # ECR repo + GitHub OIDC deploy role (once)
#   infra/deploy.sh app [ImageUri] [Email]    # bucket, Lambda, weekly schedule, alarm
#
# For "app", ImageUri defaults to the image the function currently runs (so a
# redeploy never rolls back an image published by GitHub Actions), or, on the
# first deploy, to the newest image in ECR.
# Uses the default AWS profile unless AWS_PROFILE is set; region from AWS_REGION (default us-west-1).
set -euo pipefail

REGION="${AWS_REGION:-us-west-1}"
FUNCTION_NAME="omics-sra-wgs-batches"
HERE="$(cd "$(dirname "$0")" && pwd)"
aws_() { aws --region "$REGION" "$@"; }

case "${1:-}" in
  bootstrap)
    create_oidc=true
    if aws iam list-open-id-connect-providers --query 'OpenIDConnectProviderList[].Arn' --output text \
        | grep -q 'token.actions.githubusercontent.com'; then
      # Only reuse a provider this stack doesn't own.
      owner=$(aws_ cloudformation describe-stack-resources --stack-name omics-sra-bootstrap \
                --logical-resource-id GitHubOidcProvider --query 'StackResources[0].PhysicalResourceId' \
                --output text 2>/dev/null || true)
      [[ -z "$owner" || "$owner" == "None" ]] && create_oidc=false
    fi
    aws_ cloudformation deploy --stack-name omics-sra-bootstrap \
      --template-file "$HERE/bootstrap.yaml" --capabilities CAPABILITY_NAMED_IAM \
      --parameter-overrides "CreateOidcProvider=$create_oidc"
    aws_ cloudformation describe-stacks --stack-name omics-sra-bootstrap \
      --query 'Stacks[0].Outputs' --output table
    ;;
  app)
    image="${2:-}"
    if [[ -z "$image" ]]; then
      image=$(aws_ lambda get-function --function-name "$FUNCTION_NAME" \
                --query 'Code.ImageUri' --output text 2>/dev/null || true)
    fi
    if [[ -z "$image" || "$image" == "None" ]]; then
      repo=$(aws_ ecr describe-repositories --repository-names omics-sra \
               --query 'repositories[0].repositoryUri' --output text)
      tag=$(aws_ ecr describe-images --repository-name omics-sra \
              --query 'sort_by(imageDetails,&imagePushedAt)[-1].imageTags[?@!=`latest`] | [0]' --output text)
      [[ -z "$tag" || "$tag" == "None" ]] && { echo "error: no image in ECR yet; run the deploy workflow first" >&2; exit 1; }
      image="$repo:$tag"
    fi
    echo "ImageUri: $image"
    params=("ImageUri=$image")
    [[ -n "${3:-}" ]] && params+=("AlertEmail=$3")
    aws_ cloudformation deploy --stack-name omics-sra-app \
      --template-file "$HERE/app.yaml" --capabilities CAPABILITY_IAM \
      --parameter-overrides "${params[@]}"
    aws_ cloudformation describe-stacks --stack-name omics-sra-app \
      --query 'Stacks[0].Outputs' --output table
    ;;
  *)
    sed -n '2,10p' "$0"; exit 2 ;;
esac
