#!/usr/bin/env bash
# The script pre-configures the SNS and SQS queues and their subscriptions.

# enable debug
sleep 5;

set -x

export AWS_ACCESS_KEY_ID=FAKE
export AWS_SECRET_ACCESS_KEY=FAKE
export AWS_REGION=us-east-1
mkdir ~/.aws
echo "[default]" > ~/.aws/config
echo "region = us-east-1" >> ~/.aws/config
echo "output = json" >> ~/.aws/config

echo "Creating development stack..."
aws --endpoint-url=http://localhost:4566 \
    cloudformation deploy --stack-name stack \
    --template-file "/opt/templates/localstack-cf.yml" --region ${AWS_REGION}
