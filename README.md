# HW6 Widget Request Consumer

The Python consumer retrieves widget requests from either an S3 bucket or an
SQS queue and stores widgets in S3 or DynamoDB. Create, partial update, and
delete requests are supported. S3 remains the default request source for
backwards compatibility.

## Setup

Install the AWS SDK dependency:

```powershell
python -m pip install -r requirements.txt
```

Configure AWS credentials using the standard AWS shared credentials file or
environment. Never put access keys in source code or command-line arguments.
The default AWS region is `us-east-1`.

## Run

Read requests from the default S3 request bucket and store widgets in S3:

```powershell
python consumer.py --storage s3
```

Read requests from the `cs5270-requests` SQS queue and store widgets in
DynamoDB:

```powershell
python consumer.py --request-source sqs --queue-name cs5270-requests --storage dynamodb
```

Select the request source with `--request-source s3|sqs`. S3 is the default;
set `--request-bucket` to choose another bucket. For SQS, set `--request-queue`
or its alias `--queue-name` to choose another queue. The consumer resolves the
queue name to its AWS queue URL and retrieves up to ten messages per receive
call. A message is deleted from SQS using its receipt handle only after the
request is successfully processed. Processing failures leave the message
available for retry after its visibility timeout.

Use `--storage s3` or `--storage dynamodb` (also accepted as
`--storage-strategy`) to select where widgets are stored. The defaults are
widget bucket `usu-cs5270-golf-web` and DynamoDB table `widgets`. Override them
with `--widget-bucket`, `--table-name`, `--region`, or `--profile`.

Create requests store the widget. Update requests change only the supplied
widget fields, preserving other existing attributes. Delete requests remove
the widget. Use `--max-requests N` for a bounded run. Logs are written to
`consumer.log` by default; change the location with `--log-file`.

## Tests

Run the unit tests without AWS access:

```powershell
python -m unittest discover -s tests -v
```

## Docker

The Docker image runs the SQS consumer against the `cs5270-requests` queue and
stores widgets in DynamoDB by default. The image does not contain AWS
credentials. Update the placeholder values in the local `.env` file before
running it; `.env` is excluded from Git and from the Docker build context.
For AWS SDK credential discovery, the secret variable must be named
`AWS_SECRET_ACCESS_KEY` (not `AWS_SECRET_KEY`). `AWS_SESSION_TOKEN` is needed
when using temporary credentials.

Build and run the image from the project directory:

```powershell
docker build -t widget-consumer .
docker run --rm --env-file .env widget-consumer
```

The container uses `us-east-1` by default. To override queue, region, or
storage settings, pass the desired Consumer options after the image name:

```powershell
docker run --rm --env-file .env widget-consumer `
  --request-source sqs --queue-name cs5270-requests --storage dynamodb --region us-east-1
```

To keep the log file outside the container, mount a local directory and set
`--log-file` to a path inside the mount.
