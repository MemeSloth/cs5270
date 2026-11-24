import io
import json
import logging
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from threading import Event
from unittest.mock import Mock
from unittest.mock import patch

from consumer import (
    Consumer,
    DynamoDBWidgetStorage,
    RequestMessage,
    S3RequestSource,
    S3WidgetStorage,
    SQSRequestSource,
    UnsupportedRequestTypeError,
    Widget,
    WidgetRequestProcessor,
    build_argument_parser,
    configure_logging,
    create_consumer,
)


def create_request(**changes):
    request = {
        "type": "create",
        "requestId": "request-1",
        "widgetId": "widget-1",
        "owner": "Mary Matthews",
        "label": "sample",
        "description": "test widget",
        "otherAttributes": [
            {"name": "width-unit", "value": "cm"},
            {"name": "rating", "value": "2.5"},
        ],
    }
    request.update(changes)
    return request


class WidgetTests(unittest.TestCase):
    def test_create_request_maps_core_and_flat_attributes(self):
        widget = Widget.from_create_request(create_request())

        self.assertEqual(widget.widget_id, "widget-1")
        self.assertEqual(widget.other_attributes, {"width-unit": "cm", "rating": "2.5"})
        self.assertEqual(
            widget.to_s3_document(),
            {
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
                "label": "sample",
                "description": "test widget",
                "width-unit": "cm",
                "rating": "2.5",
            },
        )
        self.assertEqual(
            widget.to_dynamodb_item(),
            {
                "widget_id": "widget-1",
                "owner": "Mary Matthews",
                "label": "sample",
                "description": "test widget",
                "width-unit": "cm",
                "rating": "2.5",
            },
        )

    def test_rejects_duplicate_or_core_field_other_attributes(self):
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            Widget.from_create_request(
                create_request(
                    otherAttributes=[
                        {"name": "size", "value": "small"},
                        {"name": "size", "value": "large"},
                    ]
                )
            )

        with self.assertRaisesRegex(ValueError, "conflicts"):
            Widget.from_create_request(
                create_request(otherAttributes=[{"name": "owner", "value": "Someone"}])
            )

    def test_optional_core_attributes_may_be_absent(self):
        request = create_request(otherAttributes=[])
        request.pop("label")
        request.pop("description")
        widget = Widget.from_create_request(request)

        self.assertEqual(widget.to_s3_document(), {"widgetId": "widget-1", "owner": "Mary Matthews"})
        self.assertIsNone(widget.to_dynamodb_item()["label"])

    def test_create_request_requires_schema_fields_and_rejects_null_strings(self):
        with self.assertRaisesRegex(ValueError, "requestId"):
            Widget.from_create_request(create_request(requestId=None))
        with self.assertRaisesRegex(ValueError, "label"):
            Widget.from_create_request(create_request(label=None))


class S3RequestSourceTests(unittest.TestCase):
    def test_reads_and_deletes_single_listed_request_before_returning_it(self):
        client = Mock()
        client.list_objects_v2.return_value = {"Contents": [{"Key": "request-1"}]}
        client.get_object.return_value = {"Body": io.BytesIO(b'{"type": "create"}')}
        events = []
        client.list_objects_v2.side_effect = lambda **kwargs: (
            events.append(("list", kwargs)) or {"Contents": [{"Key": "request-1"}]}
        )
        client.get_object.side_effect = lambda **kwargs: (
            events.append(("get", kwargs))
            or {"Body": io.BytesIO(b'{"type": "create"}')}
        )
        client.delete_object.side_effect = lambda **kwargs: events.append(("delete", kwargs))
        source = S3RequestSource(client, "requests")

        request = source.get_next_request()

        self.assertEqual(request, {"type": "create"})
        self.assertEqual([event[0] for event in events], ["list", "get", "delete"])
        self.assertEqual(events[0][1], {"Bucket": "requests", "MaxKeys": 1})
        self.assertEqual(events[2][1], {"Bucket": "requests", "Key": "request-1"})

    def test_returns_none_when_bucket_has_no_request(self):
        client = Mock()
        client.list_objects_v2.return_value = {}

        self.assertIsNone(S3RequestSource(client, "requests").get_next_request())
        client.get_object.assert_not_called()
        client.delete_object.assert_not_called()

    def test_deletes_but_rejects_non_object_json(self):
        client = Mock()
        client.list_objects_v2.return_value = {"Contents": [{"Key": "request-1"}]}
        client.get_object.return_value = {"Body": io.BytesIO(b"[]")}

        with self.assertRaisesRegex(ValueError, "JSON object"):
            S3RequestSource(client, "requests").get_next_request()

        client.delete_object.assert_called_once()

    def test_deletes_and_reports_malformed_json(self):
        client = Mock()
        client.list_objects_v2.return_value = {"Contents": [{"Key": "request-1"}]}
        client.get_object.return_value = {"Body": io.BytesIO(b"")}

        with self.assertRaises(json.JSONDecodeError):
            S3RequestSource(client, "requests").get_next_request()

        client.delete_object.assert_called_once()


class StorageTests(unittest.TestCase):
    def test_s3_storage_uses_required_key_and_json_document(self):
        client = Mock()
        widget = Widget.from_create_request(create_request())

        S3WidgetStorage(client, "widgets-bucket").store(widget)

        call = client.put_object.call_args.kwargs
        self.assertEqual(call["Bucket"], "widgets-bucket")
        self.assertEqual(call["Key"], "widgets/mary-matthews/widget-1")
        self.assertEqual(call["ContentType"], "application/json")
        self.assertEqual(
            json.loads(call["Body"].decode("utf-8")),
            widget.to_s3_document(),
        )

    def test_dynamodb_storage_writes_flat_item(self):
        table = Mock()
        widget = Widget.from_create_request(create_request())

        DynamoDBWidgetStorage(table).store(widget)

        table.put_item.assert_called_once_with(Item=widget.to_dynamodb_item())
        self.assertNotIn("otherAttributes", table.put_item.call_args.kwargs["Item"])

    def test_s3_storage_updates_existing_document_and_deletes_by_owner_key(self):
        client = Mock()
        client.get_object.return_value = {
            "Body": io.BytesIO(
                b'{"widgetId":"widget-1","owner":"Mary Matthews",'
                b'"label":"old","description":"preserve","width-unit":"cm"}'
            )
        }
        storage = S3WidgetStorage(client, "widgets-bucket")

        storage.update("widget-1", "Mary Matthews", {"owner": "Mary Matthews", "label": "new"})

        call = client.put_object.call_args.kwargs
        self.assertEqual(call["Key"], "widgets/mary-matthews/widget-1")
        self.assertEqual(
            json.loads(call["Body"].decode("utf-8")),
            {
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
                "label": "new",
                "description": "preserve",
                "width-unit": "cm",
            },
        )
        client.get_object.assert_called_once_with(
            Bucket="widgets-bucket",
            Key="widgets/mary-matthews/widget-1",
        )

        storage.delete("widget-1", "Mary Matthews")

        client.delete_object.assert_called_once_with(
            Bucket="widgets-bucket",
            Key="widgets/mary-matthews/widget-1",
        )

    def test_s3_storage_rejects_non_object_existing_widget(self):
        client = Mock()
        client.get_object.return_value = {"Body": io.BytesIO(b"[]")}

        with self.assertRaisesRegex(ValueError, "JSON object"):
            S3WidgetStorage(client, "widgets-bucket").update(
                "widget-1", "Mary Matthews", {"owner": "Mary Matthews"}
            )

        client.put_object.assert_not_called()

    def test_dynamodb_storage_updates_and_deletes_by_widget_id(self):
        table = Mock()
        storage = DynamoDBWidgetStorage(table)

        storage.update(
            "widget-1",
            "Mary Matthews",
            {"owner": "Mary Matthews", "label": "new"},
        )

        table.update_item.assert_called_once_with(
            Key={"widget_id": "widget-1"},
            UpdateExpression="SET #field0 = :value0, #field1 = :value1",
            ConditionExpression="attribute_exists(#key)",
            ExpressionAttributeNames={
                "#key": "widget_id",
                "#field0": "owner",
                "#field1": "label",
            },
            ExpressionAttributeValues={
                ":value0": "Mary Matthews",
                ":value1": "new",
            },
        )
        storage.delete("widget-1", "Mary Matthews")
        table.delete_item.assert_called_once_with(Key={"widget_id": "widget-1"})


class ProcessorTests(unittest.TestCase):
    def test_processes_create_request(self):
        storage = Mock()

        WidgetRequestProcessor(storage).process(create_request())

        stored_widget = storage.store.call_args.args[0]
        self.assertIsInstance(stored_widget, Widget)
        self.assertEqual(stored_widget.widget_id, "widget-1")

    def test_processes_update_as_a_partial_attribute_change(self):
        storage = Mock()
        request = create_request(
            type="WidgetUpdateRequest",
            label="revised",
            otherAttributes=[{"name": "rating", "value": "4"}],
        )
        request.pop("description")

        WidgetRequestProcessor(storage).process(request)

        storage.update.assert_called_once_with(
            "widget-1",
            "Mary Matthews",
            {"owner": "Mary Matthews", "label": "revised", "rating": "4"},
        )
        storage.store.assert_not_called()
        storage.delete.assert_not_called()

    def test_processes_delete_request(self):
        storage = Mock()

        WidgetRequestProcessor(storage).process(
            create_request(type="WidgetDeleteRequest")
        )

        storage.delete.assert_called_once_with("widget-1", "Mary Matthews")
        storage.store.assert_not_called()
        storage.update.assert_not_called()

    def test_rejects_invalid_update_fields_and_unknown_request_type(self):
        processor = WidgetRequestProcessor(Mock())

        with self.assertRaisesRegex(ValueError, "label"):
            processor.process(create_request(type="update", label=None))
        with self.assertRaisesRegex(ValueError, "widgetId"):
            processor.process(create_request(type="delete", widgetId=None))
        with self.assertRaisesRegex(ValueError, "otherAttributes"):
            processor.process(create_request(type="update", otherAttributes="invalid"))
        with self.assertRaisesRegex(ValueError, "conflicts"):
            processor.process(
                create_request(
                    type="update",
                    otherAttributes=[{"name": "owner", "value": "Someone"}],
                )
            )
        with self.assertRaises(UnsupportedRequestTypeError):
            processor.process(create_request(type="archive"))

    def test_accepts_schema_create_type_spelling(self):
        storage = Mock()

        WidgetRequestProcessor(storage).process(
            create_request(type="WidgetCreateRequest")
        )

        storage.store.assert_called_once()


class ConsumerTests(unittest.TestCase):
    def test_run_processes_one_request_and_stops_at_limit(self):
        source = Mock()
        source.get_next_request.return_value = create_request()
        storage = Mock()
        logger = Mock()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(storage),
            sleep=Mock(),
            logger=logger,
        )

        consumed = consumer.run(max_requests=1)

        self.assertEqual(consumed, 1)
        source.get_next_request.assert_called_once()
        storage.store.assert_called_once()
        logger.info.assert_called_once_with(
            "Processed widget request request_id=%s", "request-1"
        )

    def test_empty_source_waits_between_polls(self):
        source = Mock()
        source.get_next_request.return_value = None
        sleep = Mock(side_effect=lambda _: stop_event.set())
        stop_event = Event()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(Mock()),
            poll_interval_seconds=0.1,
            sleep=sleep,
        )

        self.assertEqual(consumer.run(stop_event=stop_event), 0)
        sleep.assert_called_once_with(0.1)
        source.get_next_request.assert_called_once()

    def test_logs_processing_failure_and_counts_consumed_request(self):
        source = Mock()
        source.get_next_request.return_value = {"type": "create"}
        logger = Mock()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(Mock()),
            logger=logger,
        )

        self.assertEqual(consumer.run(max_requests=1), 1)
        logger.exception.assert_called_once_with("Failed to process widget request")

    def test_warns_for_unknown_request_type(self):
        source = Mock()
        source.get_next_request.return_value = create_request(type="archive")
        logger = Mock()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(Mock()),
            logger=logger,
        )

        self.assertEqual(consumer.run(max_requests=1), 1)
        logger.warning.assert_called_once()

    def test_acknowledges_sqs_message_only_after_successful_processing(self):
        acknowledge = Mock()
        source = Mock()
        source.get_next_request.return_value = RequestMessage(
            create_request(),
            acknowledge_callback=acknowledge,
        )
        consumer = Consumer(source, WidgetRequestProcessor(Mock()), logger=Mock())

        self.assertEqual(consumer.run(max_requests=1), 1)

        acknowledge.assert_called_once_with()

    def test_does_not_acknowledge_message_when_processing_fails(self):
        acknowledge = Mock()
        source = Mock()
        source.get_next_request.return_value = RequestMessage(
            {"type": "create"},
            acknowledge_callback=acknowledge,
        )
        consumer = Consumer(source, WidgetRequestProcessor(Mock()), logger=Mock())

        self.assertEqual(consumer.run(max_requests=1), 1)

        acknowledge.assert_not_called()


class SQSRequestSourceTests(unittest.TestCase):
    def test_fetches_up_to_ten_messages_and_caches_remaining_requests(self):
        client = Mock()
        client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": '{"type":"create","requestId":"request-1"}',
                    "ReceiptHandle": "receipt-1",
                },
                {
                    "Body": '{"type":"delete","requestId":"request-2"}',
                    "ReceiptHandle": "receipt-2",
                },
            ]
        }
        source = SQSRequestSource(client, "https://sqs.example/queue")

        first = source.get_next_request()
        second = source.get_next_request()

        self.assertEqual(first.request["requestId"], "request-1")
        self.assertEqual(second.request["requestId"], "request-2")
        client.receive_message.assert_called_once_with(
            QueueUrl="https://sqs.example/queue",
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )
        first.acknowledge()
        second.acknowledge()
        self.assertEqual(
            client.delete_message.call_args_list[0].kwargs,
            {"QueueUrl": "https://sqs.example/queue", "ReceiptHandle": "receipt-1"},
        )
        self.assertEqual(
            client.delete_message.call_args_list[1].kwargs,
            {"QueueUrl": "https://sqs.example/queue", "ReceiptHandle": "receipt-2"},
        )

    def test_returns_none_for_empty_queue(self):
        client = Mock()
        client.receive_message.return_value = {}

        self.assertIsNone(SQSRequestSource(client, "queue-url").get_next_request())

    def test_rejects_non_object_message_body(self):
        client = Mock()
        client.receive_message.return_value = {
            "Messages": [{"Body": "[]", "ReceiptHandle": "receipt-1"}]
        }

        with self.assertRaisesRegex(ValueError, "JSON object"):
            SQSRequestSource(client, "queue-url").get_next_request()

        client.delete_message.assert_not_called()

class CommandLineAndLoggingTests(unittest.TestCase):
    def test_parser_defaults_to_provided_aws_resources(self):
        arguments = build_argument_parser().parse_args(["--storage", "s3"])

        self.assertEqual(arguments.request_bucket, "usu-cs5270-golf-requests")
        self.assertEqual(arguments.widget_bucket, "usu-cs5270-golf-web")
        self.assertEqual(arguments.table_name, "widgets")
        self.assertEqual(arguments.region, "us-east-1")
        self.assertEqual(arguments.request_source, "s3")
        self.assertEqual(arguments.request_queue, "cs5270-requests")

    def test_parser_accepts_sqs_source_and_queue_name(self):
        arguments = build_argument_parser().parse_args(
            ["--storage", "dynamodb", "--request-source", "sqs", "--queue-name", "requests"]
        )

        self.assertEqual(arguments.request_source, "sqs")
        self.assertEqual(arguments.request_queue, "requests")

    @patch("boto3.Session")
    def test_create_consumer_resolves_sqs_queue_name(self, session_factory):
        session = session_factory.return_value
        session.client.return_value.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.example/requests"
        }
        arguments = build_argument_parser().parse_args(
            ["--storage", "s3", "--request-source", "sqs", "--request-queue", "requests"]
        )

        consumer = create_consumer(arguments)

        session.client.assert_any_call("sqs")
        session.client.return_value.get_queue_url.assert_called_once_with(
            QueueName="requests"
        )
        self.assertIsInstance(consumer._request_source, SQSRequestSource)
        self.assertEqual(consumer._request_source._queue_url, "https://sqs.example/requests")

    @patch("boto3.Session")
    def test_create_consumer_keeps_s3_request_source_as_default(self, session_factory):
        session = session_factory.return_value
        arguments = build_argument_parser().parse_args(
            ["--storage", "s3", "--request-bucket", "legacy-requests"]
        )

        consumer = create_consumer(arguments)

        self.assertIsInstance(consumer._request_source, S3RequestSource)
        self.assertEqual(consumer._request_source._bucket_name, "legacy-requests")
        session.client.assert_called_once_with("s3")

    def test_parser_accepts_dynamodb_strategy_and_request_limit(self):
        arguments = build_argument_parser().parse_args(
            ["--storage-strategy", "dynamodb", "--max-requests", "3"]
        )

        self.assertEqual(arguments.storage, "dynamodb")
        self.assertEqual(arguments.max_requests, 3)

    def test_parser_rejects_invalid_poll_intervals(self):
        parser = build_argument_parser()

        for interval in ("-1", "nan", "inf"):
            with self.subTest(interval=interval), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parser.parse_args(
                        ["--storage", "s3", "--poll-interval-ms", interval]
                    )

    def test_logging_writes_to_requested_file(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "consumer.log"
            configure_logging(log_path)
            logging.getLogger("consumer-test").info("test log entry")
            logging.shutdown()

            self.assertIn("test log entry", log_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
