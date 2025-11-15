import boto3
import os

class DynamoDB:
    def __init__(self):
        self.client = boto3.resource('dynamodb')

    def put_item(self, table_name, item):
        table = self.client.Table(table_name)
        table.put_item(Item=item)

    def update_item(self, table_name, key, update_expression, expression_values, expression_names=None):
        table = self.client.Table(table_name)
        kwargs = {
            'Key': key,
            'UpdateExpression': update_expression,
            'ExpressionAttributeValues': expression_values
        }
        if expression_names:
            kwargs['ExpressionAttributeNames'] = expression_names
        table.update_item(**kwargs)

    def query(self, table_name, key_condition_expression, expression_attribute_values):
        table = self.client.Table(table_name)
        return table.query(
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def scan(self, table_name, filter_expression=None, expression_attribute_values=None):
        table = self.client.Table(table_name)
        if filter_expression:
            return table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_attribute_values
            )
        return table.scan()

    def get_item(self, table_name, key):
        table = self.client.Table(table_name)
        return table.get_item(Key=key)
