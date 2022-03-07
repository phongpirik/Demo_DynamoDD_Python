import boto3


#Table Name
table_name = "user"

#item 
item_scarface = {
    'id':{'S':'2'},
    'username':{'S':'San'}
}

#dynamodb client
dynamodb_client = boto3.client('dynamodb')

print("Phong")
if __name__ == "__main__":
    dynamodb_client.put_item(TableName = table_name, Item = item_scarface)

