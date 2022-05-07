import os
import boto3
import eval_face_recognition
import json
from boto3.dynamodb.conditions import Attr

TABLE_NAME = "cc_project_2_student_info"
RESOURCE_REGION = "us-east-1"

dynamodb_client = boto3.client('dynamodb', region_name=RESOURCE_REGION)
dynamodb = boto3.resource('dynamodb', region_name=RESOURCE_REGION)
s3 = boto3.resource('s3', region_name = 'us-east-1')
table = dynamodb.Table(TABLE_NAME)
lambdaClient = boto3.client('lambda')

def face_recognition_handler(event, context):
	request = json.loads(event['body'])
	imageName = request['ImageName']
	s3ImageName = 'Frames/' + imageName
	bucketName = request['BucketName']

	# localImagePath = os.path.join("/tmp", "Images")
	localImagePath = "/tmp/Images/"
	if not os.path.exists(localImagePath):
		os.makedirs(localImagePath)
	localImagePath = os.path.join(localImagePath, imageName)
	s3.Bucket(bucketName).download_file(s3ImageName, localImagePath)
	print(os.path.exists(localImagePath))
	if (os.path.exists(localImagePath)):
		output = eval_face_recognition.evaluate(localImagePath)
		print(output)
		if output:
			os.remove(localImagePath)

		response = table.scan(
			FilterExpression=Attr('Key').eq(output['result']),
			ProjectionExpression = '#n, Major, Graduation_Year',
			ExpressionAttributeNames = {'#n': 'Name'}
		)
		print(response)
		payload = response['Items'][0]
		payload['Image_Name'] = imageName

		print("Dynamo DB Result:", payload)

		return {
			"statusCode": 200,
			"body": payload
		}
	else:
		return {
			"statusCode": 500,
			"body": "File not saved"
		}