from boto3 import client as boto3_client
import face_recognition
import pickle
import os

input_bucket = "546proj2-oneszeros"
output_bucket = "546proj2output-oneszeros"
dynamodb_name = "546StudentData"
path = "/tmp/"

cwd = os.getcwd()

def extract_frames(video_file_path):
	resp = os.system("ffmpeg -i " + str(video_file_path) + " -r 1 " + str(path) + "image-%3d.jpeg")
	if resp == 0:
		return True
	else:
		return False
	
def clear_tmp(video_path, frames = None):
	if os.path.exists(video_path):
		os.remove(video_path)
	if frames is not None:
		for frame in frames:
			if os.path.exists(frame):
				os.remove(frame)

def open_encoding():
	with open(f"{cwd}/encoding", "rb") as f:
		known_encodings = pickle.load(f)
	names = known_encodings.get('name')
	known_encodings = known_encodings.get('encoding')
	return names, known_encodings

def read_s3_file_into_filesystem(key):
	s3 = boto3_client("s3")
	obj = s3.get_object(Bucket=input_bucket, Key=key)
	video = obj["Body"].read()
	with open(path + key, "wb") as f:
		f.write(video)
	return path + key

def search_dyno_table(name):
	dynamodb = boto3_client("dynamodb")
	resp = dynamodb.scan(
		TableName=dynamodb_name,
		FilterExpression='contains(#name, :name_val)',
		ExpressionAttributeNames={
			'#name': 'name',
		},
		ExpressionAttributeValues={
			':name_val': {'S': name}
		}
	)
	items = resp['Items']
	if len(items) == 0:
		return None
	parsed_item = {k: list(v.values())[0] for k, v in items[0].items()}
	return parsed_item

def create_csv_file(name, result):
	result_path = path + f"{name}.csv"
	with open(result_path, "w") as f:
		for item in result:
			f.write(f"{item['name']},{item['major']},{item['year']}\n")
	return result_path

def delete_csv_file(result_path):
	if os.path.exists(result_path):
		os.remove(result_path)

def upload_to_s3(key, result):
	name = key.split('.')[0]
	result_path = create_csv_file(name, result)
	s3 = boto3_client("s3")
	s3.upload_file(result_path, output_bucket, name)
	delete_csv_file(result_path)

def face_recognition_handler(event, context):
	key = event["Records"][0]["s3"]["object"]["key"]
	print(key)
	video_path = read_s3_file_into_filesystem(key)
	flag = extract_frames(video_path)
	if not flag:
		clear_tmp(video_path)
		return {
			'statusCode': 200,
			'body': "Error in extracting frames"
		}
	frames = [path + f for f in os.listdir(path) if f.endswith(".jpeg")]
	names, known_encodings = open_encoding()
	results = []
	for frame in frames:
		result = recognise(frame, names, known_encodings)
		if result is None:
			continue
		result = search_dyno_table(result)
		results.append(result)
		break
	upload_to_s3(key, results)
	clear_tmp(video_path, frames)
	return {
		'statusCode': 200,
		'body': "Success"
	}

def recognise(imgpath, names, known_encodings):
	img = face_recognition.load_image_file(imgpath)
	unknown_encodings = face_recognition.face_encodings(img)
	for unknown_encoding in unknown_encodings:
		results = face_recognition.compare_faces(known_encodings, unknown_encoding)
		for zip_result in zip(results, names):
			if zip_result[0]:
				return zip_result[1]