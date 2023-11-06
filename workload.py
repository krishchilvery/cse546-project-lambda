from boto3 import client as boto3_client
import os
import re

input_bucket = "546proj2-oneszeros"
output_bucket = "546proj2output-oneszeros"
test_cases = "test_cases/"

def clear_input_bucket():
	global input_bucket
	s3 = boto3_client('s3')
	list_obj = s3.list_objects_v2(Bucket=input_bucket)
	try:
		for item in list_obj["Contents"]:
			key = item["Key"]
			s3.delete_object(Bucket=input_bucket, Key=key)
	except:
		print("Nothing to clear in input bucket")
	
def clear_output_bucket():
	global output_bucket
	s3 = boto3_client('s3')
	list_obj = s3.list_objects_v2(Bucket=output_bucket)
	try:
		for item in list_obj["Contents"]:
			key = item["Key"]
			s3.delete_object(Bucket=output_bucket, Key=key)
	except:
		print("Nothing to clear in output bucket")

def upload_to_input_bucket_s3(path, name):
	global input_bucket
	s3 = boto3_client('s3')
	s3.upload_file(path + name, input_bucket, name)
	
	
def upload_files(test_case):	
	global input_bucket
	global output_bucket
	global test_cases
	
	
	# Directory of test case
	test_dir = test_cases + test_case + "/"
	
	# Iterate over each video
	# Upload to S3 input bucket
	for filename in os.listdir(test_dir):
		if filename.endswith(".mp4") or filename.endswith(".MP4"):
			print("Uploading to input bucket..  name: " + str(filename)) 
			upload_to_input_bucket_s3(test_dir, filename)

def read_mapping():
	results = []
	with open("mapping", "r") as f:
		for line in f.readlines():
			line = line.strip()
			filename, major, year = re.split(":|,", line)
			key = filename.split(".")[0]	
			if line:
				results.append((key, major, year))
	return results

def verify_outputs():
	global output_bucket
	expected_results = read_mapping()
	s3 = boto3_client('s3')
	total = len(expected_results)
	count = 0
	for key, major, year in expected_results:
		obj = s3.get_object(Bucket=output_bucket, Key=key)
		result = obj["Body"].read().decode("utf-8")
		result = result.strip()
		name, predicted_major, predicted_year = result.split(",")
		if predicted_major != major or predicted_year != year:
			print("Error in output for " + key)
			print("Expected: " + major + ", " + year)
			print("Got: " + predicted_major + ", " + predicted_year)
		else:
			print("Verified output for " + key)
			count += 1
	print("Total: " + str(total))
	print("Verified: " + str(count))
	print("Accuracy: " + str(count/total))

	
def workload_generator():
	
	print("Running Test Case 1")
	upload_files("test_case_1")

	print("Running Test Case 2")
	upload_files("test_case_2")
	

# First Run the workload generator
# Then run the verify outputs
if __name__ == "__main__":
	clear_input_bucket()
	clear_output_bucket()	
	workload_generator()	
	# verify_outputs()
	

