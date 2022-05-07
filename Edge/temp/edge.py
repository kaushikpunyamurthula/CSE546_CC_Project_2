 #!/usr/bin/python3
from collections import deque
from datetime import datetime
from threading import Thread, current_thread
from queue import Queue
from datetime import datetime
from botocore.exceptions import ClientError

import os
import cv2
import time
import sys
import logging
import configparser
import boto3
import json
import picamera
import requests

config = configparser.ConfigParser()
config.read('/home/pi/Edge/configuration.properties')
logging.basicConfig(filename='/home/pi/Edge/edge.log', level=logging.INFO, 
    format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s", filemode='w')

resource_region = config['AWSSection']['Region']
# s3 = boto3.resource('s3', region_name = resource_region)
s3Client = boto3.client('s3', region_name = resource_region)
sqs = boto3.client('sqs', region_name = resource_region)

processTime = sys.argv[1]
recordTime = 0.5

if processTime == "inf":
    processTimeout = time.time() + 9223372036854775807
else:
    processTimeout = time.time() + int(processTime) * 60

threadStatusMap = [{
    "record" : False,
    "process" : False,
    "upload" : False,
    "lambda" : False
} for _ in range(6)]

logging.info(threadStatusMap)

videoResultMap = {}

VIDEO_PATH = os.path.join(os.path.dirname(__file__),'Videos')
FRAME_PATH = os.path.join(os.path.dirname(__file__),'Frames')

if not os.path.exists(VIDEO_PATH):
    os.makedirs(VIDEO_PATH)

if not os.path.exists(FRAME_PATH):
    os.makedirs(FRAME_PATH)

def record_video(duration, queue):
    startTime = time.time()
    logging.info("Recording Start Time: " + str(datetime.now()))
    camera = picamera.PiCamera()
    videoFile = os.path.join(VIDEO_PATH, "video-{}.h264")
    filePath = videoFile.format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f"))
    camera.start_recording(filePath, format="h264")
    camera.wait_recording(duration)
    camera.stop_recording()
    camera.close()
    logging.info("Recording End Time: " + str(datetime.now()))
    queue.put(filePath)
    videoResultMap[filePath.split("/")[-1]] = {
        "startTime": startTime
    }

def processVideoToFrames(filePath, queue):
    logging.info("Processing Start Time: " + str(datetime.now()))
    imageFilePath = os.path.join(FRAME_PATH, filePath.split("/")[-1].split(".")[0] + ".jpg")
    vidcap = cv2.VideoCapture(filePath)
    success,image = vidcap.read()
    frameCount = 0
    while success:
        if frameCount == 7:
            resizedImage = cv2.resize(image, (160, 160))
            rgbImg = cv2.cvtColor(resizedImage, cv2.COLOR_RGBA2RGB)
            cv2.imwrite(imageFilePath, rgbImg)
            break
        success,image = vidcap.read()
        frameCount += 1
    logging.info("Processing End Time: " + str(datetime.now()))
    queue.put(imageFilePath)

def uploadFiles(queue, videoFilePath, frameFilePath):
    try:
        logging.info("Uploading Start Time: " + str(datetime.now()))
        bucketName = config['S3Section']['S3_Bucket_Name']
        logging.info("Video File Path: " + str(videoFilePath) + ", Frame File Path: " + str(frameFilePath) + ", Bucket: " + str(bucketName))
        uploaded = 0
        if videoFilePath != None:
            videoFileName = "Videos/" + videoFilePath.split("/")[-1]
            s3Client.upload_file(Filename=videoFilePath, Bucket=bucketName, Key=videoFileName)
            uploaded += 1
        if frameFilePath != None:
            frameFileName = "Frames/" + frameFilePath.split("/")[-1]
            s3Client.upload_file(Filename=frameFilePath, Bucket=bucketName, Key=frameFileName)
            uploaded += 1
        queue.put((uploaded, frameFilePath))
        logging.info("Uploading End Time: " + str(datetime.now()))
    except ClientError as e:
        logging.info(e)
    except FileNotFoundError as e:
        logging.info(e)

def getFaceRecognitionResult(queue, framePath):
    logging.info("Lambda Receiving Start Time: " + str(datetime.now()))
    bucketName = str(config['S3Section']['S3_Bucket_Name'])
    lambdaUrl = str(config['LambdaSection']['LAMBDA_FUNCTION_URL'])
    framePath = framePath.split('/')[-1]
    logging.info("Image: " + framePath + ", Bucket: " + bucketName + ", Lambda Url: " + lambdaUrl)
    payload = {
        "ImageName": str(framePath),
        "BucketName": str(bucketName)
    }
    headers = { "Content-Type": "application/json" }
    response = requests.post(url=lambdaUrl, headers=headers, json=payload)
    # output = json.loads(response['data'])
    logging.info(response.text)
    output = json.loads(response.text)
    queue.put((output, current_thread()))
    logging.info("Lambda Receiving End Time: " + str(datetime.now()))

def updateProcessStatus(count, type):
    if count > 0:
        threadStatusMap[count-1][type] = False
    else:
        threadStatusMap[5][type] = False
    count += 1
    count %= 6
    return count

recordResult, processResult, uploadResult, lambdaResult = deque(), deque(), deque(), deque()
recordCount, processCount, uploadCount, lambdaCount = 0, 0, 0, 0
count = 0

logging.info("Edge Process Started")
flag = False

lambdaQueue = Queue()
lambdaThreadList = deque()
lambdaThreadStatus = False

while True:
    logging.info("-----------------------------------------------------------------------------------------------------------")
    logging.info("Iteration #" + str(count))
    recordThread, processThread, uploadThread = None, None, None
    if not threadStatusMap[recordCount]["record"] and not flag:
        threadStatusMap[recordCount]["record"] = True
        recordQueue = Queue()
        recordThread = Thread(target=record_video, args=(recordTime, recordQueue))

    if recordResult and not threadStatusMap[processCount]["process"]:
        threadStatusMap[processCount]["process"] = True
        processQueue = Queue()
        processThread = Thread(target=processVideoToFrames, args=(recordResult[0], processQueue))            

    if processResult and not threadStatusMap[uploadCount]["upload"]:
        threadStatusMap[uploadCount]["upload"] = True
        uploadQueue = Queue()
        uploadThread = Thread(target=uploadFiles, args=(uploadQueue, recordResult.popleft() if recordResult else None, processResult.popleft()))

    if uploadResult:
        # threadStatusMap[lambdaCount]["lambda"] = True
        framePath = uploadResult.popleft()
        logging.info("Lambda results for: " + framePath)
        lambdaThreadList.append(Thread(target=getFaceRecognitionResult, args=(lambdaQueue, framePath)))
        # lambdaQueue = Queue()

        # lambdaThread = Thread(target=getFaceRecognitionResult, args=(lambdaQueue, uploadResult.popleft()))
        if not threadStatusMap[processCount]["process"]:
            processResult = None
        if not threadStatusMap[recordCount]["record"]:
            recordResult = None
        if not threadStatusMap[uploadCount]["upload"]:
            uploadResult = None

    if recordThread != None:
        recordThread.start()
        
    if processThread != None:
        processThread.start()

    if uploadThread != None:
        uploadThread.start()

    if lambdaThreadList:
        lambdaThreadList.popleft().start()
    
    if recordThread != None:
        recordThread.join()
        recordResult.append(recordQueue.get())
        recordCount = updateProcessStatus(recordCount, "record")
        logging.info("Record Count: " + str(recordCount) + ", Recorded Video: " + recordResult[-1])

    if processThread != None:
        processThread.join()
        processResult.append(processQueue.get())
        processCount = updateProcessStatus(processCount, "process")
        logging.info("Process Count: " + str(processCount) + ", Processed Frame: " + processResult[-1])

    if uploadThread != None:
        uploadThread.join()
        uploaded, framePath = uploadQueue.get()
        logging.info(uploaded)
        if uploaded == 2:
            uploadResult.append(framePath)
            uploadCount = updateProcessStatus(uploadCount, "upload")
            logging.info("Upload Count: " + str(uploadCount) + ", Uploaded Video and Frame")
        elif uploaded == 1:
            logging.info("Upload Count: " + str(uploadCount) + ", Uploaded Video")
        else:
            logging.info("Failed to upload video or frame to S3 bucket")
            break

    if not lambdaQueue.empty():
        # lambdaThread.join()
        result, thread = lambdaQueue.get()
        lambdaResult.append(result)
        # lambdaThreadList.remove(thread)
        # lambdaCount = updateProcessStatus(lambdaCount, "lambda")
        output = lambdaResult.popleft()
        imageName = output['Image_Name']
        videoName = imageName.replace('.jpg', '.h264')
        if videoName in videoResultMap:
            endTime = time.time()
            videoResultMap[videoName]['endTime'] = endTime
            latency = endTime - videoResultMap[videoName]['startTime']
            videoResultMap.pop(videoName)
            logging.info("Video: " + videoName + ", Result: " + str(output) + ", Latency: {:.2f} seconds.".format(latency))
            # print("Video: " + videoName + "Result: " + str(output) + ", Latency: {:.2f} seconds.".format(latency))

    count += 1
    # if time.time() > processTimeout:
    if count > 25:
        flag = True
    if flag and recordResult is None and processResult is None and uploadResult is None and len(videoResultMap) == 0:
        break

print(videoResultMap)
if os.path.exists(VIDEO_PATH):
    os.system("rm -rf {}".format(VIDEO_PATH))
    logging.info("Deleting Videos")
if os.path.exists(FRAME_PATH):
    os.system("rm -rf {}".format(FRAME_PATH))
    logging.info("Deleting Frames")

logging.info("Edge Process Ended")