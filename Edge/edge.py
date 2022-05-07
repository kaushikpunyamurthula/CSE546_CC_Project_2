 #!/usr/bin/python3
from collections import deque
from datetime import datetime
from threading import Thread, current_thread
from multiprocessing import Process
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

def setupLogger(loggerName, logFile, level=logging.INFO):
    logger = logging.getLogger(loggerName)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s')
    fileHandler = logging.FileHandler(logFile, mode='w')
    fileHandler.setFormatter(formatter)
    # streamHandler = logging.StreamHandler()
    # streamHandler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(fileHandler)
    # logger.addHandler(streamHandler) 
    logger.propagate = False

    return logger

logger = setupLogger(loggerName="logger", logFile=os.path.join(os.path.dirname(__file__), 'edge.log'))
resultLogger = setupLogger(loggerName="resultLogger", logFile=os.path.join(os.path.dirname(__file__), 'result.log'))

resource_region = config['AWSSection']['Region']
# s3 = boto3.resource('s3', region_name = resource_region)
s3Client = boto3.client('s3', region_name = resource_region)
sqs = boto3.client('sqs', region_name = resource_region)

processTime = sys.argv[1]
recordTime = float(processTime) * 60

if processTime == "inf":
    processTimeout = time.time() + 9223372036854775807
else:
    processTimeout = time.time() + recordTime

threadStatusMap = [{
    "record" : False,
    "process" : False,
    "upload" : False,
    "lambda" : False
} for _ in range(6)]

logger.info(threadStatusMap)

videoMap = {}

VIDEO_PATH = os.path.join(os.path.dirname(__file__),'Videos')
FRAME_PATH = os.path.join(os.path.dirname(__file__),'Frames')

if not os.path.exists(VIDEO_PATH):
    os.makedirs(VIDEO_PATH)

if not os.path.exists(FRAME_PATH):
    os.makedirs(FRAME_PATH)

def recordVideo(duration, queue):
    logger.info("Recording Start Time: " + str(datetime.now()))
    videoFile = os.path.join(VIDEO_PATH, "video.h264")
    logger.info("Started Recording")
    camera = picamera.PiCamera()
    camera.resolution = (160,160)
    recordStartTime = time.time()
    camera.start_recording(videoFile)
    startTime = time.time()
    while startTime < recordStartTime + duration:
        startDateTime = datetime.now()
        frameFile = 'frame-{}.jpg'.format(startDateTime)
        frameFilePath = os.path.join(FRAME_PATH, frameFile)
        videoMap[frameFile] = {
            'startTime': startTime
        }
        logger.info("Processing Start Time: " + str(startDateTime) + ", Frame: " + frameFile)
        camera.capture(frameFilePath, use_video_port=True)
        camera.wait_recording(0.5)
        queue.put((0, frameFilePath))
        logger.info("Processing End Time: " + str(datetime.now()))
        startTime = time.time()
    camera.stop_recording()
    logger.info("Stopped Recording")
    camera.close()
    queue.put((1, videoFile))
    logger.info("Recording End Time: " + str(datetime.now()))

def uploadFiles(queue, fileType, filePath):
    try:
        logger.info("Uploading Start Time: " + str(datetime.now()))
        bucketName = config['S3Section']['S3_Bucket_Name']
        if fileType == "video" and filePath != None:
            videoFileName = "Videos/" + filePath.split("/")[-1]
            s3Client.upload_file(Filename=filePath, Bucket=bucketName, Key=videoFileName)
            logger.info("Uploaded Video: " + filePath)
        elif fileType == "frame" and filePath != None:
            frameFileName = "Frames/" + filePath.split("/")[-1]
            s3Client.upload_file(Filename=filePath, Bucket=bucketName, Key=frameFileName)
            logger.info("Uploaded Frame: " + filePath)
        queue.put(filePath)
        logger.info("Uploading End Time: " + str(datetime.now()))
    except ClientError as e:
        logger.error(e)
    except FileNotFoundError as e:
        logger.error(e)

def getFaceRecognitionResult(queue, framePath):
    logger.info("Lambda Receiving Start Time: " + str(datetime.now()))
    bucketName = str(config['S3Section']['S3_Bucket_Name'])
    lambdaUrl = str(config['LambdaSection']['LAMBDA_FUNCTION_URL'])
    framePath = framePath.split('/')[-1]
    logger.info("Image: " + framePath + ", Bucket: " + bucketName + ", Lambda Url: " + lambdaUrl)
    payload = {
        "ImageName": str(framePath),
        "BucketName": str(bucketName)
    }
    headers = { "Content-Type": "application/json" }
    response = requests.post(url=lambdaUrl, headers=headers, json=payload)
    output = json.loads(response.text)
    queue.put((output, current_thread()))
    logger.info("Lambda Receiving End Time: " + str(datetime.now()))

def updateProcessStatus(count, type):
    if count > 0:
        threadStatusMap[count-1][type] = False
    else:
        threadStatusMap[5][type] = False
    count += 1
    count %= 6
    return count

recordResult, uploadResult, lambdaResult = deque(), deque(), deque()
recordCount, uploadCount, lambdaCount = 0, 0, 0
count = 0

logger.info("Edge Process Started")
flag = False

lambdaQueue = Queue()
lambdaThreadList = deque()
lambdaThreadStatus = False

recordQueue = Queue()
recordThread = Thread(target=recordVideo, args=(recordTime, recordQueue))
recordThread.start()

videoPath = None

changeFlag = True

while True:
    if changeFlag:
        logger.info("-----------------------------------------------------------------------------------------------------------")
        logger.info("Iteration #" + str(count))
    changeFlag = False
    uploadThread = None       

    if recordResult:
        # logger.info("Record Result Block")
        uploadQueue = Queue()
        filePath = recordResult.popleft()
        uploadThread = Thread(target=uploadFiles, args=(uploadQueue, "frame", filePath))

    if uploadResult:
        # logger.info("Upload Result Block")
        framePath = uploadResult.popleft()
        logger.info("Lambda results for: " + framePath)
        lambdaThreadList.append(Thread(target=getFaceRecognitionResult, args=(lambdaQueue, framePath)))

    if uploadThread != None:
        uploadThread.start()

    if lambdaThreadList:
        lambdaThreadList.popleft().start()

    if not recordQueue.empty():
        # logger.info("Record Queue Block")
        fileType, filePath = recordQueue.get()
        if fileType == 0:
            recordResult.append(filePath)
            recordCount += 1
            logger.info("Record Count: " + str(recordCount) + ", Frame: " + filePath)
            changeFlag = True
        else:
            videoPath = filePath
            recordThread.join()

    if uploadThread != None:
        # logger.info("Upload Queue Block")
        uploadThread.join()
        framePath = uploadQueue.get()
        uploadResult.append(framePath)
        uploadCount += 1
        logger.info("Upload Count: " + str(uploadCount) + ", Uploaded Frame: " + framePath)
        changeFlag = True

    if not lambdaQueue.empty():
        # logger.info("Lambda Queue Block")
        result, thread = lambdaQueue.get()
        lambdaResult.append(result)
        thread.join()
        output = lambdaResult.popleft()
        imageName = output['Image_Name']
        changeFlag = True
        lambdaCount += 1
        if imageName in videoMap:
            endTime = time.time()
            videoMap[imageName]['endTime'] = endTime
            latency = endTime - videoMap[imageName]['startTime']
            logger.info("Lambda Count: " + str(lambdaCount) + ", Frame: " + imageName)
            # logger.info("Image: " + imageName + ", Result: " + str(output) + ", Latency: {:.2f} seconds.".format(latency))
            resultLogger.info("Image: " + imageName + ", Result: " + str(output) + ", Latency: {:.2f} seconds.".format(latency))

    if changeFlag:
        count += 1
    if videoPath != None and lambdaCount == recordCount:
        break

logger.info("Frames Count: %d, Upload Count: %d, Lambda Count: %d", recordCount, uploadCount, lambdaCount)
logger.info(videoMap)

if videoPath != None:
    queue = Queue()
    uploadFiles(queue, "video", videoPath)
    logger.info("Uploaded Video: " + queue.get() + " to S3")

if os.path.exists(VIDEO_PATH):
    os.system("rm -rf {}".format(VIDEO_PATH))
    logger.info("Deleting Videos")
if os.path.exists(FRAME_PATH):
    os.system("rm -rf {}".format(FRAME_PATH))
    logger.info("Deleting Frames")

logger.info("Edge Process Ended")