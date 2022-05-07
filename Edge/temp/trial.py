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
    videoFile = "video-{}.h264".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f"))
    videoFilePath = os.path.join(VIDEO_PATH, videoFile)
    logger.info("Started Recording")
    camera = picamera.PiCamera()
    camera.resolution = (160,160)
    startTime = time.time()
    camera.start_recording(videoFilePath)    
    camera.wait_recording(duration)
    camera.stop_recording()
    videoMap[videoFile] = {
        'startTime': startTime
    }
    # logger.info("Stopped Recording")
    camera.close()
    queue.put(videoFilePath)
    logger.info("Recording End Time: " + str(datetime.now()))

def processVideo(videoFile, queue):
    logger.info("Processing Start Time: " + str(datetime.now()))
    imageFilePath = os.path.join(FRAME_PATH, videoFile.split("/")[-1].split(".")[0] + ".jpg")
    vidcap = cv2.VideoCapture(videoFile)
    success, image = vidcap.read()
    if success:
        cv2.imwrite(imageFilePath, image)
        queue.put(imageFilePath)
    logger.info("Processing End Time: " + str(datetime.now()))

def uploadFiles(fileType, filePath, queue):
    try:
        logger.info("Uploading Start Time: " + str(datetime.now()))
        bucketName = config['S3Section']['S3_Bucket_Name']
        if fileType == 0 and filePath != None:
            videoFileName = "Videos/" + filePath.split("/")[-1]
            s3Client.upload_file(Filename=filePath, Bucket=bucketName, Key=videoFileName)
            logger.info("Uploaded Video: " + filePath)
        elif fileType == 1 and filePath != None:
            frameFileName = "Frames/" + filePath.split("/")[-1]
            s3Client.upload_file(Filename=filePath, Bucket=bucketName, Key=frameFileName)
            logger.info("Uploaded Frame: " + filePath)
        if queue != None:
            queue.put(filePath)
        logger.info("Uploading End Time: " + str(datetime.now()))
    except ClientError as e:
        logger.error(e)
    except FileNotFoundError as e:
        logger.error(e)

def getLambdaResult(framePath, queue):
    logger.info("Lambda Receiving Start Time: " + str(datetime.now()))
    bucketName = str(config['S3Section']['S3_Bucket_Name'])
    lambdaUrl = str(config['LambdaSection']['LAMBDA_FUNCTION_URL'])
    imageName = framePath.split('/')[-1]
    logger.info("Image: " + imageName + ", Bucket: " + bucketName + ", Lambda Url: " + lambdaUrl)
    payload = {
        "ImageName": str(imageName),
        "BucketName": str(bucketName)
    }
    headers = { "Content-Type": "application/json" }
    response = requests.post(url=lambdaUrl, headers=headers, json=payload)
    endTime = time.time()
    output = json.loads(response.text)
    resultImage = output['Image_Name']
    videoName = resultImage.replace('.jpg', '.h264')
    if videoName in videoMap:
        videoMap[videoName]['endTime'] = endTime
    queue.put((output, current_thread()))
    logger.info("Lambda Receiving End Time: " + str(datetime.now()) + ", Image: " + resultImage)

def updateProcessStatus(count, type):
    if count > 0:
        threadStatusMap[count-1][type] = False
    else:
        threadStatusMap[5][type] = False
    count += 1
    count %= 6
    return count

recordResult, processResult, uploadResult, lambdaResult = deque(), deque(), deque(), deque()
recordCount, processCount, uploadVideoCount, uploadFrameCount, lambdaCount = 0, 0, 0, 0, 0
count = 0

logger.info("Edge Process Started")
flag = False

lambdaQueue = Queue()
lambdaThreadList = deque()
lambdaThreadStatus = False

if processTime == "inf":
    processTimeout = time.time() + 9223372036854775807
else:
    processTimeout = time.time() + recordTime

changeFlag = True

videoList = []

while True:
    if changeFlag:
        logger.info("-----------------------------------------------------------------------------------------------------------")
        logger.info("Iteration #" + str(count))
    changeFlag = False
    recordThread, processThread, uploadVideoThread, uploadFrameThread = None, None, None, None

    if not flag:
        recordQueue = Queue()
        recordThread = Thread(target=recordVideo, args=(0.5, recordQueue))

    if recordResult:
        processQueue = Queue()
        filePath = recordResult.popleft()
        processThread = Thread(target=processVideo, args=(filePath, processQueue))
        videoList.append(filePath)
        # uploadVideoQueue = Queue()
        # uploadVideoThread = Thread(target=uploadFiles, args=(0, filePath, uploadVideoQueue))

    if processResult:
        # logger.info("Record Result Block")
        uploadFrameQueue = Queue()
        filePath = processResult.popleft()
        uploadFrameThread = Thread(target=uploadFiles, args=(1, filePath, uploadFrameQueue))

    if uploadResult:
        # logger.info("Upload Result Block")
        framePath = uploadResult.popleft()
        logger.info("Lambda results for: " + framePath)
        lambdaThreadList.append(Thread(target=getLambdaResult, args=(framePath, lambdaQueue)))

    if recordThread != None:
        recordThread.start()

    if processThread != None:
        processThread.start()

    # if uploadVideoThread != None:
    #     uploadVideoThread.start()
    
    if uploadFrameThread != None:
        uploadFrameThread.start()

    if lambdaThreadList:
        lambdaThreadList.popleft().start()

    if recordThread != None:
        # logger.info("Record Queue Block")
        recordThread.join()
        filePath = recordQueue.get()
        recordResult.append(filePath)
        recordCount += 1
        logger.info("Record Count: " + str(recordCount) + ", Video: " + filePath)
        changeFlag = True

    if processThread != None:
        processThread.join()
        framePath = processQueue.get()
        processResult.append(framePath)
        processCount += 1
        logger.info("Process Count: " + str(processCount) + ", Processed Frame: " + framePath)

    # if uploadVideoThread != None:
    #     # logger.info("Upload Queue Block")
    #     uploadVideoThread.join()
    #     videoPath = uploadVideoQueue.get()
    #     uploadVideoCount += 1
    #     logger.info("Upload Count: " + str(uploadVideoCount) + ", Uploaded Video: " + videoPath)
    #     changeFlag = True

    if uploadFrameThread != None:
        # logger.info("Upload Queue Block")
        uploadFrameThread.join()
        framePath = uploadFrameQueue.get()
        uploadResult.append(framePath)
        uploadFrameCount += 1
        logger.info("Upload Count: " + str(uploadFrameCount) + ", Uploaded Video: " + framePath)
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
        videoName = imageName.replace('.jpg', '.h264')
        if videoName in videoMap:
            latency = videoMap[videoName]['endTime'] - videoMap[videoName]['startTime']
            logger.info("Lambda Count: " + str(lambdaCount) + ", Frame: " + imageName)
            resultLogger.info("Image: " + imageName + ", Result: " + str(output) + ", Latency: {:.2f} seconds.".format(latency))

    if changeFlag:
        count += 1
    if time.time() > processTimeout:
        flag = True
    if flag and lambdaCount == recordCount:
        break

logger.info("Frames Count: %d, Upload Count %d, Lambda Count: %d", recordCount, uploadFrameCount, lambdaCount)
logger.info(videoMap)

for video in videoList:
    uploadFiles(0, video, None)

if os.path.exists(VIDEO_PATH):
    os.system("rm -rf {}".format(VIDEO_PATH))
    logger.info("Deleting Videos")
if os.path.exists(FRAME_PATH):
    os.system("rm -rf {}".format(FRAME_PATH))
    logger.info("Deleting Frames")

logger.info("Edge Process Ended")