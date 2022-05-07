import cv2
import os
choice = int(input('Enter Choice'))
video = str(input('Enter video path'))
if choice == 1 or choice == 3:
    vidcap = cv2.VideoCapture(video)
    success,image = vidcap.read()
    count = 1
    while success:
        cv2.imwrite("kaushik/%d.png" % count, image)     # save frame as JPEG file      
        success,image = vidcap.read()
        print('Read a new frame: ', success)
        count += 1
if choice == 2 or choice == 3:
    for file in os.listdir(os.path.join(os.path.dirname(__file__), 'kaushik')):
        img_path = os.path.join(os.path.join(os.path.dirname(__file__), 'kaushik'), file)
        print(img_path)
        image = cv2.imread(img_path)
        resizedImage = cv2.resize(image, (160, 160))
        rgbImg = cv2.cvtColor(resizedImage, cv2.COLOR_RGBA2RGB)
        cv2.imwrite(img_path, rgbImg)