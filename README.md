# CSE546_CC_Project_2 

Group 37
Sai Surya Kaushik Punyamurthula - 1220096111
Krisha Vijay Gala - 1222514124
Mrunalini Jitendra Dighe - 1219515557

Steps to compile and run the code -
1) The first important step is to  configure and setup the Raspberry Pi 
Reference document- https://docs.google.com/document/d/1MaHuP5qyA29oy2vhYwPEdCB6vR5qJtOu5nQp2tVIJaM/edit
This is reference document provided by the professor which we have used as the baseline.
step1 - Download the Raspbian Os and flash it using Microsd card provided and mount it onto the Raspberry pi
This will setup and boot the Pi.Thenafter using monitor and keyboard setup the Raspberry Pi. Enable the wifi setup on it by 
putting in the required network credentials. You can also choose to enable the SSH on the Pi.
Also as we need to do facial recognition, we need to mount the camera module onto the Pi.This is used for capturing the images and the videos.
2)Store this files on the local system of the Pi-
https://github.com/sskp-kaushik/CSE546_CC_Project_2/tree/main/Edge
3)When you are confident that the Pi is connected to the internet, open the command line and run the command 
"pip install -r requirements.txt" or you can choose to manually install the packages from 'requirements.txt'
4)Now run the python file on Pi using the command "python3 edge.py 5 &"
5)We have configured logging in our rasberry pi scripts so the results will be displayed in 'result.log' file generated in the same directory and the intermediatory logs will be saved in the 'edge.log' file.

Links to AWS Resources:
Lambda Function URL: https://643yzh5h2cknqi7i2mxv3td2iy0kbwcj.lambda-url.us-east-1.on.aws/
S3 Bucket Link: http://cc-2-37-bucket.s3-website-us-east-1.amazonaws.com
