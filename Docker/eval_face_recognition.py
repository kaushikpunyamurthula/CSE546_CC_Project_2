import torch
import torchvision.transforms as transforms
from PIL import Image
import json
import numpy as np
import argparse
import build_custom_model
import os

def evaluate(img_path):
     labels_dir = os.path.join(os.path.dirname(__file__), "checkpoint", "labels.json")
     model_path = os.path.join(os.path.dirname(__file__), "checkpoint", "model_vggface2_best.pth")
     # labels_dir = "/tmp/torch/checkpoint/labels.json"
     # model_path = "./checkpoint/model_vggface2_best.pth"

     # read labels
     with open(labels_dir) as f:
          labels = json.load(f)

     device = torch.device('cpu')
     model = build_custom_model.build_model(len(labels)).to(device)
     model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu'))['model'])
     model.eval()


     img = Image.open(img_path)
     img_tensor = transforms.ToTensor()(img).unsqueeze_(0).to(device)
     outputs = model(img_tensor)
     _, predicted = torch.max(outputs.data, 1)
     result = labels[np.array(predicted.cpu())[0]]


     img_name = img_path.split("/")[-1]
     img_and_result = {
          "image" : img_name,
          "result" : result
     }
     return img_and_result

if __name__ == "__main__":

     parser = argparse.ArgumentParser(description='Evaluate your customized face recognition model')
     parser.add_argument('--img_path', type=str, default="./data/test_me/val/angelina_jolie/1.png", help='the path of the dataset')
     args = parser.parse_args()
     img_path = args.img_path
     result = evaluate(img_path)
     print(result)