
import requests
import os
import base64
import io
from PIL import Image

def image_to_base64(image_path, target_size=(960, 540), img_format='JPEG'):
    # 读取图像并转换为RGB模式
    image = Image.open(image_path).convert('RGB')
    # 调整图像大小
    resized_image = image.resize(target_size)
    # 将图像保存到内存中的字节流
    byte_stream = io.BytesIO()
    resized_image.save(byte_stream, format=img_format)
    byte_data = byte_stream.getvalue()
    # 生成Base64编码并解码为字符串
    base64_str = base64.b64encode(byte_data).decode('utf-8')

    return base64_str

def VLM_infer(image_path, system_prompt, word):

    encoded_image_text = image_to_base64(image_path)
    base64_qwen = f"data:image/jpeg;base64,{encoded_image_text}"

    url = "https://dev-alignment-hdmap.evad.mioffice.cn"
    proUrl = url + "/model-base64"
    print(proUrl)
    #Headers = {"X-Platform-Type": "1"}
    params = {
        "image": [
            base64_qwen
        ],
        "system_prompt": system_prompt,
        "word": word
    }
    #print(params)
    #response = requests.post(url=proUrl, headers=Headers, json=params)
    response = requests.post(url=proUrl, json=params)
    if 200 <= response.status_code < 300:
        resp_data = response.json()
        if resp_data["code"] == 200:
            #print(resp_data.keys())
            #print(resp_data['message'])
            return resp_data['message']
        else:
            raise Exception("Get hdmap data failed. status_code: {} code: {}".format(
                response.status_code, resp_data["code"]))
    else:
        print("no data error")



if __name__ == '__main__':
    image_path = "/Users/chentong/Documents/cdwcw.png"
    #result = VLM_infer(image_path,  "你是一个司机", "是否施工")   #自定义提示词
    result = VLM_infer(image_path,  "你是一个图像识别人员", "请仔细分析图像，按照以下标准执行，执行结果输出超出或未超出。识别图像中红色线是否在白色车道线上，没有偏离出白色车道线，偏离出白色车道线输出结果为超出，")   #默认提示词是预置了施工复杂场景提示词
    print(result)      #返回 VLM 判断结果的  字符串






