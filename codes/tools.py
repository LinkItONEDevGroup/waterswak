# @file tools.py
# @brief outside tool
# @author wuulong@gmail.com
#standard
import requests
import json
#extend

#library

def rivercode_search(pos): # ex: pos=[120.6666785,24.1755573]
    #document: https://hackmd.io/@d9OQzm9mRx2Q-m9bpq7QDQ/SkPm0qQyY
    url = 'https://vps.pointing.tw/rivercode/search'
    headers = {'accept':'*/*','content-type': 'application/json'}
    #print("pos=%s" %(pos))
    r = requests.post(url, data=json.dumps(pos), headers=headers)
    #print(r.text)
    if r.text=='':
        return None
    else:
        data = json.loads(r.text)
        return data
