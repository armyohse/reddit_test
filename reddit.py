import requests

headers = {"Authorization": "bearer bGJFfk-SMJ9RTeiL_MfI8U_XLDc", "User-Agent": "ChangeMeClient/0.1 by YourUsername"}
params = {"t": "day"}
response = requests.get("https://oauth.reddit.com/r/python/top", headers=headers, params=params)

python_top = response.json()
print(python_top)
