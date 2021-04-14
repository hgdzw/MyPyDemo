# 手机号注册的网站

from fake_useragent import UserAgent


class PhoneSite:

    def __init__(self, phone):
        self.phone = phone

    def getUA(self):
        return UserAgent().random


p = PhoneSite("131")
print(p.getUA())
