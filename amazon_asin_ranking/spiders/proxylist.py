import random
proxy_username = "a"
proxy_password = "a"

proxies = [
    '173.208.36.175:3128',
    '108.186.244.109:3128',
    '108.186.244.189:3128',
    '173.234.250.48:3128',
    '173.208.36.7:3128',
    '173.234.181.135:3128',
    '173.234.250.35:3128',
    '173.234.250.59:3128',
    '89.32.66.18:3128',
    '89.32.66.160:3128',
    '89.32.66.36:3128',
    '173.234.181.92:3128',
    '104.140.210.7:3128',
    '104.140.210.112:3128',
    '108.186.244.188:3128',
    '104.140.210.65:3128',
    '173.234.181.157:3128',
    '104.140.210.25:3128',
    '173.234.250.157:3128',
    '89.32.66.101:3128',
    '108.186.244.54:3128',
    '173.234.181.188:3128',
    '173.208.36.120:3128',
    '173.208.36.245:3128',
    '108.186.244.197:3128'
]


def get_proxy():
    return random.choice(proxies)