import requests
from config import config
from user_agent import agents
import random
print(config)
headers = {
        'User-Agent': random.choice(agents)
}
