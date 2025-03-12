import os
from dotenv import load_dotenv

# .env faylidan konfiguratsiyalarni o'qiymiz
load_dotenv()

# Shopify konfiguratsiyasi
SHOPIFY_SHOP = os.getenv('SHOPIFY_SHOP')
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_SECRET = os.getenv('SHOPIFY_API_SECRET')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')

# Moysklad konfiguratsiyasi
MOYSKLAD_LOGIN = os.getenv('MOYSKLAD_LOGIN')
MOYSKLAD_PASSWORD = os.getenv('MOYSKLAD_PASSWORD')
MOYSKLAD_API_TOKEN = os.getenv('MOYSKLAD_API_TOKEN')