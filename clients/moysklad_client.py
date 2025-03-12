import requests
import base64
import json
import time
from utils.logger import get_logger
from config import MOYSKLAD_LOGIN, MOYSKLAD_PASSWORD, MOYSKLAD_API_TOKEN

logger = get_logger("moysklad_client")

class MoyskladClient:
    def __init__(self):
        self.base_url = "https://online.moysklad.ru/api/remap/1.2"
        
        # Token orqali autentifikatsiya
        if MOYSKLAD_API_TOKEN:
            self.headers = {
                'Authorization': f'Bearer {MOYSKLAD_API_TOKEN}',
                'Content-Type': 'application/json'
            }
        # Login/password orqali autentifikatsiya
        else:
            credentials = f"{MOYSKLAD_LOGIN}:{MOYSKLAD_PASSWORD}"
            auth_header = base64.b64encode(credentials.encode()).decode()
            self.headers = {
                'Authorization': f'Basic {auth_header}',
                'Content-Type': 'application/json'
            }
    
    def _handle_rate_limit(self, response):
        """Rate limit xatoliklarini qayta ishlash"""
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds.")
            time.sleep(retry_after)
            return True
        return False
    
    def _make_request(self, method, endpoint, params=None, data=None, max_retries=3):
        """API so'rovlarini yuborish"""
        url = f"{self.base_url}/{endpoint}"
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if method == 'GET':
                    response = requests.get(url, headers=self.headers, params=params)
                elif method == 'POST':
                    response = requests.post(url, headers=self.headers, json=data)
                elif method == 'PUT':
                    response = requests.put(url, headers=self.headers, json=data)
                elif method == 'DELETE':
                    response = requests.delete(url, headers=self.headers)
                
                # Rate limit tekshirish
                if self._handle_rate_limit(response):
                    retry_count += 1
                    continue
                
                if response.status_code >= 400:
                    logger.error(f"Moysklad API error: {response.status_code} {response.text}")
                    response.raise_for_status()
                
                return response.json()
            
            except Exception as e:
                logger.error(f"Error making request to {endpoint}: {str(e)}")
                retry_count += 1
                if retry_count >= max_retries:
                    raise
                time.sleep(2 ** retry_count)  # Eksponentli kutish vaqti
    
    def get_all_products(self, limit=100, offset=0):
        """Barcha mahsulotlarni olish"""
        endpoint = "entity/product"
        params = {
            "limit": limit,
            "offset": offset,
            "expand": "images,uom"
        }
        return self._make_request('GET', endpoint, params=params)
    
    def get_product_by_id(self, product_id):
        """ID orqali mahsulotni olish"""
        endpoint = f"entity/product/{product_id}"
        params = {"expand": "images,uom"}
        return self._make_request('GET', endpoint, params=params)
    
    def get_product_by_sku(self, sku):
        """SKU (artikul) orqali mahsulotni izlash"""
        endpoint = "entity/product"
        params = {
            "filter": f"article={sku}",
            "expand": "images,uom"
        }
        result = self._make_request('GET', endpoint, params=params)
        
        if result.get('rows'):
            return result['rows'][0]
        return None
    
    def get_stock(self, product_id=None):
        """Mahsulot(lar) uchun sklad qoldig'ini olish"""
        endpoint = "report/stock/all"
        params = {}
        
        if product_id:
            params["filter"] = f"product.id={product_id}"
        
        return self._make_request('GET', endpoint, params=params)
    
    def create_product(self, product_data):
        """Yangi mahsulot yaratish"""
        endpoint = "entity/product"
        return self._make_request('POST', endpoint, data=product_data)
    
    def update_product(self, product_id, product_data):
        """Mahsulotni yangilash"""
        endpoint = f"entity/product/{product_id}"
        return self._make_request('PUT', endpoint, data=product_data)
    
    def upload_image(self, image_url):
        """Rasmni yuklash"""
        try:
            # Rasmni URL orqali yuklab olish
            image_response = requests.get(image_url)
            if image_response.status_code != 200:
                logger.error(f"Failed to download image from {image_url}")
                return None
            
            # Rasm uchun fayl nomi
            filename = image_url.split('/')[-1]
            
            # Moysklad API ga yuklash
            upload_url = f"{self.base_url}/entity/product/metadata/images/download"
            headers = self.headers.copy()
            headers.pop('Content-Type', None)  # Content-Type headerini o'chiramiz
            
            files = {'file': (filename, image_response.content)}
            response = requests.post(upload_url, headers=headers, files=files)
            
            if response.status_code != 200:
                logger.error(f"Failed to upload image to Moysklad: {response.text}")
                return None
            
            return response.json()
        
        except Exception as e:
            logger.error(f"Error uploading image: {str(e)}")
            return None
    
    def create_customer_order(self, order_data):
        """Yangi buyurtma yaratish"""
        endpoint = "entity/customerorder"
        return self._make_request('POST', endpoint, data=order_data)
    
    def get_or_create_customer(self, customer_data):
        """Mijozni topish yoki yaratish"""
        # Avval email orqali izlaymiz
        endpoint = "entity/counterparty"
        email = customer_data.get("email")
        name = customer_data.get("name")
        
        if email:
            params = {"filter": f"email={email}"}
            result = self._make_request('GET', endpoint, params=params)
            
            if result.get('rows'):
                return result['rows'][0]
        
        # Nomi orqali izlaymiz
        if name:
            params = {"filter": f"name={name}"}
            result = self._make_request('GET', endpoint, params=params)
            
            if result.get('rows'):
                return result['rows'][0]
        
        # Mijoz topilmadi, yangi yaratamiz
        return self._make_request('POST', endpoint, data=customer_data)
    
    def get_all_variants(self, product_id):
        """Mahsulotning barcha variantlarini olish"""
        endpoint = f"entity/variant"
        params = {
            "filter": f"product.id={product_id}",
            "expand": "characteristics"
        }
        return self._make_request('GET', endpoint, params=params)
    
    def get_organization(self):
        """Tashkilot ma'lumotlarini olish"""
        endpoint = "entity/organization"
        result = self._make_request('GET', endpoint)
        
        if result.get('rows'):
            return result['rows'][0]
        return None
    
    def get_store(self):
        """Sklad ma'lumotlarini olish"""
        endpoint = "entity/store"
        result = self._make_request('GET', endpoint)
        
        if result.get('rows'):
            return result['rows'][0]
        return None
    
    def get_currency_by_code(self, code="USD"):
        """Valyutani kodi orqali topish"""
        endpoint = "entity/currency"
        params = {"filter": f"code={code}"}
        result = self._make_request('GET', endpoint, params=params)
        
        if result.get('rows'):
            return result['rows'][0]
        return None
    
    def get_uom(self):
        """O'lchov birliklarini olish"""
        endpoint = "entity/uom"
        result = self._make_request('GET', endpoint)
        
        if result.get('rows'):
            # Standart o'lchov birligi (dona)
            for uom in result['rows']:
                if uom.get("name") == "шт":
                    return uom
            # Agar topilmasa, birinchisini qaytaramiz
            return result['rows'][0]
        return None