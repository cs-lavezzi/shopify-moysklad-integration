import requests
import base64
from config import MOYSKLAD_LOGIN, MOYSKLAD_PASSWORD, MOYSKLAD_API_TOKEN

class MoyskladClient:
    def __init__(self):
        self.base_url = "https://online.moysklad.ru/api/remap/1.2"
        
        # Token orqali autentifikatsiya
        if hasattr(self, 'api_token') and MOYSKLAD_API_TOKEN:
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
    
    def get_assortment(self, limit=100):
        """Get products from Moysklad"""
        response = requests.get(
            f"{self.base_url}/entity/assortment",
            headers=self.headers,
            params={"limit": limit}
        )
        return response.json()
    
    def create_product(self, product_data):
        """Create a product in Moysklad"""
        response = requests.post(
            f"{self.base_url}/entity/product",
            headers=self.headers,
            json=product_data
        )
        return response.json()
    
    def create_order(self, order_data):
        """Create an order in Moysklad"""
        response = requests.post(
            f"{self.base_url}/entity/customerorder",
            headers=self.headers,
            json=order_data
        )
        return response.json()