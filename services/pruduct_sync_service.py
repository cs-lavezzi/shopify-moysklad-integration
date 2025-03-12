from shopify_client import ShopifyClient
from moysklad_client import MoyskladClient

class SyncService:
    def __init__(self):
        self.shopify = ShopifyClient()
        self.moysklad = MoyskladClient()
    
    def sync_products_to_moysklad(self):
        """Sync products from Shopify to Moysklad"""
        # Shopify'dan mahsulotlarni olish
        shopify_products = self.shopify.get_products(limit=50)
        
        # Har bir mahsulot uchun
        for product_edge in shopify_products.get('data', {}).get('products', {}).get('edges', []):
            product = product_edge.get('node', {})
            
            # Moysklad uchun mahsulot ma'lumotlarini tayyorlash
            moysklad_product = {
                "name": product.get('title'),
                "description": product.get('description'),
                "article": product.get('handle'),
                "externalCode": product.get('id').split('/')[-1],
                "vat": 20,
                "vatEnabled": True,
                "uom": {
                    "meta": {
                        "href": "https://online.moysklad.ru/api/remap/1.2/entity/uom/19f1edc0-fc42-4001-94cb-c9ec9c62ec10",
                        "type": "uom"
                    }
                },
                "minPrice": {
                    "value": 0,
                    "currency": {
                        "meta": {
                            "href": "https://online.moysklad.ru/api/remap/1.2/entity/currency/b942e6f2-9128-11e6-8a84-bae500000058",
                            "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/currency/metadata",
                            "type": "currency"
                        }
                    }
                }
            }
            
            # Moysklad'ga mahsulotni qo'shish
            result = self.moysklad.create_product(moysklad_product)
            print(f"Synced product: {product.get('title')}, result: {result.get('id', 'error')}")
    
    def sync_orders_to_moysklad(self):
        """Sync orders from Shopify to Moysklad"""
        # TODO: Implement order sync
        pass