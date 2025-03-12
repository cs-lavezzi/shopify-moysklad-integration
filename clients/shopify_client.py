import requests
import json
import time
from utils.logger import get_logger
from config import SHOPIFY_SHOP, SHOPIFY_ACCESS_TOKEN

logger = get_logger("shopify_client")

class ShopifyClient:
    def __init__(self):
        self.shop = SHOPIFY_SHOP
        self.access_token = SHOPIFY_ACCESS_TOKEN
        self.api_version = "2025-01"
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.access_token
        }
        self.base_url = f"https://{self.shop}/admin/api/{self.api_version}"
    
    def _execute_graphql(self, query, variables=None):
        """GraphQL so'rovni bajarish"""
        if variables is None:
            variables = {}
        
        response = requests.post(
            f"{self.base_url}/graphql.json",
            headers=self.headers,
            json={"query": query, "variables": variables}
        )
        
        if response.status_code != 200:
            logger.error(f"Shopify GraphQL error: {response.text}")
            response.raise_for_status()
        
        return response.json()
    
    def get_products(self, limit=50, cursor=None):
        """Shopify'dan mahsulotlarni olish"""
        query = """
        query ($limit: Int!, $cursor: String) {
            products(first: $limit, after: $cursor) {
                edges {
                    node {
                        id
                        title
                        handle
                        vendor
                        productType
                        description
                        images(first: 10) {
                            edges {
                                node {
                                    id
                                    src
                                }
                            }
                        }
                        variants(first: 20) {
                            edges {
                                node {
                                    id
                                    price
                                    sku
                                    barcode
                                    inventoryQuantity
                                    title
                                    inventoryItem {
                                        id
                                    }
                                    selectedOptions {
                                        name
                                        value
                                    }
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """
        variables = {"limit": limit}
        if cursor:
            variables["cursor"] = cursor
        
        return self._execute_graphql(query, variables)
    
    def get_product_by_sku(self, sku):
        """SKU orqali mahsulotni izlash"""
        query = """
        query ($query: String!) {
            products(first: 1, query: $query) {
                edges {
                    node {
                        id
                        title
                        handle
                        variants(first: 20) {
                            edges {
                                node {
                                    id
                                    sku
                                    inventoryItem {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"query": f"sku:{sku}"}
        result = self._execute_graphql(query, variables)
        
        products = result.get('data', {}).get('products', {}).get('edges', [])
        if products:
            return products[0]['node']
        return None
    
    def create_product(self, product_data):
        """Yangi mahsulot yaratish"""
        mutation = """
        mutation createProduct($input: ProductInput!) {
            productCreate(input: $input) {
                product {
                    id
                    title
                    handle
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        variables = {"input": product_data}
        return self._execute_graphql(mutation, variables)
    
    def create_product_variant(self, product_id, variant_data):
        """Mahsulotga yangi variant qo'shish"""
        mutation = """
        mutation createProductVariant($input: ProductVariantInput!) {
            productVariantCreate(input: $input) {
                productVariant {
                    id
                    title
                    sku
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        variables = {
            "input": {
                "productId": product_id,
                **variant_data
            }
        }
        return self._execute_graphql(mutation, variables)
    
    def update_product(self, product_id, product_data):
        """Mahsulot ma'lumotlarini yangilash"""
        mutation = """
        mutation updateProduct($input: ProductInput!) {
            productUpdate(input: $input) {
                product {
                    id
                    title
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        variables = {
            "input": {
                "id": product_id,
                **product_data
            }
        }
        return self._execute_graphql(mutation, variables)
    
    def update_inventory_level(self, inventory_item_id, location_id, quantity):
        """Inventarni yangilash"""
        mutation = """
        mutation updateInventoryLevel($input: InventoryAdjustQuantityInput!) {
            inventoryAdjustQuantity(input: $input) {
                inventoryLevel {
                    available
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        variables = {
            "input": {
                "inventoryItemId": inventory_item_id,
                "locationId": location_id,
                "availableDelta": quantity
            }
        }
        return self._execute_graphql(mutation, variables)
    
    def get_location_id(self):
        """Birinchi manzilni olish"""
        query = """
        query {
            locations(first: 1) {
                edges {
                    node {
                        id
                        name
                    }
                }
            }
        }
        """
        result = self._execute_graphql(query)
        locations = result.get('data', {}).get('locations', {}).get('edges', [])
        if locations:
            return locations[0]['node']['id']
        return None
    
    def get_orders(self, limit=50, processed=False, cursor=None):
        """Shopify'dan buyurtmalarni olish"""
        query = """
        query ($limit: Int!, $cursor: String, $query: String) {
            orders(first: $limit, after: $cursor, query: $query) {
                edges {
                    node {
                        id
                        name
                        email
                        phone
                        totalPrice
                        createdAt
                        shippingAddress {
                            address1
                            address2
                            city
                            country
                            firstName
                            lastName
                            phone
                            zip
                        }
                        lineItems(first: 20) {
                            edges {
                                node {
                                    title
                                    quantity
                                    variant {
                                        id
                                        sku
                                        price
                                    }
                                }
                            }
                        }
                        customer {
                            id
                            firstName
                            lastName
                            email
                            phone
                        }
                        tags
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """
        variables = {"limit": limit}
        if cursor:
            variables["cursor"] = cursor
        
        # Agar processed=False bo'lsa, faqat 'moysklad-synced' tegi yo'q buyurtmalarni olish
        if not processed:
            variables["query"] = "tag_not:moysklad-synced"
        
        return self._execute_graphql(query, variables)
    
    def add_tag_to_order(self, order_id, tag):
        """Buyurtmaga teg qo'shish"""
        # Avval buyurtmani olish
        query = """
        query ($id: ID!) {
            order(id: $id) {
                id
                tags
            }
        }
        """
        variables = {"id": order_id}
        result = self._execute_graphql(query, variables)
        
        current_tags = result.get('data', {}).get('order', {}).get('tags', [])
        
        # Yangi teg qo'shamiz
        if tag not in current_tags:
            current_tags.append(tag)
        
        # Buyurtmani yangilaymiz
        mutation = """
        mutation updateOrder($input: OrderInput!) {
            orderUpdate(input: $input) {
                order {
                    id
                    tags
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        variables = {
            "input": {
                "id": order_id,
                "tags": current_tags
            }
        }
        return self._execute_graphql(mutation, variables)