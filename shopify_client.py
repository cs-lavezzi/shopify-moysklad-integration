import requests
import json
from config import SHOPIFY_SHOP, SHOPIFY_ACCESS_TOKEN

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
    
    def get_products(self, limit=10):
        """Get products from Shopify"""
        query = """
        query ($limit: Int!) {
            products(first: $limit) {
                edges {
                    node {
                        id
                        title
                        handle
                        vendor
                        productType
                        description
                        variants(first: 10) {
                            edges {
                                node {
                                    id
                                    price
                                    sku
                                    inventoryQuantity
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"limit": limit}
        response = requests.post(
            f"{self.base_url}/graphql.json",
            headers=self.headers,
            json={"query": query, "variables": variables}
        )
        return response.json()
    
    def get_orders(self, limit=10):
        """Get orders from Shopify"""
        query = """
        query ($limit: Int!) {
            orders(first: $limit) {
                edges {
                    node {
                        id
                        name
                        email
                        totalPrice
                        createdAt
                        lineItems(first: 10) {
                            edges {
                                node {
                                    title
                                    quantity
                                    variant {
                                        id
                                        sku
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"limit": limit}
        response = requests.post(
            f"{self.base_url}/graphql.json",
            headers=self.headers,
            json={"query": query, "variables": variables}
        )
        return response.json()