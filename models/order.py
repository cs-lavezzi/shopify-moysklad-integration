from datetime import datetime

class OrderMapper:
    @staticmethod
    def shopify_to_moysklad(shopify_order, moysklad_client):
        """Shopify buyurtmasini Moysklad formatiga o'zgartirish"""
        # Mijoz ma'lumotlari
        customer_data = None
        if shopify_order.get("customer"):
            customer = shopify_order["customer"]
            first_name = customer.get("firstName", "")
            last_name = customer.get("lastName", "")
            
            customer_data = {
                "name": f"{first_name} {last_name}".strip() or "Shopify Customer",
                "email": customer.get("email", ""),
                "phone": customer.get("phone", ""),
                "tags": ["Shopify"]
            }
        
        # Mijozni topish yoki yaratish
        if customer_data:
            customer = moysklad_client.get_or_create_customer(customer_data)
        else:
            # Standart mijoz
            customer_data = {"name": "Shopify Customer", "tags": ["Shopify"]}
            customer = moysklad_client.get_or_create_customer(customer_data)
        
        # Organizatsiya va sklad
        organization = moysklad_client.get_organization()
        store = moysklad_client.get_store()
        
        # Buyurtma ma'lumotlari
        order_data = {
            "name": shopify_order.get("name", ""),
            "externalCode": shopify_order.get("id", "").split("/")[-1],
            "moment": datetime.fromisoformat(shopify_order.get("createdAt", "")).strftime("%Y-%m-%d %H:%M:%S"),
            "organization": {
                "meta": organization["meta"] if organization else None
            },
            "agent": {
                "meta": customer["meta"]
            },
            "store": {
                "meta": store["meta"] if store else None
            },
            "state": {
                "name": "Новый"
            },
            "positions": [],
            "description": f"Заказ из Shopify {shopify_order.get('name', '')}",
            "attributes": [
                {
                    "name": "Shopify ID",
                    "value": shopify_order.get("id", "")
                }
            ]
        }
        
        # Manzil ma'lumotlari
        if shopify_order.get("shippingAddress"):
            address = shopify_order["shippingAddress"]
            address_str = ""
            if address.get("address1"):
                address_str += address["address1"] + " "
            if address.get("address2"):
                address_str += address["address2"] + " "
            if address.get("city"):
                address_str += address["city"] + " "
            if address.get("zip"):
                address_str += address["zip"] + " "
            if address.get("country"):
                address_str += address["country"]
            
            if address_str:
                order_data["shipmentAddress"] = address_str.strip()
        
        # Mahsulotlar
        if shopify_order.get("lineItems", {}).get("edges"):
            for item in shopify_order["lineItems"]["edges"]:
                line_item = item["node"]
                
                # Mahsulotni SKU orqali izlash
                product = None
                if line_item.get("variant", {}).get("sku"):
                    product = moysklad_client.get_product_by_sku(line_item["variant"]["sku"])
                
                if not product:
                    # Nomi orqali izlash
                    product_name = line_item.get("title", "")
                    # Bu oddiy logika, kerakli bo'lsa murakkablashtirish mumkin
                    continue
                
                # Buyurtma pozitsiyasini qo'shish
                position = {
                    "quantity": line_item.get("quantity", 1),
                    "price": int(float(line_item.get("variant", {}).get("price", 0)) * 100),  # Kopeykalarga o'tkazish
                    "assortment": {
                        "meta": product["meta"]
                    }
                }
                
                order_data["positions"].append(position)
        
        return order_data