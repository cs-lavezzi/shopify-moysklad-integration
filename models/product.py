class ProductMapper:
    @staticmethod
    def moysklad_to_shopify(moysklad_product, moysklad_stock=None, moysklad_client=None):
        """Moysklad mahsulotini Shopify formatiga o'zgartirish"""
        shopify_product = {
            "title": moysklad_product.get("name", ""),
            "bodyHtml": moysklad_product.get("description", ""),
            "vendor": moysklad_product.get("supplier", {}).get("name", ""),
            "productType": moysklad_product.get("productFolder", {}).get("name", ""),
            "handle": moysklad_product.get("code", ""),
            "variants": []
        }
        
        # Mahsulot variantlarini olish
        if moysklad_client:
            variants = moysklad_client.get_all_variants(moysklad_product["id"])
            
            if variants and variants.get("rows"):
                # Har bir variant uchun
                for variant in variants.get("rows", []):
                    shopify_variant = {
                        "sku": variant.get("code", moysklad_product.get("article", "")),
                        "price": str(variant.get("salePrices", [{}])[0].get("value", 0) / 100),  # Kopeykalardi Shopify uchun konvert qilish
                        "barcode": variant.get("barcodes", [None])[0],
                        "option1": variant.get("characteristics", [{}])[0].get("name", "Default"),
                        "option2": variant.get("characteristics", [{}])[1].get("name", "") if len(variant.get("characteristics", [])) > 1 else None,
                        "option3": variant.get("characteristics", [{}])[2].get("name", "") if len(variant.get("characteristics", [])) > 2 else None,
                    }
                    
                    # Inventar miqdori
                    if moysklad_stock:
                        for stock_item in moysklad_stock.get("rows", []):
                            if stock_item.get("assortment", {}).get("id") == variant.get("id"):
                                shopify_variant["inventoryQuantity"] = stock_item.get("stock", 0)
                                break
                    
                    shopify_product["variants"].append(shopify_variant)
            else:
                # Asosiy mahsulot (variant yo'q)
                shopify_variant = {
                    "sku": moysklad_product.get("article", ""),
                    "price": str(moysklad_product.get("salePrices", [{}])[0].get("value", 0) / 100),
                    "barcode": moysklad_product.get("barcodes", [None])[0],
                    "option1": "Default",
                }
                
                # Inventar miqdori
                if moysklad_stock:
                    for stock_item in moysklad_stock.get("rows", []):
                        if stock_item.get("assortment", {}).get("id") == moysklad_product.get("id"):
                            shopify_variant["inventoryQuantity"] = stock_item.get("stock", 0)
                            break
                
                shopify_product["variants"].append(shopify_variant)
        
        # Rasmlarni qo'shish
        shopify_product["images"] = []
        
        if moysklad_product.get("images") and moysklad_product["images"].get("rows"):
            for image in moysklad_product["images"]["rows"]:
                shopify_product["images"].append({
                    "src": image.get("miniature", {}).get("href")
                })
        
        return shopify_product
    
    @staticmethod
    def shopify_to_moysklad(shopify_product, moysklad_client=None, uom=None):
        """Shopify mahsulotini Moysklad formatiga o'zgartirish"""
        title = shopify_product.get("title", "")
        article = ""
        
        # SKU ni birinchi variantdan olish
        variants = shopify_product.get("variants", {}).get("edges", [])
        if variants:
            article = variants[0]["node"].get("sku", "")
        
        moysklad_product = {
            "name": title,
            "description": shopify_product.get("description", ""),
            "article": article,
            "code": shopify_product.get("handle", ""),
            "externalCode": shopify_product.get("id", "").split("/")[-1]
        }
        
        # O'lchov birligini qo'shish
        if uom:
            moysklad_product["uom"] = {
                "meta": {
                    "href": uom.get("meta", {}).get("href"),
                    "metadataHref": uom.get("meta", {}).get("metadataHref"),
                    "type": uom.get("meta", {}).get("type"),
                    "mediaType": uom.get("meta", {}).get("mediaType")
                }
            }
        
        return moysklad_product