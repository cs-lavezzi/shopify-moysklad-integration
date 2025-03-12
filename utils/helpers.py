import re
import unicodedata

def slugify(value):
    """
    Parametr sifatida berilgan qiymatni slugga o'zgartiradi
    (Shopify handle yasash uchun)
    """
    value = str(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')

def normalize_sku(value):
    """SKU ni normalizatsiya qilish"""
    if not value:
        return ""
    return str(value).strip().upper()