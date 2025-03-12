# Moysklad-Shopify Integration

Bu dastur Shopify va Moysklad platformalari o'rtasida integratsiyani ta'minlaydi.

## O'rnatish

```bash
# Repositoriyni klonlash
git clone https://github.com/cs-lavezzi/moysklad-shopify-integration.git
cd moysklad-shopify-integration

# Virtual muhitni yaratish
python -m venv venv
source venv/bin/activate  # Linux/Mac
# yoki
venv\Scripts\activate  # Windows

# Kerakli paketlarni o'rnatish
pip install -r requirements.txt

# .env faylini sozlash
cp .env.example .env
# .env faylini tahrirlang