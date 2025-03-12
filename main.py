from sync_service import SyncService

def main():
    sync_service = SyncService()
    
    # Mahsulotlarni sinxronlashtirish
    print("Syncing products...")
    sync_service.sync_products_to_moysklad()
    
    # Buyurtmalarni sinxronlashtirish
    # print("Syncing orders...")
    # sync_service.sync_orders_to_moysklad()

if __name__ == "__main__":
    main()