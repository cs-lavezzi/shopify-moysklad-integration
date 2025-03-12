import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any

from tenacity import retry, stop_after_attempt, wait_fixed

from models.models import MoyskladProduct, ShopifyProduct
from services.shopify_service import ShopifyService
from services.moysklad_service import MoyskladService
from utils.config import Config
from utils.converters import (
    moysklad_to_shopify_product, 
    price_to_moysklad_format,
    shopify_to_moysklad_product
)
from utils.exceptions import MoyskladError, ShopifyError

logger = logging.getLogger(__name__)

# Paketviy ishlash uchun konstantalar
BATCH_SIZE = 50  # Bir vaqtda ishlash uchun mahsulotlar soni
MAX_CONCURRENT_TASKS = 5  # Bir vaqtda bajariladigan parallel vazifalar


class ProductSyncService:
    """
    Service for synchronizing products between Moysklad and Shopify.
    Optimized for incremental updates and batch processing.
    """

    def __init__(self, config: Config):
        """
        Initialize the ProductSyncService.

        Args:
            config: Configuration object containing API credentials and settings
        """
        self.config = config
        self.shopify_service = ShopifyService(config)
        self.moysklad_service = MoyskladService(config)
        self.last_sync_time = None

    async def sync_products(self, full_sync: bool = False) -> None:
        """
        Main synchronization method that handles bidirectional sync between
        Moysklad and Shopify.

        Args:
            full_sync: If True, sync all products. If False, sync only recently modified products.
        """
        try:
            start_time = datetime.now()
            logger.info(f"Starting product synchronization. Full sync: {full_sync}")
            
            # Get timestamp for incremental sync
            if not full_sync and self.last_sync_time:
                modified_since = self.last_sync_time
                logger.info(f"Performing incremental sync since {modified_since}")
            else:
                # Default to 24 hours if no previous sync or full sync requested
                modified_since = datetime.now() - timedelta(hours=24) if not full_sync else None
                logger.info(f"Performing {'full' if full_sync else 'default 24-hour'} sync")
            
            # Get products from both platforms
            moysklad_products = await self.moysklad_service.get_all_products(modified_since=modified_since)
            shopify_products = await self.shopify_service.get_all_products(modified_since=modified_since)
            
            logger.info(f"Found {len(moysklad_products)} products in Moysklad to process")
            logger.info(f"Found {len(shopify_products)} products in Shopify to process")
            
            # If we're doing incremental sync, we need to fetch all products for lookup
            # but only sync the recently modified ones
            all_moysklad_products = moysklad_products
            all_shopify_products = shopify_products
            
            if not full_sync and modified_since:
                # For lookups, we need all products
                all_moysklad_products = await self.moysklad_service.get_all_products()
                all_shopify_products = await self.shopify_service.get_all_products()
            
            # Create mappings for easier lookup
            moysklad_by_code = {p.code: p for p in all_moysklad_products if p.code}
            shopify_by_sku = {p.sku: p for p in all_shopify_products if p.sku}
            
            # Synchronize in both directions
            await asyncio.gather(
                self._sync_moysklad_to_shopify(moysklad_products, shopify_by_sku),
                self._sync_shopify_to_moysklad(shopify_products, moysklad_by_code)
            )
            
            # Update last sync time
            self.last_sync_time = start_time
            logger.info(f"Product synchronization completed successfully in {datetime.now() - start_time}")
        
        except (MoyskladError, ShopifyError) as e:
            logger.error(f"Error during product synchronization: {str(e)}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error during product synchronization: {str(e)}")
            raise

    async def _sync_moysklad_to_shopify(
        self, 
        moysklad_products: List[MoyskladProduct],
        shopify_by_sku: Dict[str, ShopifyProduct]
    ) -> None:
        """
        Synchronize products from Moysklad to Shopify using batch processing.
        
        Args:
            moysklad_products: List of products from Moysklad
            shopify_by_sku: Dictionary of Shopify products indexed by SKU
        """
        logger.info("Starting sync from Moysklad to Shopify")
        
        # Filter products with valid codes
        valid_products = [p for p in moysklad_products if p.code]
        logger.info(f"Found {len(valid_products)} Moysklad products with valid codes")
        
        # Process in batches
        for i in range(0, len(valid_products), BATCH_SIZE):
            batch = valid_products[i:i+BATCH_SIZE]
            logger.info(f"Processing Moysklad batch {i//BATCH_SIZE + 1}/{(len(valid_products) + BATCH_SIZE - 1)//BATCH_SIZE}, size: {len(batch)}")
            
            # Process batch with limited concurrency
            tasks = []
            for ms_product in batch:
                shopify_product = shopify_by_sku.get(ms_product.code)
                if shopify_product:
                    # Update existing product
                    tasks.append(self._update_shopify_product(ms_product, shopify_product))
                else:
                    # Create new product
                    tasks.append(self._create_shopify_product(ms_product))
            
            # Execute tasks with limited concurrency
            await self._execute_tasks_with_limited_concurrency(tasks)
            
            # Small delay between batches to avoid rate limiting
            await asyncio.sleep(0.5)
    
    async def _sync_shopify_to_moysklad(
        self, 
        shopify_products: List[ShopifyProduct],
        moysklad_by_code: Dict[str, MoyskladProduct]
    ) -> None:
        """
        Synchronize products from Shopify to Moysklad using batch processing.
        
        Args:
            shopify_products: List of products from Shopify
            moysklad_by_code: Dictionary of Moysklad products indexed by code
        """
        logger.info("Starting sync from Shopify to Moysklad")
        
        # Filter products with valid SKUs
        valid_products = [p for p in shopify_products if p.sku]
        logger.info(f"Found {len(valid_products)} Shopify products with valid SKUs")
        
        # Process in batches
        for i in range(0, len(valid_products), BATCH_SIZE):
            batch = valid_products[i:i+BATCH_SIZE]
            logger.info(f"Processing Shopify batch {i//BATCH_SIZE + 1}/{(len(valid_products) + BATCH_SIZE - 1)//BATCH_SIZE}, size: {len(batch)}")
            
            # Process batch with limited concurrency
            tasks = []
            for shopify_product in batch:
                moysklad_product = moysklad_by_code.get(shopify_product.sku)
                if moysklad_product:
                    # Update existing product
                    tasks.append(self._update_moysklad_product(shopify_product, moysklad_product))
                else:
                    # Create new product
                    tasks.append(self._create_moysklad_product(shopify_product))
            
            # Execute tasks with limited concurrency
            await self._execute_tasks_with_limited_concurrency(tasks)
            
            # Small delay between batches to avoid rate limiting
            await asyncio.sleep(0.5)

    async def _execute_tasks_with_limited_concurrency(self, tasks: List[asyncio.Task]) -> List[Any]:
        """
        Execute a list of tasks with limited concurrency to avoid overwhelming APIs.
        
        Args:
            tasks: List of tasks to execute
            
        Returns:
            List of task results
        """
        results = []
        for i in range(0, len(tasks), MAX_CONCURRENT_TASKS):
            batch = tasks[i:i+MAX_CONCURRENT_TASKS]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            # Process results to handle exceptions
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Task {i+j} failed: {str(result)}")
                else:
                    results.append(result)
                    
        return results

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _create_shopify_product(self, ms_product: MoyskladProduct) -> ShopifyProduct:
        """
        Create a new product in Shopify based on Moysklad product.
        
        Args:
            ms_product: Moysklad product to create in Shopify
            
        Returns:
            Newly created Shopify product
        """
        logger.info(f"Creating new Shopify product from Moysklad ID: {ms_product.id}")
        
        try:
            # Convert Moysklad product to Shopify format
            shopify_product_data = moysklad_to_shopify_product(ms_product)
            
            # Create product in Shopify
            shopify_product = await self.shopify_service.create_product(shopify_product_data)
            
            logger.info(f"Successfully created Shopify product: {shopify_product.id}")
            return shopify_product
        except Exception as e:
            logger.error(f"Failed to create Shopify product from Moysklad ID {ms_product.id}: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _update_shopify_product(
        self, 
        ms_product: MoyskladProduct, 
        shopify_product: ShopifyProduct
    ) -> ShopifyProduct:
        """
        Update an existing Shopify product with data from Moysklad.
        
        Args:
            ms_product: Moysklad product with updated data
            shopify_product: Existing Shopify product to update
            
        Returns:
            Updated Shopify product
        """
        logger.info(f"Updating Shopify product {shopify_product.id} from Moysklad ID: {ms_product.id}")
        
        try:
            # Check if product needs updating by comparing modified dates if available
            if hasattr(ms_product, 'updated_at') and hasattr(shopify_product, 'updated_at'):
                if ms_product.updated_at <= shopify_product.updated_at:
                    logger.info(f"Skipping update for Shopify product {shopify_product.id} - no changes detected")
                    return shopify_product
            
            # Convert Moysklad product to Shopify format
            updated_data = moysklad_to_shopify_product(ms_product)
            
            # Preserve the Shopify ID
            updated_data.id = shopify_product.id
            
            # Update product in Shopify
            updated_product = await self.shopify_service.update_product(updated_data)
            
            logger.info(f"Successfully updated Shopify product: {shopify_product.id}")
            return updated_product
        except Exception as e:
            logger.error(f"Failed to update Shopify product {shopify_product.id}: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _create_moysklad_product(self, shopify_product: ShopifyProduct) -> MoyskladProduct:
        """
        Create a new product in Moysklad based on Shopify product.
        
        Args:
            shopify_product: Shopify product to create in Moysklad
            
        Returns:
            Newly created Moysklad product
        """
        logger.info(f"Creating new Moysklad product from Shopify ID: {shopify_product.id}")
        
        try:
            # Convert Shopify product to Moysklad format
            moysklad_product_data = shopify_to_moysklad_product(shopify_product)
            
            # Create product in Moysklad
            moysklad_product = await self.moysklad_service.create_product(moysklad_product_data)
            
            logger.info(f"Successfully created Moysklad product: {moysklad_product.id}")
            return moysklad_product
        except Exception as e:
            logger.error(f"Failed to create Moysklad product from Shopify ID {shopify_product.id}: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _update_moysklad_product(
        self, 
        shopify_product: ShopifyProduct, 
        moysklad_product: MoyskladProduct
    ) -> MoyskladProduct:
        """
        Update an existing Moysklad product with data from Shopify.
        
        Args:
            shopify_product: Shopify product with updated data
            moysklad_product: Existing Moysklad product to update
            
        Returns:
            Updated Moysklad product
        """
        logger.info(f"Updating Moysklad product {moysklad_product.id} from Shopify ID: {shopify_product.id}")
        
        try:
            # Check if product needs updating by comparing modified dates if available
            if hasattr(shopify_product, 'updated_at') and hasattr(moysklad_product, 'updated_at'):
                if shopify_product.updated_at <= moysklad_product.updated_at:
                    logger.info(f"Skipping update for Moysklad product {moysklad_product.id} - no changes detected")
                    return moysklad_product
            
            # Convert Shopify product to Moysklad format
            updated_data = shopify_to_moysklad_product(shopify_product)
            
            # Preserve the Moysklad ID
            updated_data.id = moysklad_product.id
            
            # Update product in Moysklad
            updated_product = await self.moysklad_service.update_product(updated_data)
            
            logger.info(f"Successfully updated Moysklad product: {moysklad_product.id}")
            return updated_product
        except Exception as e:
            logger.error(f"Failed to update Moysklad product {moysklad_product.id}: {str(e)}")
            raise

    async def sync_product_inventory(self, full_sync: bool = False) -> None:
        """
        Synchronize product inventory (stock) between Moysklad and Shopify.
        
        Args:
            full_sync: If True, sync all products. If False, sync only recently modified products.
        """
        try:
            start_time = datetime.now()
            logger.info(f"Starting inventory synchronization. Full sync: {full_sync}")
            
            # Get timestamp for incremental sync
            modified_since = None
            if not full_sync and self.last_sync_time:
                modified_since = self.last_sync_time
                logger.info(f"Performing incremental inventory sync since {modified_since}")
            
            # Get products with stock information
            moysklad_products = await self.moysklad_service.get_all_products(
                with_stock=True, 
                modified_since=modified_since
            )
            shopify_products = await self.shopify_service.get_all_products(
                with_inventory=True
            )
            
            logger.info(f"Found {len(moysklad_products)} Moysklad products with stock information")
            logger.info(f"Found {len(shopify_products)} Shopify products for inventory update")
            
            # Create mappings by code/SKU
            moysklad_by_code = {p.code: p for p in moysklad_products if p.code}
            shopify_by_sku = {p.sku: p for p in shopify_products if p.sku}
            
            # Prepare batch processing
            valid_products = [
                (ms_product, shopify_by_sku.get(ms_product.code))
                for ms_product in moysklad_products
                if ms_product.code and ms_product.code in shopify_by_sku
            ]
            
            logger.info(f"Found {len(valid_products)} products to update inventory")
            
            # Process in batches
            for i in range(0, len(valid_products), BATCH_SIZE):
                batch = valid_products[i:i+BATCH_SIZE]
                logger.info(f"Processing inventory batch {i//BATCH_SIZE + 1}/{(len(valid_products) + BATCH_SIZE - 1)//BATCH_SIZE}, size: {len(batch)}")
                
                # Process batch with limited concurrency
                tasks = [
                    self._update_shopify_inventory(ms_product, shopify_product)
                    for ms_product, shopify_product in batch
                ]
                
                # Execute tasks with limited concurrency
                await self._execute_tasks_with_limited_concurrency(tasks)
                
                # Small delay between batches to avoid rate limiting
                await asyncio.sleep(0.5)
                
            logger.info(f"Inventory synchronization completed successfully in {datetime.now() - start_time}")
            
        except Exception as e:
            logger.exception(f"Error during inventory synchronization: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _update_shopify_inventory(
        self, 
        ms_product: MoyskladProduct, 
        shopify_product: ShopifyProduct
    ) -> None:
        """
        Update Shopify product inventory based on Moysklad stock.
        
        Args:
            ms_product: Moysklad product with stock information
            shopify_product: Shopify product to update inventory
        """
        logger.info(f"Updating inventory for Shopify product {shopify_product.id}")
        
        try:
            # Check if inventory needs updating (avoid unnecessary API calls)
            current_stock = getattr(shopify_product, 'inventory_quantity', None)
            moysklad_stock = ms_product.stock if ms_product.stock is not None else 0
            
            if current_stock is not None and current_stock == moysklad_stock:
                logger.info(f"Skipping inventory update for Shopify product {shopify_product.id} - no change needed")
                return
            
            # Update inventory in Shopify
            await self.shopify_service.update_inventory(
                product_id=shopify_product.id,
                inventory_item_id=shopify_product.inventory_item_id,
                quantity=moysklad_stock
            )
            
            logger.info(f"Successfully updated inventory for Shopify product {shopify_product.id} to {moysklad_stock}")
        except Exception as e:
            logger.error(f"Failed to update inventory for Shopify product {shopify_product.id}: {str(e)}")
            raise

    async def sync_product_prices(self, full_sync: bool = False) -> None:
        """
        Synchronize product prices between Moysklad and Shopify.
        
        Args:
            full_sync: If True, sync all products. If False, sync only recently modified products.
        """
        try:
            start_time = datetime.now()
            logger.info(f"Starting price synchronization. Full sync: {full_sync}")
            
            # Get timestamp for incremental sync
            modified_since = None
            if not full_sync and self.last_sync_time:
                modified_since = self.last_sync_time
                logger.info(f"Performing incremental price sync since {modified_since}")
            
            # Get products with price information
            moysklad_products = await self.moysklad_service.get_all_products(
                modified_since=modified_since
            )
            shopify_products = await self.shopify_service.get_all_products(
                modified_since=modified_since
            )
            
            # Create mappings by code/SKU
            moysklad_by_code = {p.code: p for p in moysklad_products if p.code}
            shopify_by_sku = {p.sku: p for p in shopify_products if p.sku}
            
            # Determine which platform is source of truth for prices based on config
            if self.config.price_sync_direction == "moysklad_to_shopify":
                await self._sync_prices_moysklad_to_shopify(moysklad_products, shopify_by_sku, batch_size=BATCH_SIZE)
            elif self.config.price_sync_direction == "shopify_to_moysklad":
                await self._sync_prices_shopify_to_moysklad(shopify_products, moysklad_by_code, batch_size=BATCH_SIZE)
            else:
                logger.warning(f"Invalid price sync direction: {self.config.price_sync_direction}")
                
            logger.info(f"Price synchronization completed successfully in {datetime.now() - start_time}")
            
        except Exception as e:
            logger.exception(f"Error during price synchronization: {str(e)}")
            raise

    async def _sync_prices_moysklad_to_shopify(
        self, 
        moysklad_products: List[MoyskladProduct],
        shopify_by_sku: Dict[str, ShopifyProduct],
        batch_size: int = BATCH_SIZE
    ) -> None:
        """
        Sync prices from Moysklad to Shopify.
        
        Args:
            moysklad_products: List of Moysklad products
            shopify_by_sku: Mapping of Shopify products by SKU
            batch_size: Number of products to process in each batch
        """
        logger.info("Syncing prices from Moysklad to Shopify")
        
        # Prepare batch processing
        valid_products = [
            (ms_product, shopify_by_sku.get(ms_product.code))
            for ms_product in moysklad_products
            if ms_product.code and ms_product.price is not None and ms_product.code in shopify_by_sku
        ]
        
        logger.info(f"Found {len(valid_products)} products to update prices")
        
        # Process in batches
        for i in range(0, len(valid_products), batch_size):
            batch = valid_products[i:i+batch_size]
            logger.info(f"Processing price batch {i//batch_size + 1}/{(len(valid_products) + batch_size - 1)//batch_size}, size: {len(batch)}")
            
            # Process batch with limited concurrency
            tasks = []
            for ms_product, shopify_product in batch:
                # Check if price needs updating
                if shopify_product.price != ms_product.price:
                    tasks.append(self._update_shopify_price(shopify_product.id, shopify_product.variant_id, ms_product.price))
            
            if tasks:
                # Execute tasks with limited concurrency
                await self._execute_tasks_with_limited_concurrency(tasks)
            else:
                logger.info("No price updates needed for this batch")
            
            # Small delay between batches to avoid rate limiting
            await asyncio.sleep(0.5)

    async def _sync_prices_shopify_to_moysklad(
        self, 
        shopify_products: List[ShopifyProduct],
        moysklad_by_code: Dict[str, MoyskladProduct],
        batch_size: int = BATCH_SIZE
    ) -> None:
        """
        Sync prices from Shopify to Moysklad.
        
        Args:
            shopify_products: List of Shopify products
            moysklad_by_code: Mapping of Moysklad products by code
            batch_size: Number of products to process in each batch
        """
        logger.info("Syncing prices from Shopify to Moysklad")
        
        # Prepare batch processing
        valid_products = [
            (shopify_product, moysklad_by_code.get(shopify_product.sku))
            for shopify_product in shopify_products
            if shopify_product.sku and shopify_product.price is not None and shopify_product.sku in moysklad_by_code
        ]
        
        logger.info(f"Found {len(valid_products)} products to update prices")
        
        # Process in batches
        for i in range(0, len(valid_products), batch_size):
            batch = valid_products[i:i+batch_size]
            logger.info(f"Processing price batch {i//batch_size + 1}/{(len(valid_products) + batch_size - 1)//batch_size}, size: {len(batch)}")
            
            # Process batch with limited concurrency
            tasks = []
            for shopify_product, moysklad_product in batch:
                # Convert price to Moysklad format
                moysklad_price = price_to_moysklad_format(shopify_product.price)
                
                # Check if price needs updating
                if moysklad_product.price != moysklad_price:
                    tasks.append(self._update_moysklad_price(moysklad_product.id, moysklad_price))
            
            if tasks:
                # Execute tasks with limited concurrency
                await self._execute_tasks_with_limited_concurrency(tasks)
            else:
                logger.info("No price updates needed for this batch")
            
            # Small delay between batches to avoid rate limiting
            await asyncio.sleep(0.5)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _update_shopify_price(
        self, 
        product_id: str,
        variant_id: str,
        price: float
    ) -> None:
        """
        Update price for a Shopify product.
        
        Args:
            product_id: Shopify product ID
            variant_id: Shopify variant ID
            price: New price
        """
        try:
            await self.shopify_service.update_price(
                product_id=product_id,
                variant_id=variant_id,
                price=price
            )
            logger.info(f"Updated price for Shopify product {product_id} to {price}")
        except Exception as e:
            logger.error(f"Failed to update price for Shopify product {product_id}: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def _update_moysklad_price(
        self, 
        product_id: str,
        price: float
    ) -> None:
        """
        Update price for a Moysklad product.
        
        Args:
            product_id: Moysklad product ID
            price: New price
        """
        try:
            await self.moysklad_service.update_price(
                product_id=product_id,
                price=price
            )
            logger.info(f"Updated price for Moysklad product {product_id} to {price}")
        except Exception as e:
            logger.error(f"Failed to update price for Moysklad product {product_id}: {str(e)}")
            raise
            
    async def run_complete_sync(self) -> None:
        """
        Run a complete synchronization of products, inventory and prices.
        """
        try:
            logger.info("Starting complete synchronization process")
            start_time = datetime.now()
            
            # Run all sync operations in sequence
            await self.sync_products(full_sync=True)
            await self.sync_product_inventory(full_sync=True)
            await self.sync_product_prices(full_sync=True)
            
            logger.info(f"Complete synchronization finished successfully in {datetime.now() - start_time}")
        except Exception as e:
            logger.exception(f"Error during complete synchronization: {str(e)}")
            raise
            
    async def run_incremental_sync(self) -> None:
        """
        Run an incremental synchronization of products, inventory and prices.
        Only updates products that have changed since the last sync.
        """
        try:
            logger.info("Starting incremental synchronization process")
            start_time = datetime.now()
            
            # Run all sync operations in sequence with incremental mode
            await self.sync_products(full_sync=False)
            await self.sync_product_inventory(full_sync=False)
            await self.sync_product_prices(full_sync=False)
            
            logger.info(f"Incremental synchronization finished successfully in {datetime.now() - start_time}")
        except Exception as e:
            logger.exception(f"Error during incremental synchronization: {str(e)}")
            raise