"""
Kreloses Product Data Scraper
Extracts product information from the Kreloses Product List and exports to CSV.
"""

import asyncio
import csv
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
LOGIN_URL = os.getenv("KRELOSES_LOGIN_URL", "https://www.kreloses.com/account/login")
PRODUCT_LIST_URL = os.getenv("KRELOSES_PRODUCT_LIST_URL", "https://sea.kreloses.com/Product/List")
EMAIL = os.getenv("KRELOSES_EMAIL")
PASSWORD = os.getenv("KRELOSES_PASSWORD")

# File configuration
PRODUCTS_OUTPUT_FILE = os.getenv("PRODUCTS_OUTPUT_FILE", "products.csv")
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "product_scraper_progress.txt")
LOCK_FILE = os.getenv("LOCK_FILE", "product_scraper.lock")
PRODUCT_ID_START = int(os.getenv("PRODUCT_ID_START", "1"))


def load_category_mapping(csv_file):
    """Load category mapping from CSV file.
    
    Returns dict: {category_name: category_id}
    """
    category_map = {}
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Handle potential BOM in first column
                id_key = 'id' if 'id' in row else list(row.keys())[0]
                name_key = 'name' if 'name' in row else list(row.keys())[1]
                
                category_name = row[name_key].strip().upper()
                category_id = row[id_key].strip()
                category_map[category_name] = category_id
        
        print(f"Loaded {len(category_map)} categories from {csv_file}")
        return category_map
    except Exception as e:
        print(f"Error loading category mapping: {e}")
        return {}


def generate_slug(product_name, used_slugs):
    """Generate a unique slug from product name.
    
    - Replace special characters with hyphens
    - Make lowercase
    - If duplicate, append -1, -2, etc.
    """
    if not product_name:
        return ''
    
    # Convert to lowercase and replace special chars with hyphen
    slug = re.sub(r'[^a-z0-9]+', '-', product_name.lower())
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    
    # Ensure uniqueness
    if slug not in used_slugs:
        used_slugs.add(slug)
        return slug
    
    # Append counter if duplicate
    counter = 1
    while f"{slug}-{counter}" in used_slugs:
        counter += 1
    
    unique_slug = f"{slug}-{counter}"
    used_slugs.add(unique_slug)
    return unique_slug


async def login(page):
    """Login to Kreloses"""
    print("Logging in...")
    await page.goto(LOGIN_URL)
    await page.fill('input[name="Email"], input[placeholder*="Email"], [aria-label="Email"]', EMAIL)
    await page.fill('input[name="Password"], input[placeholder*="Password"], [aria-label="Password"]', PASSWORD)
    await page.click('button:has-text("Log in")')
    await page.wait_for_url("**/sea.kreloses.com/**", timeout=30000)
    print("Login successful!")


async def configure_grid(page):
    """Configure the product grid to show required columns."""
    print("Configuring grid columns...")
    
    try:
        # Click the Config button
        config_btn = page.locator('[data-hook="ConfigBtn"], #GridSettings')
        await config_btn.click()
        await asyncio.sleep(0.5)
        
        # Wait for modal to appear
        await page.wait_for_selector('.modal-content', timeout=5000)
        
        # Debug: Check current column configuration
        column_states = await page.evaluate("""() => {
            const columns = {};
            const items = document.querySelectorAll('.picking li');
            for (const item of items) {
                const span = item.querySelector('span[id]');
                if (span && span.id) {
                    const isChecked = item.classList.contains('checked') || 
                                    span.classList.contains('glyphicons-check');
                    columns[span.id] = isChecked;
                }
            }
            return columns;
        }""")
        
        print(f"  [DEBUG] Current column configuration:")
        for col, enabled in column_states.items():
            status = "✓" if enabled else "✗"
            print(f"    {status} {col}")
        
        # Enable Cost column if not already enabled
        cost_checkbox = page.locator('#Cost')
        if await cost_checkbox.count() > 0:
            is_cost_enabled = column_states.get('Cost', False)
            if not is_cost_enabled:
                print("  [DEBUG] Enabling Cost column...")
                await cost_checkbox.click()
                await asyncio.sleep(0.2)
            else:
                print("  [DEBUG] Cost column already enabled")
        else:
            print("  [DEBUG] Cost checkbox not found!")
        
        # Click OK to save
        ok_btn = page.locator('#Ok')
        await ok_btn.click()
        await asyncio.sleep(1.5)  # Wait for grid to reload
        
        print("Grid configured successfully")
    except Exception as e:
        print(f"Warning: Could not configure grid: {e}")
        print("Continuing with default columns...")


async def scrape_products_from_table(page, category_map):
    """Scrape all products from the table using pagination."""
    all_products = []
    used_slugs = set()
    page_num = 1
    
    # Debug: Check table headers on first page
    if page_num == 1:
        headers = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('table thead th')).map(th => th.textContent.trim());
        }""")
        print(f"\n[DEBUG] Table headers: {headers}\n")
    
    while True:
        print(f"Scraping page {page_num}...")
        
        # Wait for table to load
        await page.wait_for_selector('table tbody tr', timeout=10000)
        await asyncio.sleep(1)
        
        # Extract data from current page
        products_data = await page.evaluate("""() => {
            const rows = document.querySelectorAll('table tbody tr');
            const products = [];
            
            // Get headers
            const headers = Array.from(document.querySelectorAll('table thead th')).map(th => th.textContent.trim().toLowerCase());
            
            for (const row of rows) {
                const cells = row.querySelectorAll('td');
                if (cells.length === 0) continue;
                
                // Extract text from each cell
                const cellTexts = Array.from(cells).map(cell => cell.textContent.trim());
                
                const product = {
                    name: '',
                    sku: '',
                    category: '',
                    price: '',
                    cost: '',
                    rawData: {}  // Store all cell data for debugging
                };
                
                // Map columns based on header names
                headers.forEach((header, index) => {
                    if (index >= cellTexts.length) return;
                    
                    product.rawData[header] = cellTexts[index];
                    
                    if (header.includes('name') && !header.includes('group') && !header.includes('supplier')) {
                        product.name = cellTexts[index];
                    } else if (header.includes('sku') && !header.includes('supplier')) {
                        product.sku = cellTexts[index];
                    } else if (header.includes('category')) {
                        product.category = cellTexts[index];
                    } else if (header === 'price' || (header.includes('price') && !header.includes('market') && !header.includes('cost'))) {
                        product.price = cellTexts[index];
                    } else if (header.includes('cost')) {
                        product.cost = cellTexts[index];
                    }
                });
                
                if (product.name) {
                    products.push(product);
                }
            }
            
            return products;
        }""")
        
        print(f"  Found {len(products_data)} products on page {page_num}")
        
        # Debug: Show first product's raw data
        if page_num == 1 and products_data:
            print(f"\n[DEBUG] First product raw data:")
            for key, value in products_data[0].get('rawData', {}).items():
                print(f"  {key}: {value}")
            print(f"[DEBUG] Extracted values:")
            print(f"  name: {products_data[0].get('name')}")
            print(f"  sku: {products_data[0].get('sku')}")
            print(f"  category: {products_data[0].get('category')}")
            print(f"  price: {products_data[0].get('price')}")
            print(f"  cost: {products_data[0].get('cost')}")
            print()
        
        # Process each product
        for product_data in products_data:
            # Clean price and cost
            price = re.sub(r'[^\d.]', '', product_data.get('price', '0'))
            cost = re.sub(r'[^\d.]', '', product_data.get('cost', '0'))
            
            # Default to 0 if empty
            price = price if price else '0'
            cost = cost if cost else '0'
            
            # Map category name to ID
            category_name = product_data.get('category', '').strip().upper()
            category_id = category_map.get(category_name, '')
            
            # Generate unique slug
            slug = generate_slug(product_data.get('name', ''), used_slugs)
            
            product = {
                'product_name': product_data.get('name', ''),
                'product_code': product_data.get('sku', ''),
                'category_id': category_id,
                'price': price,
                'cost_price': cost,
                'current_stock': '0',
                'slug': slug
            }
            
            all_products.append(product)
        
        # Check if Next button is available and not disabled
        next_btn = await page.query_selector('#FrontBtn:not(.disabled)')
        if not next_btn:
            print("No more pages")
            break
        
        # Click Next button
        await next_btn.click()
        page_num += 1
        await asyncio.sleep(1)
    
    return all_products


def _acquire_lock(lock_file):
    """Acquire an exclusive lock."""
    if os.path.exists(lock_file):
        try:
            with open(lock_file, 'r') as f:
                lock_data = json.load(f)
            old_pid = lock_data.get('pid')
            if old_pid and _is_process_running(old_pid):
                return False, f"Another scraper instance is running (PID {old_pid})"
            else:
                os.unlink(lock_file)
        except:
            try:
                os.unlink(lock_file)
            except:
                pass
    
    try:
        lock_data = {
            'pid': os.getpid(),
            'started_at': datetime.now().isoformat()
        }
        with open(lock_file, 'w') as f:
            json.dump(lock_data, f)
        return True, lock_file
    except Exception as e:
        return False, f"Could not create lock file: {e}"


def _release_lock(lock_file):
    """Release the lock file."""
    try:
        if os.path.exists(lock_file):
            os.unlink(lock_file)
    except:
        pass


def _is_process_running(pid):
    """Check if a process is running."""
    try:
        if os.name == 'nt':
            import ctypes
            kernel32 = ctypes.windll.kernel32
            SYNCHRONIZE = 0x00100000
            process = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
            if process:
                kernel32.CloseHandle(process)
                return True
            return False
        else:
            os.kill(pid, 0)
            return True
    except:
        return False


def _atomic_csv_write(filename, fieldnames, rows):
    """Atomically write CSV data."""
    if not rows:
        return
    
    target_dir = os.path.dirname(os.path.abspath(filename)) or '.'
    base_name = os.path.basename(filename)
    fd, temp_path = tempfile.mkstemp(suffix=f'.{base_name}.tmp', dir=target_dir)
    
    try:
        with os.fdopen(fd, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            csvfile.flush()
            os.fsync(csvfile.fileno())
        
        if os.path.exists(filename):
            backup_path = filename + '.backup'
            try:
                shutil.copy2(filename, backup_path)
            except:
                pass
        
        os.replace(temp_path, filename)
    except Exception as e:
        try:
            os.unlink(temp_path)
        except:
            pass
        raise RuntimeError(f"Failed to save {filename}: {e}") from e


async def scrape_all_products(category_csv_file):
    """Main scraping function."""
    # Load category mapping
    category_map = load_category_mapping(category_csv_file)
    if not category_map:
        print("Warning: No categories loaded. Category IDs will be empty.")
    
    # Acquire lock
    print("Acquiring lock...")
    lock_acquired, lock_result = _acquire_lock(LOCK_FILE)
    if not lock_acquired:
        print(f"ERROR: {lock_result}")
        return []
    print(f"Lock acquired (PID {os.getpid()})")
    
    all_products = []
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Login
                await login(page)
                
                # Navigate to Product List
                await page.goto(PRODUCT_LIST_URL)
                await page.wait_for_selector('table', timeout=30000)
                await asyncio.sleep(1)
                
                # Configure grid to show Cost column
                await configure_grid(page)
                
                # Scrape all products
                all_products = await scrape_products_from_table(page, category_map)
                
                print(f"\nTotal products scraped: {len(all_products)}")
                
            finally:
                await browser.close()
        
        # Save to CSV
        if all_products:
            fieldnames = ['product_name', 'product_code', 'category_id', 'price', 'cost_price', 'current_stock', 'slug']
            _atomic_csv_write(PRODUCTS_OUTPUT_FILE, fieldnames, all_products)
            print(f"Saved to {PRODUCTS_OUTPUT_FILE}")
        
        return all_products
    
    finally:
        _release_lock(LOCK_FILE)


async def main():
    """Main entry point"""
    print("=" * 60)
    print("Kreloses Product Data Scraper")
    print("=" * 60)
    
    # Check for category CSV argument
    if len(sys.argv) < 2:
        print("Usage: python kreloses_product_scraper.py <category_csv_file>")
        print("Example: python kreloses_product_scraper.py product-categories-caloi.csv")
        sys.exit(1)
    
    category_csv_file = sys.argv[1]
    
    if not os.path.exists(category_csv_file):
        print(f"Error: Category CSV file not found: {category_csv_file}")
        sys.exit(1)
    
    print(f"Using category mapping from: {category_csv_file}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    products = await scrape_all_products(category_csv_file)
    
    print()
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
