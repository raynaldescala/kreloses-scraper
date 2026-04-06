"""
Kreloses Service Data Scraper
Extracts service information from the Kreloses clinic management system and exports to CSV.
"""

import asyncio
import csv
import hashlib
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
SERVICE_LIST_URL = os.getenv("KRELOSES_SERVICE_LIST_URL", "https://sea.kreloses.com/Service/List")
EMAIL = os.getenv("KRELOSES_EMAIL")
PASSWORD = os.getenv("KRELOSES_PASSWORD")

# File configuration
SERVICES_OUTPUT_FILE = os.getenv("SERVICES_OUTPUT_FILE", "services.csv")
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "service_scraper_progress.txt")
LOCK_FILE = os.getenv("LOCK_FILE", "service_scraper.lock")
SERVICE_ID_START = int(os.getenv("SERVICE_ID_START", "1"))


async def login(page):
    """Login to Kreloses"""
    print("Logging in...")
    await page.goto(LOGIN_URL)
    await page.fill('input[name="Email"], input[placeholder*="Email"], [aria-label="Email"]', EMAIL)
    await page.fill('input[name="Password"], input[placeholder*="Password"], [aria-label="Password"]', PASSWORD)
    await page.click('button:has-text("Log in")')
    await page.wait_for_url("**/sea.kreloses.com/**", timeout=30000)
    print("Login successful!")


async def get_service_links(page):
    """Get all service detail links using pagination (Next button)"""
    print("Fetching service list...")
    await page.goto(SERVICE_LIST_URL)
    await page.wait_for_selector('table', timeout=30000)
    await asyncio.sleep(1)  # Wait for initial data to load

    service_links = {}  # dict preserves insertion order
    page_num = 1

    while True:
        # Get pagination info
        current_page_text = ""
        try:
            footer = await page.query_selector('.table-footer span')
            if footer:
                current_page_text = await footer.inner_text()
                print(f"  Page {page_num}: {current_page_text}")
        except:
            pass

        # Collect all service links on current page
        links = await page.query_selector_all('a[href*="/service/details/"], a[href*="/Service/Details/"]')
        for link in links:
            href = await link.get_attribute('href')
            if href:
                href_lower = href.lower()
                if '/service/details/' in href_lower:
                    match = re.search(r'/service/details/(\d+)', href_lower)
                    if match:
                        service_id = match.group(1)
                        full_url = f"https://sea.kreloses.com/Service/Details/{service_id}"
                        service_links[full_url] = True

        print(f"  Collected {len(service_links)} services so far...")

        # Check if Next button (#FrontBtn) is available and not disabled
        next_btn = await page.query_selector('#FrontBtn:not(.disabled)')
        if not next_btn:
            print("  No more pages (Next button disabled)")
            break

        # Click Next button to go to next page
        await next_btn.click()
        page_num += 1
        
        # Wait for dynamic content to load
        try:
            await page.wait_for_function(
                f"""() => {{
                    const span = document.querySelector('.table-footer span');
                    return span && span.textContent !== '{current_page_text}';
                }}""",
                timeout=10000
            )
        except:
            await asyncio.sleep(2)
        
        await asyncio.sleep(0.5)

    print(f"Found {len(service_links)} total services across {page_num} pages")
    return list(service_links.keys())


async def extract_service_info(page, service_url):
    """Extract service info (name, price, category) from the service details page.
    
    Returns service_data dict (always returns, never None).
    """
    service_data = {'name': '', 'price': '', 'category': ''}
    
    try:
        await page.goto(service_url, timeout=30000)
        await page.wait_for_selector('[data-hook="main-page-title"]', timeout=10000)
        
        # Get service name from the title span
        title_span = await page.query_selector('[data-hook="main-page-title"]')
        if title_span:
            service_data['name'] = (await title_span.inner_text()).strip()
        
        # Wait for the service-prices-card to be visible
        await page.wait_for_selector('#service-prices-card', timeout=10000)
        await asyncio.sleep(2)  # Extra wait for React to fully render
        
        # Try to extract price using Playwright locators (more reliable)
        try:
            # Check if fixed-prices-table exists and is visible
            # IMPORTANT: Count only DIRECT children tr, not nested tr elements
            fixed_table_rows = page.locator('#fixed-prices-table > tbody > tr')
            row_count = await fixed_table_rows.count()
            
            print(f"  [DEBUG] Fixed price table rows (direct children): {row_count}")
            
            if row_count == 1:
                # Single row - try to get the price
                # The price input is deeply nested: tr > td > table > tbody > tr > td > div > span > input
                # Let's just find the input with id containing "fixed-price-price"
                price_inputs = page.locator('#fixed-prices-table input[id*="fixed-price-price"]')
                input_count = await price_inputs.count()
                print(f"  [DEBUG] Found {input_count} price input(s)")
                
                if input_count > 0:
                    price_value = await price_inputs.first.input_value()
                    service_data['price'] = price_value
                    print(f"  [DEBUG] Fixed price extracted: '{price_value}'")
                else:
                    print(f"  [DEBUG] No price input found in fixed table")
            elif row_count > 1:
                print(f"  [DEBUG] Multiple fixed price rows - omitting price")
                service_data['price'] = ''
            else:
                # Try rated prices
                rated_table_rows = page.locator('#rated-prices-table > tbody > tr')
                rated_row_count = await rated_table_rows.count()
                
                print(f"  [DEBUG] Rated price table rows (direct children): {rated_row_count}")
                
                if rated_row_count == 1:
                    # Single row - try to get the price
                    price_inputs = page.locator('#rated-prices-table input[id*="RatedPricesPrice"]')
                    input_count = await price_inputs.count()
                    print(f"  [DEBUG] Found {input_count} rated price input(s)")
                    
                    if input_count > 0:
                        price_value = await price_inputs.first.input_value()
                        service_data['price'] = price_value
                        print(f"  [DEBUG] Rated price extracted: '{price_value}'")
                    else:
                        print(f"  [DEBUG] No price input found in rated table")
                elif rated_row_count > 1:
                    print(f"  [DEBUG] Multiple rated price rows - omitting price")
                    service_data['price'] = ''
        except Exception as e:
            print(f"  [DEBUG] Error extracting price: {e}")
        
        # Now click on the Categories tab to get the category with chip class
        try:
            # Find and click the Categories tab using the tab id
            categories_tab = page.locator('#location-profile-tabs-tab-6')
            if await categories_tab.count() > 0:
                await categories_tab.click()
                await asyncio.sleep(0.5)  # Wait for tab content to load
                
                # Extract category with chip class (leave blank if not found)
                category_button = page.locator('.tree-node .entry .category-text-btn.chip')
                if await category_button.count() > 0:
                    service_data['category'] = await category_button.inner_text()
                
        except Exception as e:
            print(f"  [DEBUG] Error extracting category: {e}")
        
    except Exception as e:
        print(f"  [DEBUG] Error in extract_service_info: {e}")
    
    return service_data


def _acquire_lock(lock_file):
    """Acquire an exclusive lock to prevent concurrent scraper runs."""
    if os.path.exists(lock_file):
        try:
            with open(lock_file, 'r') as f:
                lock_data = json.load(f)
            old_pid = lock_data.get('pid')
            if old_pid:
                if _is_process_running(old_pid):
                    return False, f"Another scraper instance is running (PID {old_pid}). Lock file: {lock_file}"
                else:
                    print(f"  Removing stale lock file (PID {old_pid} no longer running)")
                    os.unlink(lock_file)
        except (json.JSONDecodeError, KeyError, OSError):
            try:
                os.unlink(lock_file)
            except:
                pass
    
    try:
        lock_data = {
            'pid': os.getpid(),
            'started_at': datetime.now().isoformat(),
            'command': ' '.join(sys.argv)
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
    except Exception as e:
        print(f"  Warning: Could not remove lock file {lock_file}: {e}")


def _is_process_running(pid):
    """Check if a process with the given PID is still running."""
    try:
        if os.name == 'nt':  # Windows
            import ctypes
            kernel32 = ctypes.windll.kernel32
            SYNCHRONIZE = 0x00100000
            process = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
            if process:
                kernel32.CloseHandle(process)
                return True
            return False
        else:  # Unix
            os.kill(pid, 0)
            return True
    except (OSError, PermissionError):
        return False


def _load_progress(progress_file):
    """Load progress file."""
    if not os.path.exists(progress_file):
        return set()
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            lines = f.read().strip().split('\n')
        return set(line.strip() for line in lines if line.strip())
    except Exception:
        return set()


def _save_progress(progress_file, processed_urls):
    """Save progress file."""
    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sorted(processed_urls)))
    except Exception as e:
        print(f"  Warning: Could not save progress: {e}")


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
        
        # Create backup before replacing
        if os.path.exists(filename):
            backup_path = filename + '.backup'
            try:
                shutil.copy2(filename, backup_path)
            except Exception as e:
                print(f"  Warning: Could not create backup: {e}")
        
        os.replace(temp_path, filename)
        
    except Exception as e:
        try:
            os.unlink(temp_path)
        except:
            pass
        raise RuntimeError(f"Failed to save {filename}: {e}") from e


def _read_csv_safe(filename):
    """Safely read a CSV file."""
    if not os.path.exists(filename):
        return [], None
    
    try:
        with open(filename, 'r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
        return rows, None
    except Exception as e:
        return [], f"Could not read {filename}: {e}"


async def scrape_all_services():
    """Main scraping function with incremental saving"""
    all_services = []
    processed_service_urls = set()
    failed_services = []
    next_service_id = SERVICE_ID_START
    
    # Acquire lock
    print("Acquiring lock...")
    lock_acquired, lock_result = _acquire_lock(LOCK_FILE)
    if not lock_acquired:
        print(f"  ERROR: {lock_result}")
        print("  Exiting to prevent data corruption.")
        return []
    print(f"  Lock acquired (PID {os.getpid()})")
    
    try:
        # Load progress
        processed_service_urls = _load_progress(PROGRESS_FILE)
        if processed_service_urls:
            print(f"Resuming from previous session: {len(processed_service_urls)} services already processed")
        
        # Load existing data
        print("Loading existing data...")
        all_services, services_error = _read_csv_safe(SERVICES_OUTPUT_FILE)
        if services_error:
            print(f"  Warning: {services_error}")
        elif all_services:
            first_row_keys = list(all_services[0].keys()) if all_services else []
            id_key = 'id' if 'id' in first_row_keys else (first_row_keys[0] if first_row_keys else 'id')
            max_id = max(int(s.get(id_key, s.get('id', SERVICE_ID_START))) for s in all_services)
            next_service_id = max_id + 1
            print(f"  Loaded {len(all_services)} existing services from {SERVICES_OUTPUT_FILE}")
        
        pending_urls = []
        pending_services = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Login
                await login(page)
                
                # Get all service links
                service_links = await get_service_links(page)
                
                # Filter out already processed services
                remaining_links = [url for url in service_links if url not in processed_service_urls]
                print(f"Remaining services to process: {len(remaining_links)}")
                
                # Extract data from each service
                total = len(remaining_links)
                for i, service_url in enumerate(remaining_links, 1):
                    print(f"Processing service {i}/{total}: {service_url}")
                    
                    try:
                        service_data = await extract_service_info(page, service_url)
                        
                        # Always include the service (never skip)
                        if service_data and service_data['name']:
                            service_data['id'] = next_service_id
                            pending_services.append(service_data)
                            all_services.append(service_data)
                            next_service_id += 1
                            
                            # Show what we found
                            category_display = service_data['category'] if service_data['category'] else '(no category)'
                            price_display = service_data['price'] if service_data['price'] else '(no price)'
                            print(f"  Found service: {service_data['name']} - {category_display} - {price_display}")
                        
                        pending_urls.append(service_url)
                        
                        # Save progress every 10 services
                        if i % 10 == 0 or i == total:
                            _atomic_csv_write(
                                SERVICES_OUTPUT_FILE,
                                ['id', 'name', 'price', 'category'],
                                all_services
                            )
                            processed_service_urls.update(pending_urls)
                            _save_progress(PROGRESS_FILE, processed_service_urls)
                            
                            print(f"  Progress saved: {len(all_services)} services")
                            pending_urls.clear()
                            pending_services.clear()
                        
                    except Exception as e:
                        print(f"  Error processing {service_url}: {e}")
                        failed_services.append(service_url)
                    
                    await asyncio.sleep(0.5)
                
            finally:
                # Save any pending work before closing
                if pending_urls:
                    print(f"\n  Saving {len(pending_urls)} pending services before exit...")
                    try:
                        _atomic_csv_write(
                            SERVICES_OUTPUT_FILE,
                            ['id', 'name', 'price', 'category'],
                            all_services
                        )
                        processed_service_urls.update(pending_urls)
                        _save_progress(PROGRESS_FILE, processed_service_urls)
                    except Exception as e:
                        print(f"  Warning: Failed to save pending work: {e}")
                
                await browser.close()
        
        # Final save
        if all_services:
            _atomic_csv_write(
                SERVICES_OUTPUT_FILE,
                ['id', 'name', 'price', 'category'],
                all_services
            )
        
        # Clean up progress file on success
        if os.path.exists(PROGRESS_FILE) and not failed_services:
            os.remove(PROGRESS_FILE)
        
        # Report failed services
        if failed_services:
            print(f"\nWarning: {len(failed_services)} services failed to process:")
            for url in failed_services[:10]:
                print(f"  - {url}")
            if len(failed_services) > 10:
                print(f"  ... and {len(failed_services) - 10} more")
        
        return all_services
    
    finally:
        _release_lock(LOCK_FILE)


async def main():
    """Main entry point"""
    print("=" * 60)
    print("Kreloses Service Data Scraper")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    services = await scrape_all_services()
    
    print()
    print(f"Total: {len(services)} services")
    print(f"Output file: {SERVICES_OUTPUT_FILE}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
