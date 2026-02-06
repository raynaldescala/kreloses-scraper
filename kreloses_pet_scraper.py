"""
Kreloses Pet Data Scraper
Extracts pet information from the Kreloses clinic management system and exports to CSV.
"""

import asyncio
import csv
import glob
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
CUSTOMER_LIST_URL = os.getenv("KRELOSES_CUSTOMER_LIST_URL", "https://sea.kreloses.com/Customer/List")
EMAIL = os.getenv("KRELOSES_EMAIL")
PASSWORD = os.getenv("KRELOSES_PASSWORD")
DEFAULT_PASSWORD_HASH = os.getenv("DEFAULT_PASSWORD_HASH")

# File configuration
PETS_OUTPUT_FILE = os.getenv("PETS_OUTPUT_FILE", "pets.csv")
CUSTOMERS_OUTPUT_FILE = os.getenv("CUSTOMERS_OUTPUT_FILE", "customers.csv")
MEDICAL_RECORDS_OUTPUT_FILE = os.getenv("MEDICAL_RECORDS_OUTPUT_FILE", "medical_records.csv")
MEDICAL_RECORD_ENTRIES_OUTPUT_FILE = os.getenv("MEDICAL_RECORD_ENTRIES_OUTPUT_FILE", "medical_record_entries.csv")
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "scraper_progress.txt")
LOCK_FILE = os.getenv("LOCK_FILE", "scraper.lock")
CUSTOMER_ID_START = int(os.getenv("CUSTOMER_ID_START", "100"))
PET_ID_START = int(os.getenv("PET_ID_START", "100"))
MEDICAL_RECORD_ID_START = int(os.getenv("MEDICAL_RECORD_ID_START", "100"))


async def _extract_pet_fields_from_card(card):
    """Extract all pet field values from a white-card container.
    
    The actual DOM structure is:
    <div class="white-card collapsible">
      <div class="header">...</div>
      <div>
        <div class="tables keyvalue-set value-table">
          <div class="rows">
            <div class="cells">Species:</div>
            <div class="cells">Canine</div>
          </div>
        </div>
        ...
      </div>
    </div>
    """
    try:
        result = await card.evaluate(
            """(el) => {
                const data = {};
                
                // Find all keyvalue tables in the card
                const tables = el.querySelectorAll('.tables.keyvalue-set, .value-table, .keyvalue-set');
                
                for (const table of tables) {
                    // Each row has label in first .cells, value in second .cells
                    const rows = table.querySelectorAll('.rows');
                    
                    for (const row of rows) {
                        const cells = row.querySelectorAll('.cells');
                        if (cells.length >= 2) {
                            let labelText = (cells[0].textContent || '').trim();
                            let valueText = (cells[1].textContent || '').trim();
                            
                            // Normalize label (remove colon)
                            labelText = labelText.replace(/:$/, '').toLowerCase();
                            
                            // Map to our field names
                            if (labelText === 'species') data.species = valueText;
                            else if (labelText === 'breed') data.breed = valueText;
                            else if (labelText === 'gender') data.gender = valueText;
                            else if (labelText === 'neutered') data.neutered = valueText;
                            else if (labelText.includes('d.o.b') || labelText.includes('dob')) data.birthdate = valueText;
                            else if (labelText === 'colour' || labelText === 'color') data.color = valueText;
                        }
                    }
                }
                
                return data;
            }"""
        )
        return result
    except Exception:
        return {}


def _format_pet_data(pet_data):
    """Format and clean pet data fields."""
    
    # Clean pet_name: remove parenthesis content, e.g., "PEPPER (Canine)" → "PEPPER"
    if pet_data.get('pet_name'):
        pet_data['pet_name'] = re.sub(r'\s*\([^)]*\)', '', pet_data['pet_name']).strip()
    
    # Format birthdate: "25/07/2023 (2y, 6m)" → "2023-07-25"
    if pet_data.get('birthdate'):
        # Remove parenthesis content first
        birthdate = re.sub(r'\s*\([^)]*\)', '', pet_data['birthdate']).strip()
        # Try to parse DD/MM/YYYY format and convert to YYYY-MM-DD
        date_match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', birthdate)
        if date_match:
            day, month, year = date_match.groups()
            pet_data['birthdate'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            pet_data['birthdate'] = ''  # Clear invalid dates (e.g., "Invalid date")
    
    # Format spayed (neutered): "Yes" → "true", "No"/empty → "false"
    spayed = (pet_data.get('spayed') or '').strip().lower()
    if spayed == 'yes':
        pet_data['spayed'] = 'true'
    else:
        pet_data['spayed'] = 'false'
    
    # Format species: empty → "Unknown"
    if not pet_data.get('species') or not pet_data['species'].strip():
        pet_data['species'] = 'Unknown'
    
    return pet_data


def _is_valid_mobile(number):
    """Check if a number is a valid Philippine mobile number.
    
    Valid mobile: 11-12 digits with 63 prefix.
    - 12 digits (full): 639XXXXXXXXX
    - 11 digits (missing 1): 63XXXXXXXXX - still valid
    - 10 or fewer digits: invalid
    """
    if not number:
        return False
    digits = re.sub(r'[^0-9]', '', number)
    # Must start with 63 and be 11-12 digits
    if digits.startswith('63') and 11 <= len(digits) <= 12:
        return True
    return False


def _is_landline(number):
    """Check if a number looks like a landline/dial number.
    
    Landlines are typically 7-8 digits (without area code) or with area code.
    We consider it a landline if it's 7-10 digits and doesn't look like a mobile.
    """
    if not number:
        return False
    digits = re.sub(r'[^0-9]', '', number)
    # Landline: 7-10 digits, not starting with 639
    if 7 <= len(digits) <= 10 and not digits.startswith('639'):
        return True
    # Also consider numbers starting with area codes (02, 03, etc.)
    if digits.startswith('0') and 8 <= len(digits) <= 11 and not digits.startswith('09'):
        return True
    return False


def _parse_phone_numbers(phone_str):
    """Parse concatenated phone numbers and format with 63 prefix.
    
    Returns: (mobiles, landlines) - two lists
    - mobiles: valid mobile numbers (11-12 digits with 63 prefix)
    - landlines: dial/landline numbers
    
    Invalid numbers (single digit, mobile missing 2+ digits) are discarded.
    """
    if not phone_str:
        return [], []
    
    # Remove UI artifacts like 'arrow_drop_down' and other non-numeric characters
    phone_str = re.sub(r'arrow_drop_down|arrow_drop_up|expand_more|expand_less', '', phone_str, flags=re.IGNORECASE)
    # Remove any spaces, dashes, letters, or other separators - keep only digits
    phone_str = re.sub(r'[^0-9]', '', phone_str)
    
    raw_numbers = []
    
    # Try to split into numbers
    remaining = phone_str
    while remaining:
        # Check if starts with 09 (Philippine mobile format - 11 digits)
        if remaining.startswith('09') and len(remaining) >= 11:
            number = remaining[:11]
            # Format: remove leading 0, add 63 prefix
            formatted = '63' + number[1:]  # 09xxx -> 639xxx
            raw_numbers.append(formatted)
            remaining = remaining[11:]
        # Check if starts with 639 (already has country code - 12 digits)
        elif remaining.startswith('639') and len(remaining) >= 12:
            number = remaining[:12]
            raw_numbers.append(number)
            remaining = remaining[12:]
        # Check if starts with 63 but not 639 (might be incomplete)
        elif remaining.startswith('63') and len(remaining) >= 11:
            # Take 11-12 digits
            take_len = min(12, len(remaining))
            number = remaining[:take_len]
            raw_numbers.append(number)
            remaining = remaining[take_len:]
        # Check for landline starting with 0 (area code)
        elif remaining.startswith('0') and not remaining.startswith('09') and len(remaining) >= 8:
            # Landline with area code (e.g., 028XXXXXXX)
            # Take up to 11 digits
            take_len = min(11, len(remaining))
            number = remaining[:take_len]
            raw_numbers.append(number)
            remaining = remaining[take_len:]
        else:
            # Whatever is left
            if remaining and len(remaining) >= 7:
                raw_numbers.append(remaining)
            break
    
    # Classify numbers
    mobiles = []
    landlines = []
    
    for num in raw_numbers:
        digits = re.sub(r'[^0-9]', '', num)
        
        # Skip very short numbers (1-6 digits) - invalid
        if len(digits) < 7:
            continue
        
        # Check if it's a valid mobile
        if _is_valid_mobile(num):
            mobiles.append(num)
        # Check if it's a landline
        elif _is_landline(num):
            landlines.append(num)
        # If it starts with 63 but is too short (missing 2+ digits), skip it
        elif digits.startswith('63') and len(digits) < 11:
            continue  # Invalid mobile - missing 2+ digits
        # Otherwise, keep as landline if reasonable length
        elif 7 <= len(digits) <= 10:
            landlines.append(num)
    
    return mobiles, landlines


# Invalid email patterns that should be treated as no email
INVALID_EMAIL_PATTERNS = {'na', 'n/a', 'cc', 'tf', 'none', '-', ''}


def _is_invalid_email(email):
    """Check if email is invalid/placeholder."""
    if not email:
        return True
    email_clean = email.strip().lower()
    # Check against known invalid patterns
    if email_clean in INVALID_EMAIL_PATTERNS:
        return True
    # Check if it looks like a valid email (has @ and .)
    if '@' not in email_clean or '.' not in email_clean:
        return True
    # Check for invalid characters (semicolons, spaces, etc.)
    if re.search(r'[;,\s<>()\[\]]', email_clean):
        return True
    # Basic format check: local@domain.tld
    email_match = re.match(r'^[^@]+@[^@]+\.[^@]+$', email_clean)
    if not email_match:
        return True
    # Check domain has valid format (no semicolons, at least 2 chars after last dot)
    domain = email_clean.split('@')[1]
    if not re.match(r'^[a-z0-9.-]+\.[a-z]{2,}$', domain):
        return True
    return False


def _clean_html_content(html_content):
    """Clean HTML content by removing style/class attributes but keeping tags.
    
    Wraps content in <html> tags.
    """
    if not html_content:
        return ''
    
    # Remove style attributes
    html_content = re.sub(r'\s*style="[^"]*"', '', html_content)
    # Remove class attributes
    html_content = re.sub(r'\s*class="[^"]*"', '', html_content)
    # Remove data-* attributes
    html_content = re.sub(r'\s*data-[a-z-]+="[^"]*"', '', html_content)
    # Remove id attributes
    html_content = re.sub(r'\s*id="[^"]*"', '', html_content)
    # Remove border attributes from tables
    html_content = re.sub(r'\s*border="[^"]*"', '', html_content)
    # Clean up empty attribute lists
    html_content = re.sub(r'<(\w+)\s+>', r'<\1>', html_content)
    # Remove &nbsp; and replace with space
    html_content = html_content.replace('&nbsp;', ' ')
    # Clean up multiple spaces
    html_content = re.sub(r' +', ' ', html_content)
    
    # Wrap in <html> tags
    return f'<html>{html_content.strip()}</html>'


def _parse_record_date(date_str):
    """Parse date string like 'Wednesday, 21 January 2026' to 'YYYY-MM-DD'."""
    if not date_str:
        return ''
    
    # Remove day name if present (e.g., "Wednesday, ")
    date_str = re.sub(r'^[A-Za-z]+,\s*', '', date_str.strip())
    
    # Try to parse "21 January 2026" format
    try:
        dt = datetime.strptime(date_str, '%d %B %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    # Try "January 21, 2026" format
    try:
        dt = datetime.strptime(date_str, '%B %d, %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass
    
    return ''


async def extract_medical_records(page, pets_list):
    """Extract medical records from the Notes tab.
    
    Returns: (medical_records, medical_record_entries)
    - medical_records: list of {pet_id, created_by, record_date}
    - medical_record_entries: list of {medical_record_id, entry_type, title, description, created_by}
    """
    medical_records = []
    medical_record_entries = []
    
    try:
        # Click on Notes tab
        notes_tab = page.get_by_role("tab", name="Notes")
        if await notes_tab.count() == 0:
            return medical_records
        
        await notes_tab.click()
        await asyncio.sleep(0.5)  # Wait for notes to load
        
        # Click on "All Notes" tab if available
        all_notes_tab = page.get_by_role("tab", name="All Notes")
        if await all_notes_tab.count() > 0:
            await all_notes_tab.click()
            await asyncio.sleep(0.3)
        
        # Extract notes data using JavaScript
        notes_data = await page.evaluate("""
            () => {
                const results = [];
                const journalList = document.querySelector('.journal-list');
                if (!journalList) return results;
                
                // Find all booking headers
                const bookings = journalList.querySelectorAll('.journal-booking');
                
                bookings.forEach((booking, idx) => {
                    const bookingText = booking.querySelector('span:first-child')?.textContent?.replace('⏺ ', '').trim() || '';
                    const bookingDateEl = booking.querySelector('.journal-booking-date');
                    const bookingDate = bookingDateEl?.textContent?.replace(' - ', '').trim() || '';
                    
                    // Find the journal-day (note creation date)
                    let journalDay = '';
                    let sibling = booking.nextElementSibling;
                    while (sibling) {
                        if (sibling.classList.contains('journal-day')) {
                            journalDay = sibling.textContent.trim();
                            break;
                        }
                        if (sibling.classList.contains('journal-booking')) break;
                        sibling = sibling.nextElementSibling;
                    }
                    
                    // Find all journal entries under this booking
                    const entries = [];
                    sibling = booking.nextElementSibling;
                    while (sibling) {
                        if (sibling.classList.contains('journal-booking')) break;
                        if (sibling.classList.contains('journal-entry')) {
                            const noteBody = sibling.querySelector('.tinymceNoteView, [data-hook="body-text"]');
                            const petNameEl = sibling.querySelector('[class*="person_outline"]')?.parentElement;
                            let petName = '';
                            if (petNameEl) {
                                petName = petNameEl.textContent.replace('person_outline', '').trim();
                            }
                            
                            entries.push({
                                petName: petName,
                                htmlContent: noteBody?.innerHTML || ''
                            });
                        }
                        sibling = sibling.nextElementSibling;
                    }
                    
                    // Parse pet name and title from booking text
                    // Format: "PETNAME, Service1, Service2"
                    const parts = bookingText.split(',').map(p => p.trim());
                    const petNameFromBooking = parts[0] || '';
                    const title = parts.slice(1).join(', ') || '';
                    
                    results.push({
                        bookingText,
                        petNameFromBooking,
                        title,
                        journalDay,
                        entries
                    });
                });
                
                return results;
            }
        """)
        
        # Build a map of pet names to pet IDs
        pet_name_to_id = {}
        for pet in pets_list:
            pet_name = pet.get('pet_name', '').upper()
            if pet_name:
                pet_name_to_id[pet_name] = pet.get('user_id')  # This will be updated later
        
        # Process notes data
        for note_data in notes_data:
            pet_name = note_data.get('petNameFromBooking', '').upper()
            title = note_data.get('title', '')
            record_date = _parse_record_date(note_data.get('journalDay', ''))
            entries = note_data.get('entries', [])
            
            if not entries or not pet_name:
                continue
            
            # We'll assign pet_id and medical_record_id later in the main flow
            for entry in entries:
                html_content = _clean_html_content(entry.get('htmlContent', ''))
                if html_content and html_content != '<html></html>':
                    medical_records.append({
                        'pet_name': pet_name,  # Will be converted to pet_id later
                        'record_date': record_date,
                        'title': title,
                        'description': html_content
                    })
    
    except Exception as e:
        print(f"  Warning: Could not extract medical records: {e}")
    
    return medical_records


def _clean_pet_name(pet_name):
    """Clean pet name by removing parenthetical content and extra whitespace.
    
    This ensures consistent pet name matching between Sub Accounts and Notes tabs.
    """
    if not pet_name:
        return ''
    # Remove parenthetical content like "(Canine)" or "(Dog, Labrador)"
    cleaned = re.sub(r'\s*\([^)]*\)', '', pet_name)
    # Normalize whitespace and strip
    cleaned = ' '.join(cleaned.split()).strip()
    return cleaned


def _format_customer_data(name, phone, email, address, customer_id, kreloses_id, used_usernames, used_emails, noemail_counter):
    """Format customer data from extracted fields."""
    name = (name or '').strip()
    name_parts = name.split() if name else []
    
    first_name = name_parts[0] if name_parts else ''
    last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
    
    # Generate unique username
    base_username = name.lower().replace(' ', '-') if name else f'customer-{customer_id}'
    username = base_username
    suffix = 1
    while username in used_usernames:
        username = f"{base_username}-{suffix}"
        suffix += 1
    used_usernames.add(username)
    
    # Handle empty or invalid email
    email = (email or '').strip()
    if _is_invalid_email(email):
        while f"vpnoemail{noemail_counter[0]}@gmail.com" in used_emails:
            noemail_counter[0] += 1
        email = f"vpnoemail{noemail_counter[0]}@gmail.com"
        noemail_counter[0] += 1
    used_emails.add(email)
    
    # Parse phone numbers - mobiles go to main phone, landlines to additional
    mobiles, landlines = _parse_phone_numbers(phone)
    
    # Main phone: first valid mobile, or empty if none
    phone_1 = mobiles[0] if mobiles else ''
    
    # Additional phones: remaining mobiles first, then landlines
    additional = mobiles[1:] + landlines
    phone_2 = additional[0] if len(additional) > 0 else ''
    phone_3 = additional[1] if len(additional) > 1 else ''
    
    # If no valid mobile but have landline, put landline in additional_phone_1
    if not phone_1 and landlines:
        phone_2 = landlines[0]
        phone_3 = landlines[1] if len(landlines) > 1 else ''
    
    return {
        'id': customer_id,
        'kreloses_id': kreloses_id,
        'first_name': first_name,
        'last_name': last_name,
        'username': username,
        'password': DEFAULT_PASSWORD_HASH,
        'role': 'customer',
        'phone': phone_1,
        'additional_phone_1': phone_2,
        'additional_phone_2': phone_3,
        'email': email,
        'street_address': (address or '').strip()
    }


async def login(page):
    """Login to Kreloses"""
    print("Logging in...")
    await page.goto(LOGIN_URL)
    await page.fill('input[name="Email"], input[placeholder*="Email"], [aria-label="Email"]', EMAIL)
    await page.fill('input[name="Password"], input[placeholder*="Password"], [aria-label="Password"]', PASSWORD)
    await page.click('button:has-text("Log in")')
    await page.wait_for_url("**/sea.kreloses.com/**", timeout=30000)
    print("Login successful!")


async def get_customer_links(page):
    """Get all customer profile links using pagination (Next button)"""
    print("Fetching customer list...")
    await page.goto(CUSTOMER_LIST_URL)
    await page.wait_for_selector('table', timeout=30000)
    await asyncio.sleep(1)  # Wait for initial data to load

    customer_links = {}  # dict preserves insertion order
    page_num = 1

    while True:
        # Get pagination info (e.g., "1-500 of 8199")
        current_page_text = ""
        try:
            footer = await page.query_selector('.table-footer span')
            if footer:
                current_page_text = await footer.inner_text()
                print(f"  Page {page_num}: {current_page_text}")
        except:
            pass

        # Collect all customer links on current page
        links = await page.query_selector_all('a[href*="/customer/overview/"], a[href*="/Customer/Overview/"]')
        for link in links:
            href = await link.get_attribute('href')
            if href:
                href_lower = href.lower()
                if '/customer/overview/' in href_lower:
                    match = re.search(r'/customer/overview/(\d+)', href_lower)
                    if match:
                        customer_id = match.group(1)
                        full_url = f"https://sea.kreloses.com/Customer/Overview/{customer_id}"
                        customer_links[full_url] = True

        print(f"  Collected {len(customer_links)} customers so far...")

        # Check if Next button (#FrontBtn) is available and not disabled
        next_btn = await page.query_selector('#FrontBtn:not(.disabled)')
        if not next_btn:
            print("  No more pages (Next button disabled)")
            break

        # Click Next button to go to next page
        await next_btn.click()
        page_num += 1
        
        # Wait for dynamic content to load - watch for pagination text to change
        try:
            await page.wait_for_function(
                f"""() => {{
                    const span = document.querySelector('.table-footer span');
                    return span && span.textContent !== '{current_page_text}';
                }}""",
                timeout=10000
            )
        except:
            # Fallback: just wait a bit
            await asyncio.sleep(2)
        
        # Additional wait for table rows to render
        await asyncio.sleep(0.5)

    print(f"Found {len(customer_links)} total customers across {page_num} pages")
    return list(customer_links.keys())


async def extract_customer_info(page):
    """Extract customer info (name, phone, email, address) from the current page."""
    customer_info = {'name': '', 'phone': '', 'email': '', 'address': ''}
    
    try:
        # Get customer name from h1
        h1 = await page.query_selector('h1')
        if h1:
            customer_info['name'] = (await h1.inner_text()).strip()
        
        # Extract phone and email from the Details tab (default view)
        info = await page.evaluate("""() => {
            const result = {phone: '', email: ''};
            
            // Find all generic divs that might contain key-value pairs
            const allDivs = document.querySelectorAll('div');
            for (const div of allDivs) {
                const text = (div.textContent || '').trim();
                
                // Look for Phone label
                if (text.startsWith('Phone:')) {
                    const nextSibling = div.nextElementSibling;
                    if (nextSibling) {
                        result.phone = nextSibling.textContent.trim();
                    }
                }
                
                // Look for Email label
                if (text.startsWith('Email:')) {
                    const nextSibling = div.nextElementSibling;
                    if (nextSibling) {
                        result.email = nextSibling.textContent.trim();
                    }
                }
            }
            
            // Alternative: Look for labeled fields in key-value structure
            const keyValuePairs = document.querySelectorAll('[class*="keyvalue"], [class*="value-table"]');
            for (const table of keyValuePairs) {
                const rows = table.querySelectorAll('[class*="rows"], [class*="row"]');
                for (const row of rows) {
                    const cells = row.querySelectorAll('[class*="cells"], [class*="cell"]');
                    if (cells.length >= 2) {
                        const label = (cells[0].textContent || '').trim().toLowerCase();
                        const value = (cells[1].textContent || '').trim();
                        if (label.includes('phone')) result.phone = result.phone || value;
                        if (label.includes('email')) result.email = result.email || value;
                    }
                }
            }
            
            return result;
        }""")
        
        customer_info['phone'] = info.get('phone', '')
        customer_info['email'] = info.get('email', '')
        
        # Now click on the Addresses tab under Basic Info to get the address
        try:
            # Find and click the Addresses tab (it's a nested tab under Basic Info)
            addresses_tab = page.locator('tab:has-text("Addresses"), [role="tab"]:has-text("Addresses")').first
            if await addresses_tab.count() > 0:
                await addresses_tab.click()
                await asyncio.sleep(0.3)  # Wait for tab content to load
                
                # Extract address from the Addresses tab panel
                address_text = await page.evaluate("""() => {
                    // Look for address in the Addresses tabpanel
                    const tabpanel = document.querySelector('[role="tabpanel"][aria-label*="Addresses"], [role="tabpanel"]:has(p)');
                    if (tabpanel) {
                        const paragraph = tabpanel.querySelector('p');
                        if (paragraph) return paragraph.textContent.trim();
                    }
                    
                    // Alternative: Look for any paragraph in the currently visible addresses section
                    const addressParagraphs = document.querySelectorAll('p');
                    for (const p of addressParagraphs) {
                        const text = p.textContent.trim();
                        // Address typically contains location info
                        if (text && text.length > 10 && !text.includes('Loading')) {
                            return text;
                        }
                    }
                    
                    return '';
                }""")
                
                # Flatten to single line - replace newlines with comma+space
                address_text = ' '.join(address_text.split())
                customer_info['address'] = address_text
                
                # Click back to Details tab to restore state
                details_tab = page.locator('tab:has-text("Details"), [role="tab"]:has-text("Details")').first
                if await details_tab.count() > 0:
                    await details_tab.click()
                    await asyncio.sleep(0.1)
        except Exception as e:
            print(f"  Warning: Could not extract address from Addresses tab: {e}")
        
    except Exception as e:
        print(f"  Warning: Could not extract full customer info: {e}")
    
    return customer_info


async def extract_pet_data(page, customer_url, customer_id, kreloses_id, used_usernames, used_emails, noemail_counter):
    """Extract pet data from a customer's Sub Accounts tab"""
    pets = []
    customer_data = None
    raw_medical_records = []

    try:
        await page.goto(customer_url, timeout=30000)
        await page.wait_for_selector('[role="tablist"]', timeout=10000)

        # Extract customer information first
        customer_info = await extract_customer_info(page)
        customer_data = _format_customer_data(
            customer_info['name'],
            customer_info['phone'],
            customer_info['email'],
            customer_info['address'],
            customer_id,
            kreloses_id,
            used_usernames,
            used_emails,
            noemail_counter
        )

        # Click on Sub Accounts tab
        sub_accounts_tab = page.get_by_role("tab", name="Sub Accounts")
        if await sub_accounts_tab.count() == 0:
            print(f"  No Sub Accounts tab for {customer_url}")
            return customer_data, pets, raw_medical_records

        await sub_accounts_tab.click()
        panel = page.get_by_role("tabpanel", name="Sub Accounts")
        await panel.wait_for(state="visible", timeout=10000)
        await asyncio.sleep(0.5)  # Wait for content to load

        # Find all pet cards (white-card collapsible elements)
        pet_cards = panel.locator('div.white-card.collapsible')
        card_count = await pet_cards.count()
        
        if card_count == 0:
            # Try alternative selectors
            alt_cards = panel.locator('.sub-account-entry-container .white-card')
            alt_count = await alt_cards.count()
            if alt_count > 0:
                pet_cards = alt_cards
                card_count = alt_count
            else:
                print(f"  No pets found for {customer_data['first_name']} {customer_data['last_name']}")
                # Still try to extract medical records even if no pets in Sub Accounts
                raw_medical_records = await extract_medical_records(page, pets)
                return customer_data, pets, raw_medical_records

        # First, expand ALL collapsed cards to load their content
        for i in range(card_count):
            card = pet_cards.nth(i)
            try:
                # Check if card is collapsed (has expand_more icon, not expand_less)
                heading = card.locator('h2').first
                heading_text = await heading.inner_text()
                if 'expand_more' in heading_text.lower():
                    # Click the expand button to open the card
                    expand_btn = card.locator('.white-card-expand-btn')
                    if await expand_btn.count() > 0:
                        await expand_btn.click()
                        await asyncio.sleep(0.3)  # Wait for expansion animation
            except Exception:
                pass

        # Wait for all content to load after expansion
        await asyncio.sleep(0.5)

        # Re-query cards after expansion (DOM may have changed)
        pet_cards = panel.locator('div.white-card.collapsible')
        card_count = await pet_cards.count()

        for i in range(card_count):
            card = pet_cards.nth(i)
            try:
                # Get pet name from h2 heading
                heading = card.locator('h2').first
                heading_text = (await heading.inner_text()).strip()
                
                if not heading_text:
                    continue

                # Clean up heading text (remove expand_less/expand_more icons)
                pet_name = re.sub(r'^expand_(?:less|more)\s*', '', heading_text, flags=re.I)
                pet_name = re.sub(r'\s+', ' ', pet_name).strip()
                
                # Apply consistent pet name cleaning
                pet_name = _clean_pet_name(pet_name)

                if not pet_name:
                    continue

                pet_data = {
                    'user_id': customer_id,
                    'pet_name': pet_name,
                    'species': '',
                    'breed': '',
                    'spayed': '',
                    'birthdate': '',
                    'color': ''
                }

                # Check if pet name has format "Name (Species, Breed)"
                match = re.search(r'([^(]+)\s*\(([^,]+),\s*([^)]+)\)', pet_name)
                if match:
                    pet_data['pet_name'] = match.group(1).strip()
                    pet_data['species'] = match.group(2).strip()
                    pet_data['breed'] = match.group(3).strip()

                # Extract field values from the card's keyvalue tables
                fields = await _extract_pet_fields_from_card(card)
                
                # Update pet_data with extracted fields (only if not empty)
                if fields.get('species'):
                    pet_data['species'] = fields['species']
                if fields.get('breed'):
                    pet_data['breed'] = fields['breed']
                if fields.get('neutered'):
                    pet_data['spayed'] = fields['neutered']
                if fields.get('birthdate'):
                    pet_data['birthdate'] = fields['birthdate']
                if fields.get('color'):
                    pet_data['color'] = fields['color']

                # Format/clean the extracted data
                pet_data = _format_pet_data(pet_data)

                # Only save if we have at least a pet name
                if pet_data['pet_name']:
                    pets.append(pet_data)
                    species_breed = f"{pet_data['species']}, {pet_data['breed']}" if pet_data['species'] else "Unknown"
                    print(f"  Found pet: {pet_data['pet_name']} ({species_breed})")

            except Exception as e:
                print(f"  Error processing pet {i+1}: {e}")
                continue
        
        # Extract medical records from Notes tab
        raw_medical_records = await extract_medical_records(page, pets)
        if raw_medical_records:
            print(f"  Found {len(raw_medical_records)} medical record entries")

    except Exception as e:
        print(f"Error extracting pets from {customer_url}: {e}")

    return customer_data, pets, raw_medical_records


async def scrape_all_data():
    """Main scraping function with incremental saving"""
    all_pets = []
    all_customers = []
    all_medical_records = []
    all_medical_record_entries = []
    processed_customer_urls = set()
    failed_customers = []
    next_customer_id = CUSTOMER_ID_START
    next_pet_id = PET_ID_START
    next_medical_record_id = MEDICAL_RECORD_ID_START
    
    # Track used usernames and emails for uniqueness
    used_usernames = set()
    used_emails = set()
    noemail_counter = [1]  # Use list to allow modification in nested function
    
    # Track processed Kreloses IDs to prevent duplicates on resume
    processed_kreloses_ids = set()
    
    # Get working directory for temp file recovery
    working_dir = os.path.dirname(os.path.abspath(CUSTOMERS_OUTPUT_FILE)) or '.'
    
    # =========================================================================
    # ACQUIRE LOCK - Prevent concurrent scraper runs
    # =========================================================================
    print("Acquiring lock...")
    lock_acquired, lock_result = _acquire_lock(LOCK_FILE)
    if not lock_acquired:
        print(f"  ERROR: {lock_result}")
        print("  Exiting to prevent data corruption.")
        return [], [], [], []
    print(f"  Lock acquired (PID {os.getpid()})")
    
    try:
        # Check for and clean up orphaned temp files from previous crashes
        print("Checking for orphaned temp files...")
        orphaned_temps = _recover_temp_files(working_dir)
        if orphaned_temps:
            print(f"  Found {len(orphaned_temps)} orphaned temp files - will attempt recovery")
        _cleanup_temp_files(working_dir)
        
        # =========================================================================
        # REPAIR CSV FILES - Fix any truncated rows from previous crashes
        # =========================================================================
        print("Checking CSV files for corruption...")
        for csv_file in [CUSTOMERS_OUTPUT_FILE, PETS_OUTPUT_FILE, MEDICAL_RECORDS_OUTPUT_FILE, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE]:
            if os.path.exists(csv_file):
                was_repaired, rows_removed, error = _repair_csv_if_needed(csv_file)
                if was_repaired:
                    print(f"  Repaired {csv_file}: removed {rows_removed} truncated row(s)")
                elif error:
                    print(f"  Warning: Could not check {csv_file}: {error}")
        
        # =========================================================================
        # LOAD PROGRESS FILE WITH MANIFEST
        # =========================================================================
        processed_customer_urls, manifest = _load_progress_with_manifest(PROGRESS_FILE)
        if processed_customer_urls:
            print(f"Resuming from previous session: {len(processed_customer_urls)} customers already processed")
            if manifest:
                print(f"  Last save: {manifest.get('timestamp', 'unknown')}")
                print(f"  Manifest counts - customers: {manifest.get('customers', '?')}, pets: {manifest.get('pets', '?')}")
        
        # Load existing data using safe reader (handles encoding properly)
        print("Loading existing data...")
        
        # Load pets
        all_pets, pets_error = _read_csv_safe(PETS_OUTPUT_FILE)
        if pets_error:
            print(f"  Warning: {pets_error}")
        elif all_pets:
            # Fix: ensure 'id' key exists (handle BOM issue)
            first_row_keys = list(all_pets[0].keys()) if all_pets else []
            id_key = 'id' if 'id' in first_row_keys else (first_row_keys[0] if first_row_keys else 'id')
            max_pet_id = max(int(p.get(id_key, p.get('id', PET_ID_START))) for p in all_pets)
            next_pet_id = max_pet_id + 1
            print(f"  Loaded {len(all_pets)} existing pets from {PETS_OUTPUT_FILE}")
        
        # Load customers
        all_customers, customers_error = _read_csv_safe(CUSTOMERS_OUTPUT_FILE)
        if customers_error:
            print(f"  Warning: {customers_error}")
        elif all_customers:
            # Fix: handle potential BOM in first column
            first_row_keys = list(all_customers[0].keys()) if all_customers else []
            id_key = 'id' if 'id' in first_row_keys else (first_row_keys[0] if first_row_keys else 'id')
            
            max_id = max(int(c.get(id_key, c.get('id', CUSTOMER_ID_START))) for c in all_customers)
            next_customer_id = max_id + 1
            
            # Rebuild used sets from existing data
            for c in all_customers:
                if c.get('username'):
                    used_usernames.add(c['username'])
                if c.get('email'):
                    used_emails.add(c['email'])
                    # Track noemail counter
                    email = c['email']
                    if email.startswith('vpnoemail') and email.endswith('@gmail.com'):
                        try:
                            num = int(email.replace('vpnoemail', '').replace('@gmail.com', ''))
                            noemail_counter[0] = max(noemail_counter[0], num + 1)
                        except:
                            pass
                # Track processed Kreloses IDs to prevent duplicates
                if c.get('kreloses_id'):
                    processed_kreloses_ids.add(c['kreloses_id'])
            print(f"  Loaded {len(all_customers)} existing customers from {CUSTOMERS_OUTPUT_FILE}")
        
        # Load medical records
        all_medical_records, mr_error = _read_csv_safe(MEDICAL_RECORDS_OUTPUT_FILE)
        if mr_error:
            print(f"  Warning: {mr_error}")
        elif all_medical_records:
            first_row_keys = list(all_medical_records[0].keys()) if all_medical_records else []
            id_key = 'id' if 'id' in first_row_keys else (first_row_keys[0] if first_row_keys else 'id')
            max_id = max(int(r.get(id_key, r.get('id', MEDICAL_RECORD_ID_START))) for r in all_medical_records)
            next_medical_record_id = max_id + 1
            print(f"  Loaded {len(all_medical_records)} existing medical records")
        
        # Load medical record entries
        all_medical_record_entries, mre_error = _read_csv_safe(MEDICAL_RECORD_ENTRIES_OUTPUT_FILE)
        if mre_error:
            print(f"  Warning: {mre_error}")
        elif all_medical_record_entries:
            print(f"  Loaded {len(all_medical_record_entries)} existing medical record entries")
        
        # CRITICAL: Validate consistency between progress file and actual data
        # Use CSV data (kreloses_ids) as the source of truth, NOT progress file
        if processed_customer_urls and not all_customers:
            print()
            print("  " + "=" * 56)
            print("  WARNING: Data inconsistency detected!")
            print(f"  Progress file says {len(processed_customer_urls)} URLs processed,")
            print(f"  but customers.csv has 0 customers.")
            print()
            print("  This likely means data was lost due to interruption.")
            print("  Options:")
            print("    1. Check for backup file: customers.csv.backup")
            print("    2. Re-scrape from scratch (delete scraper_progress.txt)")
            print("  " + "=" * 56)
            print()
            
            # Check for backup
            backup_file = CUSTOMERS_OUTPUT_FILE + '.backup'
            if os.path.exists(backup_file):
                backup_customers, _ = _read_csv_safe(backup_file)
                if backup_customers:
                    print(f"  Found backup with {len(backup_customers)} customers!")
                    response = input("  Restore from backup? (y/n): ").strip().lower()
                    if response == 'y':
                        all_customers = backup_customers
                        # Rebuild tracking data from backup
                        for c in all_customers:
                            if c.get('username'):
                                used_usernames.add(c['username'])
                            if c.get('email'):
                                used_emails.add(c['email'])
                            if c.get('kreloses_id'):
                                processed_kreloses_ids.add(c['kreloses_id'])
                        
                        first_row_keys = list(all_customers[0].keys()) if all_customers else []
                        id_key = 'id' if 'id' in first_row_keys else (first_row_keys[0] if first_row_keys else 'id')
                        max_id = max(int(c.get(id_key, c.get('id', CUSTOMER_ID_START))) for c in all_customers)
                        next_customer_id = max_id + 1
                        print(f"  Restored {len(all_customers)} customers from backup.")
            
            # If still no customers, offer to reset progress
            if not all_customers:
                response = input("  Reset progress and start fresh? (y/n): ").strip().lower()
                if response == 'y':
                    processed_customer_urls.clear()
                    print("  Progress reset. Will re-scrape all customers.")
                else:
                    print("  Continuing with inconsistent state (may skip already-processed URLs).")
        
        # Also warn if there's a significant mismatch
        elif processed_customer_urls and all_customers:
            url_count = len(processed_customer_urls)
            customer_count = len(all_customers)
            if customer_count < url_count * 0.8:  # More than 20% missing
                print()
                print(f"  Warning: Progress file has {url_count} URLs but only {customer_count} customers in CSV.")
                print(f"  Some data may have been lost. Using kreloses_ids from CSV as source of truth.")
                print()
        
        # Track customers processed in current batch (defined here for finally block access)
        pending_urls = []
        pending_customers = []
        pending_pets = []
        pending_medical_records = []
        pending_medical_record_entries = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)  # Set to True for production
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Login
                await login(page)
                
                # Get all customer links
                customer_links = await get_customer_links(page)
                
                # Filter out already processed customers
                remaining_links = [url for url in customer_links if url not in processed_customer_urls]
                print(f"Remaining customers to process: {len(remaining_links)}")
                
                # Extract data from each customer
                total = len(remaining_links)
                for i, customer_url in enumerate(remaining_links, 1):
                    # Extract Kreloses customer ID from URL
                    kreloses_id_match = re.search(r'/customer/overview/(\d+)', customer_url.lower())
                    kreloses_id = kreloses_id_match.group(1) if kreloses_id_match else ''
                    
                    # Skip if already processed (prevents duplicates on resume)
                    if kreloses_id and kreloses_id in processed_kreloses_ids:
                        print(f"Skipping customer {i}/{total}: {customer_url} (already in CSV)")
                        continue
                    
                    print(f"Processing customer {i}/{total}: {customer_url}")
                    
                    try:
                        customer_data, pets, raw_medical_records = await extract_pet_data(page, customer_url, next_customer_id, kreloses_id, used_usernames, used_emails, noemail_counter)
                        
                        if customer_data:
                            # Track Kreloses ID to prevent duplicates
                            if kreloses_id:
                                processed_kreloses_ids.add(kreloses_id)
                            pending_customers.append(customer_data)
                            all_customers.append(customer_data)
                            next_customer_id += 1
                        
                        # Assign IDs to pets and build name-to-id mapping
                        pet_name_to_id = {}
                        for pet in pets:
                            pet['id'] = next_pet_id
                            # Use cleaned and uppercased pet name for consistent matching
                            pet_name = _clean_pet_name(pet.get('pet_name', '')).upper()
                            if pet_name:
                                pet_name_to_id[pet_name] = next_pet_id
                            next_pet_id += 1
                        
                        pending_pets.extend(pets)
                        all_pets.extend(pets)
                        
                        # Process raw medical records - group by (pet_id, record_date)
                        # One medical_record per unique (pet_id, record_date), multiple entries can share it
                        medical_record_map = {}  # (pet_id, record_date) -> medical_record_id
                        
                        for raw_record in raw_medical_records:
                            # Apply same cleaning to pet name from Notes tab for consistent matching
                            pet_name = _clean_pet_name(raw_record.get('pet_name', '')).upper()
                            pet_id = pet_name_to_id.get(pet_name, '')
                            record_date = raw_record.get('record_date', '')
                            
                            # Skip if we can't find the pet
                            if not pet_id:
                                continue
                            
                            # Check if we already have a medical_record for this pet+date
                            key = (pet_id, record_date)
                            if key not in medical_record_map:
                                # Create new medical_record
                                medical_record = {
                                    'id': next_medical_record_id,
                                    'pet_id': pet_id,
                                    'created_by': 1,
                                    'record_date': record_date
                                }
                                medical_record_map[key] = next_medical_record_id
                                pending_medical_records.append(medical_record)
                                all_medical_records.append(medical_record)
                                next_medical_record_id += 1
                            
                            # Create medical_record_entry (always, referencing the existing medical_record)
                            medical_record_entry = {
                                'medical_record_id': medical_record_map[key],
                                'entry_type': 'NOTE',
                                'title': raw_record.get('title', ''),
                                'description': raw_record.get('description', ''),
                                'created_by': 1
                            }
                            
                            pending_medical_record_entries.append(medical_record_entry)
                            all_medical_record_entries.append(medical_record_entry)
                        
                        # Add URL to pending
                        pending_urls.append(customer_url)
                        
                        # Save progress every 10 customers
                        if i % 10 == 0 or i == total:
                            # TWO-PHASE COMMIT: Save all CSVs atomically
                            row_counts = save_all_csvs_atomic(
                                all_customers, all_pets, all_medical_records, all_medical_record_entries,
                                CUSTOMERS_OUTPUT_FILE, PETS_OUTPUT_FILE, 
                                MEDICAL_RECORDS_OUTPUT_FILE, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE
                            )
                            
                            # Only THEN mark customers as processed (ensures consistency)
                            processed_customer_urls.update(pending_urls)
                            _save_progress_with_manifest(PROGRESS_FILE, processed_customer_urls, row_counts)
                            
                            print(f"  Progress saved: {len(all_customers)} customers, {len(all_pets)} pets, {len(all_medical_records)} medical records")
                            pending_urls.clear()
                            pending_customers.clear()
                            pending_pets.clear()
                            pending_medical_records.clear()
                            pending_medical_record_entries.clear()
                        
                    except Exception as e:
                        print(f"  Error processing {customer_url}: {e}")
                        failed_customers.append(customer_url)
                    
                    # Small delay to avoid overwhelming the server
                    await asyncio.sleep(0.5)
                
            finally:
                # Save any pending work before closing (handles Ctrl+C or errors)
                if pending_urls:
                    print(f"\n  Saving {len(pending_urls)} pending customers before exit...")
                    try:
                        row_counts = save_all_csvs_atomic(
                            all_customers, all_pets, all_medical_records, all_medical_record_entries,
                            CUSTOMERS_OUTPUT_FILE, PETS_OUTPUT_FILE,
                            MEDICAL_RECORDS_OUTPUT_FILE, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE
                        )
                        processed_customer_urls.update(pending_urls)
                        _save_progress_with_manifest(PROGRESS_FILE, processed_customer_urls, row_counts)
                    except Exception as e:
                        print(f"  Warning: Failed to save pending work: {e}")
                
                await browser.close()
        
        # Final save with two-phase commit
        if all_customers or all_pets or all_medical_records or all_medical_record_entries:
            row_counts = save_all_csvs_atomic(
                all_customers, all_pets, all_medical_records, all_medical_record_entries,
                CUSTOMERS_OUTPUT_FILE, PETS_OUTPUT_FILE,
                MEDICAL_RECORDS_OUTPUT_FILE, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE
            )
        
        # Validate cross-file consistency
        if all_customers and all_pets:
            is_consistent, errors = _validate_cross_file_consistency(
                all_customers, all_pets, all_medical_records, all_medical_record_entries
            )
            if not is_consistent:
                print("\nWarning: Cross-file consistency issues detected:")
                for err in errors:
                    print(f"  - {err}")
        
        # Clean up progress file on success
        if os.path.exists(PROGRESS_FILE) and not failed_customers:
            os.remove(PROGRESS_FILE)
        
        # Report failed customers
        if failed_customers:
            print(f"\nWarning: {len(failed_customers)} customers failed to process:")
            for url in failed_customers[:10]:  # Show first 10
                print(f"  - {url}")
            if len(failed_customers) > 10:
                print(f"  ... and {len(failed_customers) - 10} more")
        
        return all_customers, all_pets, all_medical_records, all_medical_record_entries
    
    finally:
        # ALWAYS release the lock
        _release_lock(LOCK_FILE)


def _recover_temp_files(target_dir):
    """Check for orphaned temp files and recover if they contain valid data.
    
    Returns dict of recovered files: {original_filename: recovered_data}
    """
    recovered = {}
    temp_pattern = os.path.join(target_dir, '*.csv.tmp')
    
    for temp_path in glob.glob(temp_pattern):
        try:
            # Try to read the temp file
            with open(temp_path, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if rows:
                # Infer original filename from temp file name
                # e.g., tmpXXXX.csv.tmp -> check which CSV it might belong to
                print(f"  Found orphaned temp file with {len(rows)} rows: {temp_path}")
                recovered[temp_path] = rows
        except Exception as e:
            print(f"  Warning: Could not read temp file {temp_path}: {e}")
        
    return recovered


def _cleanup_temp_files(target_dir):
    """Remove any orphaned temp files."""
    temp_pattern = os.path.join(target_dir, '*.csv.tmp')
    for temp_path in glob.glob(temp_pattern):
        try:
            os.unlink(temp_path)
            print(f"  Cleaned up temp file: {temp_path}")
        except:
            pass


# =============================================================================
# FILE LOCKING - Prevent concurrent scraper runs
# =============================================================================

def _acquire_lock(lock_file):
    """Acquire an exclusive lock to prevent concurrent scraper runs.
    
    Returns (success, lock_handle_or_error_message).
    On Windows, uses file creation + PID check.
    """
    if os.path.exists(lock_file):
        # Check if the process that created the lock is still running
        try:
            with open(lock_file, 'r') as f:
                lock_data = json.load(f)
            old_pid = lock_data.get('pid')
            if old_pid:
                # Check if process is still running
                if _is_process_running(old_pid):
                    return False, f"Another scraper instance is running (PID {old_pid}). Lock file: {lock_file}"
                else:
                    # Stale lock - process no longer running
                    print(f"  Removing stale lock file (PID {old_pid} no longer running)")
                    os.unlink(lock_file)
        except (json.JSONDecodeError, KeyError, OSError):
            # Corrupted lock file, remove it
            try:
                os.unlink(lock_file)
            except:
                pass
    
    # Create lock file with our PID
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
        # On Windows, we can use os.kill with signal 0
        # This doesn't actually kill the process, just checks if it exists
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


# =============================================================================
# PARTIAL ROW DETECTION AND REPAIR
# =============================================================================

def _repair_csv_if_needed(filepath):
    """Detect and repair truncated last row in a CSV file.
    
    Returns (was_repaired, rows_removed, error_message).
    If the last line is incomplete (no newline, fewer fields), removes it.
    """
    if not os.path.exists(filepath):
        return False, 0, None
    
    try:
        # Read raw bytes to check for incomplete last line
        with open(filepath, 'rb') as f:
            content = f.read()
        
        if not content:
            return False, 0, None
        
        # Check if file ends with newline
        ends_with_newline = content.endswith(b'\n') or content.endswith(b'\r\n')
        
        if ends_with_newline:
            # File properly terminated, but still check for field count issues
            pass
        
        # Try to parse and validate
        rows_before = 0
        valid_rows = []
        fieldnames = None
        
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
                fieldnames = header
                expected_field_count = len(header)
                
                for row in reader:
                    rows_before += 1
                    # Check if row has correct number of fields
                    if len(row) == expected_field_count:
                        valid_rows.append(row)
                    elif len(row) < expected_field_count and rows_before == sum(1 for _ in open(filepath)) - 1:
                        # Last row with fewer fields - likely truncated
                        print(f"  Detected truncated last row in {filepath}: {row}")
                        continue  # Skip this row
                    else:
                        valid_rows.append(row)  # Keep rows with extra fields (might be quoted commas)
            except csv.Error as e:
                return False, 0, f"CSV parse error: {e}"
        
        rows_removed = rows_before - len(valid_rows)
        
        if rows_removed > 0:
            # Rewrite file without the truncated row(s)
            _create_backup(filepath)
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(fieldnames)
                writer.writerows(valid_rows)
            return True, rows_removed, None
        
        return False, 0, None
        
    except Exception as e:
        return False, 0, f"Repair check failed: {e}"


# =============================================================================
# CROSS-FILE CONSISTENCY VALIDATION
# =============================================================================

def _validate_cross_file_consistency(customers, pets, medical_records, medical_record_entries):
    """Validate referential integrity between CSV data.
    
    Returns (is_valid, errors_list).
    Checks:
    - All pets reference valid customer IDs
    - All medical_records reference valid pet IDs
    - All medical_record_entries reference valid medical_record IDs
    """
    errors = []
    
    # Build ID sets
    customer_ids = {c.get('id') or c.get('\ufeffid') for c in customers}
    pet_ids = {p.get('id') or p.get('\ufeffid') for p in pets}
    medical_record_ids = {r.get('id') or r.get('\ufeffid') for r in medical_records}
    
    # Check pets -> customers (via user_id)
    for pet in pets:
        user_id = pet.get('user_id')
        if user_id and str(user_id) not in {str(cid) for cid in customer_ids}:
            errors.append(f"Pet {pet.get('id')} references non-existent customer {user_id}")
    
    # Check medical_records -> pets
    for record in medical_records:
        pet_id = record.get('pet_id')
        if pet_id and str(pet_id) not in {str(pid) for pid in pet_ids}:
            errors.append(f"Medical record {record.get('id')} references non-existent pet {pet_id}")
    
    # Check medical_record_entries -> medical_records
    for entry in medical_record_entries:
        mr_id = entry.get('medical_record_id')
        if mr_id and str(mr_id) not in {str(rid) for rid in medical_record_ids}:
            errors.append(f"Medical record entry references non-existent medical_record {mr_id}")
    
    # Limit error output
    if len(errors) > 10:
        errors = errors[:10] + [f"... and {len(errors) - 10} more errors"]
    
    return len(errors) == 0, errors


def _create_backup(filename):
    """Create a backup of the file before overwriting."""
    if os.path.exists(filename):
        backup_path = filename + '.backup'
        try:
            shutil.copy2(filename, backup_path)
        except Exception as e:
            print(f"  Warning: Could not create backup of {filename}: {e}")


def _read_csv_safe(filename, encoding='utf-8-sig'):
    """Safely read a CSV file with proper encoding and error handling.
    
    Returns (rows, error_message). If successful, error_message is None.
    """
    if not os.path.exists(filename):
        return [], None
    
    try:
        with open(filename, 'r', newline='', encoding=encoding) as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
        return rows, None
    except Exception as e:
        # Try fallback encoding
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
            return rows, None
        except Exception as e2:
            return [], f"Could not read {filename}: {e2}"


def _validate_csv_file(filepath, expected_row_count, fieldnames):
    """Validate that a CSV file is readable and has expected data.
    
    Returns (is_valid, actual_row_count, error_message)
    """
    try:
        with open(filepath, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Check row count matches
            if len(rows) != expected_row_count:
                return False, len(rows), f"Row count mismatch: expected {expected_row_count}, got {len(rows)}"
            
            # Check that all expected fields exist (at least in header)
            if rows:
                actual_fields = set(rows[0].keys())
                expected_fields = set(fieldnames)
                if not expected_fields.issubset(actual_fields):
                    missing = expected_fields - actual_fields
                    return False, len(rows), f"Missing fields: {missing}"
            
            return True, len(rows), None
    except Exception as e:
        return False, 0, f"Read error: {e}"


def _atomic_csv_write(filename, fieldnames, rows):
    """Atomically write CSV data - prevents data loss on Ctrl+C.
    
    Writes to a temp file first, validates it, then replaces the original.
    If interrupted or validation fails, original stays intact.
    Also creates a backup before replacing.
    """
    if not rows:
        return
    
    # Get the directory of the target file (temp file must be on same filesystem for atomic rename)
    target_dir = os.path.dirname(os.path.abspath(filename)) or '.'
    
    # Create backup of existing file BEFORE any changes
    _create_backup(filename)
    
    # Create temp file in same directory with identifiable name
    base_name = os.path.basename(filename)
    fd, temp_path = tempfile.mkstemp(suffix=f'.{base_name}.tmp', dir=target_dir)
    try:
        with os.fdopen(fd, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            # Force flush to disk
            csvfile.flush()
            os.fsync(csvfile.fileno())
        
        # CRITICAL: Validate temp file before replacing original
        is_valid, actual_count, error = _validate_csv_file(temp_path, len(rows), fieldnames)
        
        if not is_valid:
            raise ValueError(f"Temp file validation failed: {error}")
        
        # Atomic replace - only happens if validation passed
        os.replace(temp_path, filename)
        
    except Exception as e:
        # Clean up temp file on any error
        try:
            os.unlink(temp_path)
        except:
            pass
        # Re-raise with context
        raise RuntimeError(f"Failed to save {filename}: {e}. Original file preserved.") from e


# =============================================================================
# ATOMIC TEXT WRITE WITH MANIFEST (for progress file)
# =============================================================================

def _atomic_text_write(filename, content):
    """Atomically write text content to a file.
    
    Writes to temp file, fsyncs, then atomically replaces original.
    """
    target_dir = os.path.dirname(os.path.abspath(filename)) or '.'
    base_name = os.path.basename(filename)
    fd, temp_path = tempfile.mkstemp(suffix=f'.{base_name}.tmp', dir=target_dir)
    
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        
        # Atomic replace
        os.replace(temp_path, filename)
    except Exception as e:
        try:
            os.unlink(temp_path)
        except:
            pass
        raise RuntimeError(f"Failed to save {filename}: {e}") from e


def _save_progress_with_manifest(progress_file, processed_urls, row_counts):
    """Save progress file atomically with embedded manifest.
    
    The manifest includes timestamp and row counts for consistency verification.
    Format:
      # MANIFEST: {"timestamp": "...", "customers": N, "pets": N, ...}
      url1
      url2
      ...
    """
    manifest = {
        'timestamp': datetime.now().isoformat(),
        'customers': row_counts.get('customers', 0),
        'pets': row_counts.get('pets', 0),
        'medical_records': row_counts.get('medical_records', 0),
        'medical_record_entries': row_counts.get('medical_record_entries', 0),
        'url_count': len(processed_urls)
    }
    
    lines = [f"# MANIFEST: {json.dumps(manifest)}"]
    lines.extend(sorted(processed_urls))  # Sort for deterministic output
    content = '\n'.join(lines)
    
    _atomic_text_write(progress_file, content)


def _load_progress_with_manifest(progress_file):
    """Load progress file and extract manifest if present.
    
    Returns (processed_urls_set, manifest_dict_or_None).
    """
    if not os.path.exists(progress_file):
        return set(), None
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            lines = f.read().strip().split('\n')
    except Exception:
        return set(), None
    
    manifest = None
    urls = set()
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('# MANIFEST:'):
            try:
                manifest_json = line[len('# MANIFEST:'):].strip()
                manifest = json.loads(manifest_json)
            except json.JSONDecodeError:
                pass
        elif not line.startswith('#'):
            urls.add(line)
    
    return urls, manifest


# =============================================================================
# TWO-PHASE COMMIT FOR MULTI-CSV SAVES
# =============================================================================

def _prepare_csv_temp(filename, fieldnames, rows, target_dir):
    """Phase 1: Write CSV to temp file and validate.
    
    Returns (temp_path, row_count) on success, raises on failure.
    """
    if not rows:
        return None, 0
    
    base_name = os.path.basename(filename)
    fd, temp_path = tempfile.mkstemp(suffix=f'.{base_name}.tmp', dir=target_dir)
    
    try:
        with os.fdopen(fd, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            csvfile.flush()
            os.fsync(csvfile.fileno())
        
        # Validate
        is_valid, actual_count, error = _validate_csv_file(temp_path, len(rows), fieldnames)
        if not is_valid:
            raise ValueError(f"Validation failed for {filename}: {error}")
        
        return temp_path, len(rows)
    except Exception:
        try:
            os.unlink(temp_path)
        except:
            pass
        raise


def _commit_temps(temp_to_final_map):
    """Phase 2: Atomically replace all original files with temps.
    
    Only called after ALL temps are validated.
    """
    for temp_path, final_path in temp_to_final_map.items():
        if temp_path:  # Skip None entries (empty data)
            os.replace(temp_path, final_path)


def _cleanup_temps(temp_paths):
    """Clean up temp files on failure."""
    for temp_path in temp_paths:
        if temp_path:
            try:
                os.unlink(temp_path)
            except:
                pass


def save_all_csvs_atomic(customers, pets, medical_records, medical_record_entries,
                         customers_file, pets_file, medical_records_file, 
                         medical_record_entries_file):
    """Two-phase commit: save all CSVs atomically or none.
    
    Phase 1: Write all to temp files, validate each
    Phase 2: Replace all originals with temps (only if Phase 1 succeeds)
    
    Returns dict of row counts on success.
    Raises RuntimeError on failure (all originals preserved).
    """
    target_dir = os.path.dirname(os.path.abspath(customers_file)) or '.'
    
    # Create backups first
    for f in [customers_file, pets_file, medical_records_file, medical_record_entries_file]:
        _create_backup(f)
    
    # Field definitions
    customers_fields = ['id', 'kreloses_id', 'first_name', 'last_name', 'username', 'password', 'role', 'phone', 'additional_phone_1', 'additional_phone_2', 'email', 'street_address']
    pets_fields = ['id', 'user_id', 'pet_name', 'species', 'breed', 'spayed', 'birthdate', 'color']
    medical_records_fields = ['id', 'pet_id', 'created_by', 'record_date']
    medical_record_entries_fields = ['medical_record_id', 'entry_type', 'title', 'description', 'created_by']
    
    temp_paths = []
    temp_to_final = {}
    row_counts = {}
    
    try:
        # Phase 1: Prepare all temps
        temp_customers, count = _prepare_csv_temp(customers_file, customers_fields, customers, target_dir)
        temp_paths.append(temp_customers)
        if temp_customers:
            temp_to_final[temp_customers] = customers_file
        row_counts['customers'] = count
        
        temp_pets, count = _prepare_csv_temp(pets_file, pets_fields, pets, target_dir)
        temp_paths.append(temp_pets)
        if temp_pets:
            temp_to_final[temp_pets] = pets_file
        row_counts['pets'] = count
        
        temp_mr, count = _prepare_csv_temp(medical_records_file, medical_records_fields, medical_records, target_dir)
        temp_paths.append(temp_mr)
        if temp_mr:
            temp_to_final[temp_mr] = medical_records_file
        row_counts['medical_records'] = count
        
        temp_mre, count = _prepare_csv_temp(medical_record_entries_file, medical_record_entries_fields, medical_record_entries, target_dir)
        temp_paths.append(temp_mre)
        if temp_mre:
            temp_to_final[temp_mre] = medical_record_entries_file
        row_counts['medical_record_entries'] = count
        
        # Phase 2: Commit all
        _commit_temps(temp_to_final)
        
        return row_counts
        
    except Exception as e:
        # Rollback: clean up any temp files
        _cleanup_temps(temp_paths)
        raise RuntimeError(f"Failed to save CSVs (all originals preserved): {e}") from e


def save_pets_csv(pets, filename):
    """Save pet data to CSV file with UTF-8 BOM encoding (atomic write)"""
    fieldnames = ['id', 'user_id', 'pet_name', 'species', 'breed', 'spayed', 'birthdate', 'color']
    _atomic_csv_write(filename, fieldnames, pets)


def save_customers_csv(customers, filename):
    """Save customer data to CSV file with UTF-8 BOM encoding (atomic write)"""
    fieldnames = ['id', 'kreloses_id', 'first_name', 'last_name', 'username', 'password', 'role', 'phone', 'additional_phone_1', 'additional_phone_2', 'email', 'street_address']
    _atomic_csv_write(filename, fieldnames, customers)


def save_medical_records_csv(records, filename):
    """Save medical records to CSV file with UTF-8 BOM encoding (atomic write)"""
    fieldnames = ['id', 'pet_id', 'created_by', 'record_date']
    _atomic_csv_write(filename, fieldnames, records)


def save_medical_record_entries_csv(entries, filename):
    """Save medical record entries to CSV file with UTF-8 BOM encoding (atomic write)"""
    fieldnames = ['medical_record_id', 'entry_type', 'title', 'description', 'created_by']
    _atomic_csv_write(filename, fieldnames, entries)


async def main():
    """Main entry point"""
    print("=" * 60)
    print("Kreloses Customer & Pet Data Scraper")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    customers, pets, medical_records, medical_record_entries = await scrape_all_data()
    
    print()
    print(f"Total: {len(customers)} customers, {len(pets)} pets, {len(medical_records)} medical records, {len(medical_record_entries)} entries")
    print(f"Output files: {CUSTOMERS_OUTPUT_FILE}, {PETS_OUTPUT_FILE}, {MEDICAL_RECORDS_OUTPUT_FILE}, {MEDICAL_RECORD_ENTRIES_OUTPUT_FILE}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
