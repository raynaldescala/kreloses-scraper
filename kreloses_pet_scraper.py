"""
Kreloses Pet Data Scraper
Extracts pet information from the Kreloses clinic management system and exports to CSV.
"""

import asyncio
import csv
import os
import re
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


def _format_customer_data(name, phone, email, address, customer_id, used_usernames, used_emails, noemail_counter):
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


async def extract_pet_data(page, customer_url, customer_id, used_usernames, used_emails, noemail_counter):
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
    
    # Check for existing progress file
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            processed_customer_urls = set(line.strip() for line in f if line.strip())
        print(f"Resuming from previous session: {len(processed_customer_urls)} customers already processed")
    
    # Load existing data if files exist
    if os.path.exists(PETS_OUTPUT_FILE) and processed_customer_urls:
        try:
            with open(PETS_OUTPUT_FILE, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                all_pets = list(reader)
            if all_pets:
                max_pet_id = max(int(p.get('id', PET_ID_START)) for p in all_pets)
                next_pet_id = max_pet_id + 1
            print(f"Loaded {len(all_pets)} existing pets from {PETS_OUTPUT_FILE}")
        except Exception as e:
            print(f"Warning: Could not load existing pets data: {e}")
    
    if os.path.exists(CUSTOMERS_OUTPUT_FILE) and processed_customer_urls:
        try:
            with open(CUSTOMERS_OUTPUT_FILE, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                all_customers = list(reader)
            # Get next customer ID and rebuild used usernames/emails from existing data
            if all_customers:
                max_id = max(int(c.get('id', CUSTOMER_ID_START)) for c in all_customers)
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
            print(f"Loaded {len(all_customers)} existing customers from {CUSTOMERS_OUTPUT_FILE}")
        except Exception as e:
            print(f"Warning: Could not load existing customers data: {e}")
    
    # Load existing medical records if files exist
    if os.path.exists(MEDICAL_RECORDS_OUTPUT_FILE) and processed_customer_urls:
        try:
            with open(MEDICAL_RECORDS_OUTPUT_FILE, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                all_medical_records = list(reader)
            if all_medical_records:
                max_id = max(int(r.get('id', MEDICAL_RECORD_ID_START)) for r in all_medical_records)
                next_medical_record_id = max_id + 1
            print(f"Loaded {len(all_medical_records)} existing medical records")
        except Exception as e:
            print(f"Warning: Could not load existing medical records: {e}")
    
    if os.path.exists(MEDICAL_RECORD_ENTRIES_OUTPUT_FILE) and processed_customer_urls:
        try:
            with open(MEDICAL_RECORD_ENTRIES_OUTPUT_FILE, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                all_medical_record_entries = list(reader)
            print(f"Loaded {len(all_medical_record_entries)} existing medical record entries")
        except Exception as e:
            print(f"Warning: Could not load existing medical record entries: {e}")
    
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
                print(f"Processing customer {i}/{total}: {customer_url}")
                
                try:
                    customer_data, pets, raw_medical_records = await extract_pet_data(page, customer_url, next_customer_id, used_usernames, used_emails, noemail_counter)
                    
                    if customer_data:
                        pending_customers.append(customer_data)
                        all_customers.append(customer_data)
                        next_customer_id += 1
                    
                    # Assign IDs to pets and build name-to-id mapping
                    pet_name_to_id = {}
                    for pet in pets:
                        pet['id'] = next_pet_id
                        pet_name = pet.get('pet_name', '').upper()
                        if pet_name:
                            pet_name_to_id[pet_name] = next_pet_id
                        next_pet_id += 1
                    
                    pending_pets.extend(pets)
                    all_pets.extend(pets)
                    
                    # Process raw medical records - group by (pet_id, record_date)
                    # One medical_record per unique (pet_id, record_date), multiple entries can share it
                    medical_record_map = {}  # (pet_id, record_date) -> medical_record_id
                    
                    for raw_record in raw_medical_records:
                        pet_name = raw_record.get('pet_name', '').upper()
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
                        # Save CSVs first
                        save_customers_csv(all_customers, CUSTOMERS_OUTPUT_FILE)
                        save_pets_csv(all_pets, PETS_OUTPUT_FILE)
                        save_medical_records_csv(all_medical_records, MEDICAL_RECORDS_OUTPUT_FILE)
                        save_medical_record_entries_csv(all_medical_record_entries, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE)
                        
                        # Only THEN mark customers as processed (ensures consistency)
                        processed_customer_urls.update(pending_urls)
                        with open(PROGRESS_FILE, 'w') as f:
                            f.write('\n'.join(processed_customer_urls))
                        
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
                save_customers_csv(all_customers, CUSTOMERS_OUTPUT_FILE)
                save_pets_csv(all_pets, PETS_OUTPUT_FILE)
                save_medical_records_csv(all_medical_records, MEDICAL_RECORDS_OUTPUT_FILE)
                save_medical_record_entries_csv(all_medical_record_entries, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE)
                processed_customer_urls.update(pending_urls)
                with open(PROGRESS_FILE, 'w') as f:
                    f.write('\n'.join(processed_customer_urls))
            
            await browser.close()
    
    # Final save
    if all_customers:
        save_customers_csv(all_customers, CUSTOMERS_OUTPUT_FILE)
    if all_pets:
        save_pets_csv(all_pets, PETS_OUTPUT_FILE)
    if all_medical_records:
        save_medical_records_csv(all_medical_records, MEDICAL_RECORDS_OUTPUT_FILE)
    if all_medical_record_entries:
        save_medical_record_entries_csv(all_medical_record_entries, MEDICAL_RECORD_ENTRIES_OUTPUT_FILE)
    
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


def save_pets_csv(pets, filename):
    """Save pet data to CSV file with UTF-8 BOM encoding"""
    if not pets:
        return
    
    fieldnames = ['id', 'user_id', 'pet_name', 'species', 'breed', 'spayed', 'birthdate', 'color']
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(pets)


def save_customers_csv(customers, filename):
    """Save customer data to CSV file with UTF-8 BOM encoding"""
    if not customers:
        return
    
    fieldnames = ['id', 'first_name', 'last_name', 'username', 'password', 'role', 'phone', 'additional_phone_1', 'additional_phone_2', 'email', 'street_address']
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)


def save_medical_records_csv(records, filename):
    """Save medical records to CSV file with UTF-8 BOM encoding"""
    if not records:
        return
    
    fieldnames = ['id', 'pet_id', 'created_by', 'record_date']
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def save_medical_record_entries_csv(entries, filename):
    """Save medical record entries to CSV file with UTF-8 BOM encoding"""
    if not entries:
        return
    
    fieldnames = ['medical_record_id', 'entry_type', 'title', 'description', 'created_by']
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)


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
