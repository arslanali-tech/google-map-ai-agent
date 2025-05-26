# Enhanced Google Maps Business Details Scraper with Robust Social Media & Email Extraction
# Description: Extracts business details from Google Maps using Playwright and Gemini API, exports to Excel.

import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright
import os
import requests
from dotenv import load_dotenv
import json
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import httpx
from urllib.parse import urljoin, urlparse, unquote
import hashlib
from typing import Dict, List, Set, Optional, Tuple

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables.")

OUTPUT_FILE = 'google_maps_businesses.xlsx'
SEARCH_URL = 'https://www.google.com/maps'
GEMINI_MODEL = 'models/gemini-2.0-flash'
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"

class RobustSocialExtractor:
    """Enhanced social media and email extraction class"""
    
    # Comprehensive social media patterns
    SOCIAL_PATTERNS = {
        'Facebook': {
            'domains': ['facebook.com', 'fb.com', 'm.facebook.com', 'www.facebook.com', 'fb.me'],
            'patterns': [
                r'(?:https?://)?(?:www\.|m\.)?facebook\.com/(?:pages/)?([^/?&#\s]+)',
                r'(?:https?://)?fb\.com/([^/?&#\s]+)',
                r'(?:https?://)?fb\.me/([^/?&#\s]+)'
            ],
            'keywords': ['facebook', 'fb page', 'find us on facebook', 'like us on facebook']
        },
        'Instagram': {
            'domains': ['instagram.com', 'instagr.am', 'www.instagram.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.)?instagram\.com/([^/?&#\s]+)',
                r'(?:https?://)?instagr\.am/([^/?&#\s]+)',
                r'@([a-zA-Z0-9._]{1,30})\s*(?:on\s+)?instagram'
            ],
            'keywords': ['instagram', 'insta', 'follow us on instagram', '@']
        },
        'Twitter': {
            'domains': ['twitter.com', 'x.com', 'm.twitter.com', 'www.twitter.com', 'www.x.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.|m\.)?(?:twitter|x)\.com/([^/?&#\s]+)',
                r'@([a-zA-Z0-9_]{1,15})\s*(?:on\s+)?(?:twitter|x)'
            ],
            'keywords': ['twitter', 'tweet', 'follow us on twitter', 'x.com', '@']
        },
        'LinkedIn': {
            'domains': ['linkedin.com', 'www.linkedin.com', 'm.linkedin.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.|m\.)?linkedin\.com/(?:company|in)/([^/?&#\s]+)',
                r'(?:https?://)?(?:www\.|m\.)?linkedin\.com/pub/([^/?&#\s]+)'
            ],
            'keywords': ['linkedin', 'connect with us on linkedin', 'professional network']
        },
        'YouTube': {
            'domains': ['youtube.com', 'youtu.be', 'm.youtube.com', 'www.youtube.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.|m\.)?youtube\.com/(?:channel|user|c)/([^/?&#\s]+)',
                r'(?:https?://)?youtu\.be/([^/?&#\s]+)',
                r'(?:https?://)?(?:www\.)?youtube\.com/@([^/?&#\s]+)'
            ],
            'keywords': ['youtube', 'subscribe', 'youtube channel', 'watch us on youtube']
        },
        'TikTok': {
            'domains': ['tiktok.com', 'vm.tiktok.com', 'www.tiktok.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.|vm\.)?tiktok\.com/@([^/?&#\s]+)',
                r'(?:https?://)?(?:www\.|vm\.)?tiktok\.com/([^/?&#\s]+)',
                r'@([a-zA-Z0-9._]{1,24})\s*(?:on\s+)?tiktok'
            ],
            'keywords': ['tiktok', 'follow us on tiktok', 'tik tok']
        },
        'Yelp': {
            'domains': ['yelp.com', 'm.yelp.com', 'www.yelp.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.|m\.)?yelp\.com/biz/([^/?&#\s]+)',
                r'(?:https?://)?(?:www\.|m\.)?yelp\.com/([^/?&#\s]+)'
            ],
            'keywords': ['yelp', 'review us on yelp', 'find us on yelp']
        },
        'WhatsApp': {
            'domains': ['wa.me', 'api.whatsapp.com', 'whatsapp.com'],
            'patterns': [
                r'(?:https?://)?wa\.me/([0-9]+)',
                r'(?:https?://)?api\.whatsapp\.com/send\?phone=([0-9]+)',
                r'whatsapp:([0-9+\s\-()]+)'
            ],
            'keywords': ['whatsapp', 'message us on whatsapp', 'whatsapp business']
        },
        'Pinterest': {
            'domains': ['pinterest.com', 'pin.it', 'www.pinterest.com'],
            'patterns': [
                r'(?:https?://)?(?:www\.)?pinterest\.com/([^/?&#\s]+)',
                r'(?:https?://)?pin\.it/([^/?&#\s]+)'
            ],
            'keywords': ['pinterest', 'pin us', 'follow us on pinterest']
        }
    }
    
    # Enhanced email patterns
    EMAIL_PATTERNS = [
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
        r'email\s*:?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
        r'contact\s*:?\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})'
    ]
    
    # Common false positive domains to exclude
    EXCLUDED_DOMAINS = {
        'example.com', 'test.com', 'domain.com', 'placeholder.com', 'sample.com',
        'yoursite.com', 'website.com', 'company.com', 'business.com', 'email.com'
    }
    
    @staticmethod
    def extract_social_from_text(text: str) -> Dict[str, str]:
        """Extract social media links from text using advanced pattern matching"""
        results = {}
        text_lower = text.lower()
        
        for platform, config in RobustSocialExtractor.SOCIAL_PATTERNS.items():
            results[platform] = ''
            
            # First, try direct URL patterns
            for pattern in config['patterns']:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Construct full URL if needed
                    full_url = match.group(0)
                    if not full_url.startswith('http'):
                        if platform == 'Instagram' and full_url.startswith('@'):
                            full_url = f"https://instagram.com/{full_url[1:]}"
                        elif platform == 'Twitter' and full_url.startswith('@'):
                            full_url = f"https://x.com/{full_url[1:]}"
                        else:
                            # Try to build URL from the matched group
                            username = match.group(1) if match.groups() else match.group(0)
                            if platform == 'Facebook':
                                full_url = f"https://facebook.com/{username}"
                            elif platform == 'Instagram':
                                full_url = f"https://instagram.com/{username}"
                            elif platform == 'Twitter':
                                full_url = f"https://x.com/{username}"
                            elif platform == 'LinkedIn':
                                full_url = f"https://linkedin.com/company/{username}"
                            elif platform == 'YouTube':
                                full_url = f"https://youtube.com/channel/{username}"
                            elif platform == 'TikTok':
                                full_url = f"https://tiktok.com/@{username}"
                            elif platform == 'Yelp':
                                full_url = f"https://yelp.com/biz/{username}"
                    
                    if RobustSocialExtractor._is_valid_social_url(full_url, platform):
                        results[platform] = full_url.strip()
                        break
            
            # If no direct match found, look for domain mentions with context
            if not results[platform]:
                for domain in config['domains']:
                    if domain in text_lower:
                        # Look for URL context around the domain
                        domain_pattern = rf'https?://[^\s]*{re.escape(domain)}[^\s]*'
                        matches = re.finditer(domain_pattern, text, re.IGNORECASE)
                        for match in matches:
                            url = match.group(0)
                            if RobustSocialExtractor._is_valid_social_url(url, platform):
                                results[platform] = url.strip()
                                break
                        if results[platform]:
                            break
        
        return results
    
    @staticmethod
    def _is_valid_social_url(url: str, platform: str) -> bool:
        """Validate if the URL is a legitimate social media URL"""
        if not url or len(url) < 10:
            return False
        
        try:
            parsed = urlparse(url if url.startswith('http') else f'https://{url}')
            domain = parsed.netloc.lower()
            
            # Check if domain matches platform
            valid_domains = RobustSocialExtractor.SOCIAL_PATTERNS[platform]['domains']
            if not any(valid_domain in domain for valid_domain in valid_domains):
                return False
            
            # Additional validation based on platform
            if platform == 'Facebook':
                # Avoid generic Facebook URLs
                if parsed.path in ['/', '/login', '/signup', '/home']:
                    return False
            elif platform == 'Instagram':
                # Ensure it's a profile URL
                if not parsed.path or parsed.path in ['/', '/accounts/login/']:
                    return False
            elif platform == 'Twitter':
                # Ensure it's a profile URL
                if not parsed.path or parsed.path in ['/', '/login', '/home']:
                    return False
            
            return True
        except:
            return False
    
    @staticmethod
    def extract_emails_from_text(text: str) -> List[str]:
        """Extract email addresses from text with enhanced validation"""
        emails = set()
        
        for pattern in RobustSocialExtractor.EMAIL_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                email = match.group(1) if match.groups() else match.group(0)
                email = email.strip().lower()
                
                if RobustSocialExtractor._is_valid_email(email):
                    emails.add(email)
        
        return list(emails)
    
    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Validate email address"""
        if not email or '@' not in email:
            return False
        
        try:
            local, domain = email.rsplit('@', 1)
            
            # Basic validation
            if len(local) < 1 or len(domain) < 3:
                return False
            
            # Check for excluded domains
            if domain in RobustSocialExtractor.EXCLUDED_DOMAINS:
                return False
            
            # Check for common fake patterns
            if any(fake in email for fake in ['noreply', 'no-reply', 'donotreply', 'example']):
                return False
            
            # Basic format validation
            if not re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', email):
                return False
            
            return True
        except:
            return False

async def gemini_generate(prompt):
    headers = {"Content-Type": "application/json"}
    data = {
        "contents": [{"parts": [{"text": prompt}]}]
    }
    max_attempts = 5
    async with httpx.AsyncClient(timeout=30) as client:
        for attempt in range(max_attempts):
            try:
                wait_time = (1.5 ** attempt) + (0.3 * (os.urandom(1)[0] % 3))
                await asyncio.sleep(wait_time)
                response = await client.post(GEMINI_API_URL, headers=headers, json=data)
                response.raise_for_status()
                result = response.json()
                text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
                
                # Extract JSON from response
                json_match = re.search(r'\{.*?\}', text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    try:
                        decoder = json.JSONDecoder()
                        obj, _ = decoder.raw_decode(json_str)
                        return obj
                    except Exception as e:
                        print(f"Gemini JSON decode error: {e}\nResponse text: {text}")
                        return None
                print(f"Gemini response not valid JSON: {text}")
                return None
            except httpx.TimeoutException as e:
                print(f"Gemini API timeout (attempt {attempt+1}): {e}. Retrying...")
                continue
            except httpx.HTTPStatusError as e:
                print(f"Gemini API HTTP error (attempt {attempt+1}): {e}")
                if e.response.status_code == 429:
                    print(f"Rate limited, backing off...")
                    continue
                else:
                    if attempt == max_attempts - 1:
                        return None
            except Exception as e:
                print(f"Gemini API error (attempt {attempt+1}): {e}")
                if attempt == max_attempts - 1:
                    return None
                continue

def extract_with_gemini(raw_text):
    prompt = f"""
Extract the following business details from the text below. Return a JSON object with these keys: Business Name, Business Type, Address, Phone Number, Email, Website. If a field is missing, use an empty string.

Text:
{raw_text}
"""
    return gemini_generate(prompt)

def clean_field(value):
    if not value:
        return ''
    # Remove non-printable characters
    value = re.sub(r'[\u200B-\u200D\uFEFF]', '', value)
    # Remove excessive whitespace
    lines = [line.strip() for line in value.split('\n') if line.strip()]
    # Remove duplicates while preserving order
    seen = set()
    cleaned = [line for line in lines if not (line in seen or seen.add(line))]
    return ' '.join(cleaned).strip()

async def safe_text(page, selector):
    try:
        elements = await page.query_selector_all(selector)
        for el in elements:
            text = await el.inner_text()
            if text.strip():
                return text.strip()
    except Exception as e:
        print(f"Error in safe_text for selector '{selector}': {e}")
    return ''

def is_valid_url(url):
    """Check if a URL is valid and accessible"""
    try:
        parsed = urlparse(url)
        return bool(parsed.netloc) and parsed.scheme in ['http', 'https']
    except:
        return False

def normalize_url(url):
    """Normalize URL by adding https if missing"""
    if not url:
        return ''
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url

def create_business_hash(name, address, phone):
    """Create a unique hash for a business to detect duplicates"""
    name_norm = clean_field(name).lower().strip()
    address_norm = clean_field(address).lower().strip()
    phone_norm = re.sub(r'[^\d]', '', clean_field(phone))
    
    unique_string = f"{name_norm}|{address_norm}|{phone_norm}"
    return hashlib.md5(unique_string.encode()).hexdigest()

class ScraperController:
    def __init__(self):
        self.stop_scrolling_requested = False
        self.stop_all_requested = False

    def request_stop_scrolling(self):
        self.stop_scrolling_requested = True

    def request_stop_all(self):
        self.stop_all_requested = True

controller = ScraperController()

async def enhanced_extract_from_website(url: str, context) -> Tuple[Dict[str, str], List[str]]:
    """
    Enhanced website extraction for social media links and emails
    Returns: (social_media_dict, email_list)
    """
    if not is_valid_url(url):
        return {}, []
    
    try:
        print(f"Enhanced extraction from: {url}")
        page = await context.new_page()
        
        try:
            await page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            
            await page.goto(url, timeout=25000, wait_until='domcontentloaded')
            await asyncio.sleep(3)  # Allow dynamic content to load
            
            # Get all text content
            all_text = await page.evaluate('document.body.innerText || document.body.textContent || ""')
            
            # Get HTML content for link extraction
            html_content = await page.content()
            
            # Extract social media links using enhanced extractor
            social_data = RobustSocialExtractor.extract_social_from_text(all_text)
            
            # Also extract from HTML links
            links = await page.evaluate('''
                () => {
                    const anchors = document.querySelectorAll('a[href]');
                    const links = new Set();
                    
                    anchors.forEach(anchor => {
                        let href = anchor.href;
                        if (href && !href.startsWith('javascript:') && !href.startsWith('mailto:') && !href.startsWith('tel:')) {
                            try {
                                const absoluteUrl = new URL(href, window.location.href).href;
                                links.add(absoluteUrl);
                            } catch (e) {
                                // Skip invalid URLs
                            }
                        }
                    });
                    
                    return Array.from(links);
                }
            ''')
            
            # Enhance social data with found links
            for link in links:
                for platform, config in RobustSocialExtractor.SOCIAL_PATTERNS.items():
                    if not social_data.get(platform):  # Only if not already found
                        for domain in config['domains']:
                            if domain in link.lower():
                                if RobustSocialExtractor._is_valid_social_url(link, platform):
                                    social_data[platform] = link
                                    break
            
            # Extract emails using enhanced method
            emails = RobustSocialExtractor.extract_emails_from_text(all_text)
            
            # Also check for mailto links
            mailto_links = await page.evaluate('''
                () => {
                    const mailtoLinks = document.querySelectorAll('a[href^="mailto:"]');
                    return Array.from(mailtoLinks).map(link => link.href.replace('mailto:', '')).filter(email => email.includes('@'));
                }
            ''')
            
            for email in mailto_links:
                if RobustSocialExtractor._is_valid_email(email.lower()):
                    emails.append(email.lower())
            
            # Remove duplicates from emails
            emails = list(set(emails))
            
            print(f"Enhanced extraction complete: Social platforms found: {sum(1 for v in social_data.values() if v)}, Emails found: {len(emails)}")
            return social_data, emails
            
        except Exception as e:
            print(f"Error during enhanced extraction from {url}: {e}")
            return {}, []
        finally:
            await page.close()
            
    except Exception as e:
        print(f"Error creating page for enhanced extraction from {url}: {e}")
        return {}, []

async def scrape_google_maps(query, max_cards=200, controller=controller):
    data = []
    unique_hashes = set()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        print('Opening Google Maps...')
        await page.goto(SEARCH_URL)
        await page.wait_for_selector('input#searchboxinput', timeout=15000)
        await page.fill('input#searchboxinput', query)
        await page.click('button#searchbox-searchbutton')
        await page.wait_for_selector('div[role="main"]', timeout=15000)
        print('Waiting for results to load...')
        await asyncio.sleep(2)

        results_selector = '.Nv2PK, div[role="article"], .hfpxzc'
        scrollable_selector = 'div[role="main"] div[aria-label][tabindex="0"]'
        
        try:
            scrollable = await page.query_selector(scrollable_selector)
        except Exception:
            scrollable = None
        if not scrollable:
            scrollable = page

        last_count = 0
        no_new_cards_scrolls = 0
        max_no_new_cards_scrolls = 8
        print(f'Scrolling to load cards (target: {max_cards})...')
        
        # Scroll to load cards
        while True:
            if controller.stop_all_requested:
                print('All stopped by user during scrolling.')
                return data
            if controller.stop_scrolling_requested:
                print(f'Scrolling stopped by user at {last_count} cards.')
                break
                
            cards = await page.query_selector_all(results_selector)
            print(f'Cards loaded: {len(cards)} (target: {max_cards})')
            
            if len(cards) >= max_cards * 1.5:
                print(f'Loaded sufficient cards: {len(cards)}')
                break
                
            if len(cards) == last_count:
                no_new_cards_scrolls += 1
            else:
                no_new_cards_scrolls = 0
            last_count = len(cards)
            
            if no_new_cards_scrolls >= max_no_new_cards_scrolls:
                print(f'No more new cards found. Total: {len(cards)}')
                break
            
            # Scroll actions
            try:
                await scrollable.focus()
                await page.evaluate(f"let el = document.querySelector('{scrollable_selector}'); if(el) el.scrollTo(0, el.scrollHeight);")
                await page.mouse.wheel(0, 8000)
                for _ in range(6):
                    if controller.stop_all_requested or controller.stop_scrolling_requested:
                        break
                    await page.keyboard.press('PageDown')
                    await asyncio.sleep(0.3)
            except Exception:
                pass
            await asyncio.sleep(2)
        
        # Enhanced extraction loop
        cards = await page.query_selector_all(results_selector)
        print(f'Starting enhanced extraction from {len(cards)} cards...')
        
        processed_count = 0
        for idx in range(len(cards)):
            if controller.stop_all_requested:
                print('All stopped by user during extraction.')
                break
            if len(data) >= max_cards:
                print(f'Reached target of {max_cards} unique businesses.')
                break
                
            card = cards[idx]
            processed_count += 1
            
            try:
                print(f'Processing business {processed_count}/{len(cards)} (Unique found: {len(data)}/{max_cards})...')
                await card.click()
                await page.wait_for_selector('h1, .fontHeadlineLarge, .DUwDvf', timeout=8000)
                await asyncio.sleep(1)
                
                # Get all text for analysis
                all_text = await page.evaluate('document.body.innerText')
                
                # Try Gemini extraction first
                gemini_data = await extract_with_gemini(all_text)
                
                if gemini_data:
                    name = gemini_data.get('Business Name', '')
                    business_type = gemini_data.get('Business Type', '')
                    address = gemini_data.get('Address', '')
                    phone = gemini_data.get('Phone Number', '')
                    email = gemini_data.get('Email', '')
                    website = gemini_data.get('Website', '')
                else:
                    # Fallback to manual extraction
                    name = await safe_text(page, 'h1, .fontHeadlineLarge, .DUwDvf, [data-item-id="title"]')
                    business_type = await safe_text(page, '.fontBodyMedium button[jsaction*="pane.rating.category"], .skqShb')
                    address = await safe_text(page, '[data-item-id="address"], .rogA2c, .Io6YTe.fontBodyMedium, .LrzXr')
                    phone = await safe_text(page, '[data-item-id="phone"], .Io6YTe.fontBodyMedium, .UsdlK')
                    
                    if not phone:
                        phone_matches = re.findall(r'(\+?\d[\d\s\-().]{8,}\d)', all_text)
                        phone = phone_matches[0] if phone_matches else ''
                    
                    # Extract website
                    website = ''
                    website_elements = await page.query_selector_all('a[data-item-id="authority"], a[aria-label*="Website"], .rogA2c a, .Io6YTe a')
                    for element in website_elements:
                        href = await element.get_attribute('href')
                        if href and 'google.com' not in href:
                            website = href
                            break
                    
                    if not website:
                        domain_match = re.search(r'([a-zA-Z0-9\-\.]+\.(com|net|org|biz|info|co|us|in|uk|ca|au|io|me|site|store|online|tech|ai|app))', all_text)
                        if domain_match:
                            website = domain_match.group(0)
                    
                    # Extract email
                    email = ''
                    email_link = await page.query_selector('a[href^="mailto:"]')
                    if email_link:
                        email = (await email_link.get_attribute('href')).replace('mailto:', '').strip()
                    
                    if not email:
                        emails = RobustSocialExtractor.extract_emails_from_text(all_text)
                        email = emails[0] if emails else ''

                # Clean all fields
                name = clean_field(name)
                business_type = clean_field(business_type)
                address = clean_field(address)
                phone = clean_field(phone)
                email = clean_field(email)
                website = clean_field(website)
                
                # Check for duplicates
                business_hash = create_business_hash(name, address, phone)
                if business_hash in unique_hashes:
                    print(f'Skipping duplicate: {name}')
                    continue
                unique_hashes.add(business_hash)
                
                if not name.strip():
                    print(f'Skipping business with empty name')
                    continue
                
                # Normalize website URL
                if website and not website.startswith('http'):
                    website = normalize_url(website)
                
                # Initialize social media data
                social_media_data = {platform: '' for platform in RobustSocialExtractor.SOCIAL_PATTERNS.keys()}
                found_emails = [email] if email else []
                
                # Extract social media from Google Maps page text
                maps_social_data = RobustSocialExtractor.extract_social_from_text(all_text)
                for platform, link in maps_social_data.items():
                    if link:
                        social_media_data[platform] = link
                
                # Enhanced website extraction
                if website and is_valid_url(website):
                    print(f'Enhanced extraction from website: {website}')
                    try:
                        website_social_data, website_emails = await enhanced_extract_from_website(website, context)
                        
                        # Merge social media data (website takes precedence)
                        for platform, link in website_social_data.items():
                            if link and not social_media_data.get(platform):
                                social_media_data[platform] = link
                        
                        # Add website emails
                        found_emails.extend(website_emails)
                        
                    except Exception as e:
                        print(f'Error in enhanced website extraction for {website}: {e}')
                
                # Use the best email found
                final_email = found_emails[0] if found_emails else ''
                
                # Create business data entry
                business_data = {
                    'Business Name': name,
                    'Business Type': business_type,
                    'Address': address,
                    'Phone Number': phone,
                    'Email': final_email,
                    'Website': website,
                    'Facebook': social_media_data['Facebook'],
                    'Instagram': social_media_data['Instagram'],
                    'Twitter': social_media_data['Twitter'],
                    'LinkedIn': social_media_data['LinkedIn'],
                    'YouTube': social_media_data['YouTube'],
                    'TikTok': social_media_data['TikTok'],
                    'Yelp': social_media_data['Yelp'],
                    'WhatsApp': social_media_data['WhatsApp'],
                    'Pinterest': social_media_data['Pinterest']
                }
                
                data.append(business_data)
                
                # Count social media platforms found
                social_count = sum(1 for platform, link in social_media_data.items() if link)
                print(f'✓ Enhanced Extraction: {name}')
                print(f'  Website: {website}')
                print(f'  Email: {final_email}')
                print(f'  Social Platforms: {social_count}/9')
                print(f'  Progress: {len(data)}/{max_cards} unique businesses')
                
            except Exception as e:
                print(f'Error extracting business {processed_count}: {e}')
                continue
        
        await browser.close()
    return data

def export_to_excel(data, filename):
    """Enhanced Excel export with better formatting"""
    try:
        df = pd.DataFrame(data)
        
        # Reorder columns for better presentation
        column_order = [
            'Business Name', 'Business Type', 'Address', 'Phone Number', 
            'Email', 'Website', 'Facebook', 'Instagram', 'Twitter', 
            'LinkedIn', 'YouTube', 'TikTok', 'Yelp', 'WhatsApp', 'Pinterest'
        ]
        
        # Only include columns that exist in the data
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        # Export to Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Businesses')
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Businesses']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f'Enhanced export complete: {len(df)} businesses to {filename}')
        
        # Print summary statistics
        print(f'\n=== EXTRACTION SUMMARY ===')
        print(f'Total businesses: {len(df)}')
        print(f'Businesses with emails: {len(df[df["Email"] != ""])}')
        print(f'Businesses with websites: {len(df[df["Website"] != ""])}')
        
        # Social media statistics
        social_platforms = ['Facebook', 'Instagram', 'Twitter', 'LinkedIn', 'YouTube', 'TikTok', 'Yelp', 'WhatsApp', 'Pinterest']
        for platform in social_platforms:
            if platform in df.columns:
                count = len(df[df[platform] != ""])
                print(f'Businesses with {platform}: {count}')
        
    except Exception as e:
        print(f'Error exporting to Excel: {e}')

def run_scraper_from_ui(query, max_cards, status_label, button, stop_scroll_button, stop_all_button):
    def task():
        controller.stop_scrolling_requested = False
        controller.stop_all_requested = False
        try:
            status_label.config(text='Enhanced scraping in progress...')
            results = asyncio.run(scrape_google_maps(query.get(), max_cards.get(), controller=controller))
            export_to_excel(results, OUTPUT_FILE)
            status_label.config(text='Enhanced extraction complete!')
            
            # Show detailed success message
            social_count = sum(1 for business in results for platform in ['Facebook', 'Instagram', 'Twitter', 'LinkedIn', 'YouTube', 'TikTok', 'Yelp', 'WhatsApp', 'Pinterest'] if business.get(platform))
            email_count = sum(1 for business in results if business.get('Email'))
            website_count = sum(1 for business in results if business.get('Website'))
            
            message = f'''Enhanced Scraping Complete!
            
Extracted {len(results)} unique businesses
• {email_count} businesses with emails
• {website_count} businesses with websites  
• {social_count} social media links found
            
Data exported to: {OUTPUT_FILE}'''
            
            messagebox.showinfo('Enhanced Extraction Complete', message)
            
        except Exception as e:
            status_label.config(text='Error occurred')
            messagebox.showerror('Error', f'Enhanced extraction failed: {str(e)}')
        finally:
            button.config(state=tk.NORMAL)
            stop_scroll_button.config(state=tk.DISABLED)
            stop_all_button.config(state=tk.DISABLED)
    
    threading.Thread(target=task).start()

def launch_ui():
    """Enhanced UI with better styling and information"""
    root = tk.Tk()
    root.title('Enhanced Google Maps Business Scraper v2.0')
    root.geometry('600x400')
    root.configure(bg='#f0f0f0')
    
    # Main frame
    main_frame = ttk.Frame(root, padding=25)
    main_frame.pack(fill=tk.BOTH, expand=True)
    
    # Title
    title_label = ttk.Label(main_frame, text='Enhanced Google Maps Business Scraper', 
                           font=('Arial', 16, 'bold'))
    title_label.pack(anchor=tk.W, pady=(0,10))
    
    # Description
    desc_label = ttk.Label(main_frame, text='Advanced extraction of business details, emails, and social media links', 
                          font=('Arial', 10), foreground='#666666')
    desc_label.pack(anchor=tk.W, pady=(0,20))
    
    # Search query section
    query_frame = ttk.LabelFrame(main_frame, text='Search Configuration', padding=15)
    query_frame.pack(fill=tk.X, pady=(0,15))
    
    query_label = ttk.Label(query_frame, text='Google Maps search query:')
    query_label.pack(anchor=tk.W, pady=(0,5))
    
    query_var = tk.StringVar()
    query_entry = ttk.Entry(query_frame, textvariable=query_var, width=60, font=('Arial', 11))
    query_entry.pack(fill=tk.X, pady=(0,10))
    query_entry.focus()
    
    # Max cards section
    cards_frame = ttk.Frame(query_frame)
    cards_frame.pack(fill=tk.X)
    
    max_cards_label = ttk.Label(cards_frame, text='Maximum businesses to extract:')
    max_cards_label.pack(side=tk.LEFT)
    
    max_cards_var = tk.IntVar(value=80)
    max_cards_entry = ttk.Entry(cards_frame, textvariable=max_cards_var, width=10, font=('Arial', 11))
    max_cards_entry.pack(side=tk.LEFT, padx=(10,0))
    
    # Features section
    features_frame = ttk.LabelFrame(main_frame, text='Enhanced Features', padding=15)
    features_frame.pack(fill=tk.X, pady=(0,15))
    
    features_text = '''✓ Advanced social media link extraction (9 platforms)
✓ Robust email detection with validation
✓ Website crawling for additional data
✓ Duplicate business detection
✓ Enhanced data validation
✓ Comprehensive Excel export with statistics'''
    
    features_label = ttk.Label(features_frame, text=features_text, font=('Arial', 9))
    features_label.pack(anchor=tk.W)
    
    # Control buttons section
    control_frame = ttk.LabelFrame(main_frame, text='Controls', padding=15)
    control_frame.pack(fill=tk.X, pady=(0,15))
    
    # Status
    status_label = ttk.Label(control_frame, text='Ready to start enhanced extraction', 
                            font=('Arial', 10, 'italic'), foreground='#0066cc')
    status_label.pack(anchor=tk.W, pady=(0,10))
    
    # Buttons
    button_frame = ttk.Frame(control_frame)
    button_frame.pack(fill=tk.X)
    
    def on_start():
        if not query_var.get().strip():
            messagebox.showwarning('Input Required', 'Please enter a search query.')
            return
        if max_cards_var.get() <= 0:
            messagebox.showwarning('Invalid Input', 'Please enter a valid number of businesses (greater than 0).')
            return
        
        start_button.config(state=tk.DISABLED)
        stop_scroll_button.config(state=tk.NORMAL)
        stop_all_button.config(state=tk.NORMAL)
        run_scraper_from_ui(query_var, max_cards_var, status_label, start_button, stop_scroll_button, stop_all_button)
    
    def on_stop_scroll():
        controller.request_stop_scrolling()
        status_label.config(text='Stopping scrolling, will begin extraction...')
        stop_scroll_button.config(state=tk.DISABLED)
    
    def on_stop_all():
        controller.request_stop_all()
        status_label.config(text='Stopping all operations...')
        stop_all_button.config(state=tk.DISABLED)
        stop_scroll_button.config(state=tk.DISABLED)
    
    start_button = ttk.Button(button_frame, text='🚀 Start Enhanced Extraction', command=on_start, 
                             style='Accent.TButton')
    start_button.pack(side=tk.LEFT, padx=(0,10))
    
    stop_scroll_button = ttk.Button(button_frame, text='⏸️ Stop Scrolling', command=on_stop_scroll, 
                                   state=tk.DISABLED)
    stop_scroll_button.pack(side=tk.LEFT, padx=(0,10))
    
    stop_all_button = ttk.Button(button_frame, text='🛑 Stop All', command=on_stop_all, 
                                state=tk.DISABLED)
    stop_all_button.pack(side=tk.LEFT)
    
    # Help section
    help_frame = ttk.LabelFrame(main_frame, text='Tips', padding=10)
    help_frame.pack(fill=tk.X)
    
    help_text = '''• Use specific search terms (e.g., "restaurants in New York" vs "food")
• The scraper will extract emails and social links from business websites
• Larger numbers may take longer but provide more comprehensive data
• You can stop scrolling early and still extract data from loaded businesses'''
    
    help_label = ttk.Label(help_frame, text=help_text, font=('Arial', 9), foreground='#666666')
    help_label.pack(anchor=tk.W)
    
    root.mainloop()

if __name__ == '__main__':
    print("=== Enhanced Google Maps Business Scraper v2.0 ===")
    print("Features: Advanced social media extraction, robust email detection, website crawling")
    print("Supported platforms: Facebook, Instagram, Twitter, LinkedIn, YouTube, TikTok, Yelp, WhatsApp, Pinterest")
    print("=" * 80)
    launch_ui()