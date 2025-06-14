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
GEMINI_API_KEY = 'AIzaSyCC7Cd4yONA6BErnEFMtXxhqiEdARcXxcs'
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables.")

OUTPUT_FILE = 'google_maps_businesses.xlsx'
SEARCH_URL = 'https://www.google.com/maps'
GEMINI_MODEL = 'models/gemini-2.0-flash'
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
MAX_BUSINESSES = 500  # Increased maximum number of businesses

# Global cache for website extraction to prevent redundant processing
WEBSITE_EXTRACTION_CACHE = {}  # domain -> (social_data, emails)

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
        """Extract social media links from text using optimized pattern matching"""
        # Quick return for empty text
        if not text or len(text) < 20:
            return {platform: '' for platform in RobustSocialExtractor.SOCIAL_PATTERNS.keys()}
        
        results = {platform: '' for platform in RobustSocialExtractor.SOCIAL_PATTERNS.keys()}
        text_lower = text.lower()
        
        # First priority: Find fully formed URLs for each platform (most reliable)
        for platform, config in RobustSocialExtractor.SOCIAL_PATTERNS.items():
            # Exit early if we already found this platform
            if results[platform]:
                continue
                
            # Quick domain check before using regex
            if not any(domain in text_lower for domain in config['domains']):
                continue
                
            # Use domain-specific regex patterns for direct URL matches
            for pattern in config['patterns']:
                matches = list(re.finditer(pattern, text, re.IGNORECASE))
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
                                # Check if it's a company or personal profile
                                if 'company' in match.group(0).lower():
                                    full_url = f"https://linkedin.com/company/{username}"
                                else:
                                    full_url = f"https://linkedin.com/in/{username}"
                            elif platform == 'YouTube':
                                if '@' in username:
                                    full_url = f"https://youtube.com/{username}"
                                else:
                                    full_url = f"https://youtube.com/channel/{username}"
                            elif platform == 'TikTok':
                                full_url = f"https://tiktok.com/@{username}"
                            elif platform == 'Yelp':
                                full_url = f"https://yelp.com/biz/{username}"
                            elif platform == 'Pinterest':
                                full_url = f"https://pinterest.com/{username}"
                    
                    if RobustSocialExtractor._is_valid_social_url(full_url, platform):
                        results[platform] = full_url.strip()
                        break
                
                # Exit early if we found a match
                if results[platform]:
                    break
        
        # Second priority: For remaining platforms, extract from https URLs
        if not all(results.values()):
            # Extract all https URLs once
            url_pattern = r'https?://[^\s\'"<>()]+\.[a-zA-Z]{2,}[^\s\'"<>()]*'
            all_urls = re.findall(url_pattern, text)
            
            # Check each URL against remaining platforms
            for url in all_urls:
                for platform, link in results.items():
                    if link:  # Skip if already found
                        continue
                        
                    domains = RobustSocialExtractor.SOCIAL_PATTERNS[platform]['domains']
                    if any(domain in url.lower() for domain in domains):
                        if RobustSocialExtractor._is_valid_social_url(url, platform):
                            results[platform] = url
        
        # Third priority: Handle social media handles with @ symbol (for specific platforms)
        missing_platforms = ['Instagram', 'Twitter', 'TikTok']
        missing_platforms = [p for p in missing_platforms if not results[p]]
        
        if missing_platforms:
            # Find all potential handles with @ symbol (only once)
            handle_pattern = r'@([A-Za-z0-9._]{3,30})\b'
            handle_matches = list(re.finditer(handle_pattern, text))
            
            for match in handle_matches:
                handle = match.group(1)
                # Get context around the handle
                start = max(0, match.start() - 20)
                end = min(len(text), match.end() + 20)
                context = text[start:end].lower()
                
                # Check if context helps identify the platform
                if 'Instagram' in missing_platforms and ('instagram' in context or 'insta' in context):
                    results['Instagram'] = f"https://instagram.com/{handle}"
                elif 'Twitter' in missing_platforms and ('twitter' in context or 'tweet' in context or 'x.com' in context):
                    results['Twitter'] = f"https://x.com/{handle}"
                elif 'TikTok' in missing_platforms and ('tiktok' in context or 'tik tok' in context):
                    results['TikTok'] = f"https://tiktok.com/@{handle}"
        
        # Final validation pass - validate all URLs once at the end
        for platform, url in list(results.items()):
            if url and not RobustSocialExtractor._is_valid_social_url(url, platform):
                results[platform] = ''
        
        return results
    
    @staticmethod
    def _is_valid_social_url(url: str, platform: str) -> bool:
        """Validate if the URL is a legitimate social media URL with optimized validation"""
        if not url or len(url) < 10:
            return False
        
        try:
            # Normalize URL first
            if not url.startswith(('http://', 'https://')):
                url = f'https://{url}'
                
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            path = parsed.path.lower()
            
            # Fast domain check
            valid_domains = RobustSocialExtractor.SOCIAL_PATTERNS.get(platform, {}).get('domains', [])
            if not any(domain.endswith(valid_domain) or valid_domain in domain for valid_domain in valid_domains):
                return False
            
            # Common validation for all platforms
            # Reject URLs with suspicious fragments or extremely long paths
            if len(path) > 100 or '#' in url and len(parsed.fragment) > 20:
                return False
                
            # Platform-specific validation - optimized to reject obvious non-profile URLs first
            if platform == 'Facebook':
                # Quick rejection patterns
                if path in ['/', '/login', '/signup', '/home', '/pages', '/groups', '/hashtag', '/events']:
                    return False
                    
                # Reject generic URLs with query parameters that aren't profiles
                if path == '/' and parsed.query:
                    return False
                    
                # Reject numeric-only IDs which are typically not business pages
                if re.match(r'^/\d+/?$', path):
                    return False
                    
                # Must contain a username path segment that looks valid
                if not re.search(r'/[a-zA-Z][\w.]{2,}/?', path):
                    return False
                    
            elif platform == 'Instagram':
                # Reject generic URLs and post/photo URLs
                if not path or path == '/' or '/p/' in path:
                    return False
                    
                # Reject explore, stories, etc.
                if any(segment in path for segment in ['/explore/', '/reels/', '/stories/']):
                    return False
                    
                # Must have a valid username format
                if not re.search(r'/[a-zA-Z][\w.]{2,}/?', path):
                    return False
                    
            elif platform == 'Twitter':
                # Reject generic paths
                if not path or path in ['/', '/home', '/explore', '/notifications', '/messages']:
                    return False
                    
                # Reject posts, hashtags, etc.
                if any(segment in path for segment in ['/status/', '/lists/', '/i/', '/hashtag/']):
                    return False
                    
                # Must have a valid username format
                if not re.search(r'/[a-zA-Z][\w]{2,}/?', path):
                    return False
                    
            elif platform == 'LinkedIn':
                # Must be a company, school, or personal profile
                if not any(segment in path for segment in ['/company/', '/school/', '/in/', '/pub/']):
                    return False
                    
                # Ensure there's something after the segment
                if path.endswith('/company/') or path.endswith('/in/') or path.endswith('/school/'):
                    return False
                    
            elif platform == 'YouTube':
                # Allow shortened youtu.be links
                if 'youtu.be' in domain:
                    return True
                    
                # Must be a channel, user, or custom URL
                valid_segments = ['/channel/', '/user/', '/c/', '/@']
                if not any(segment in path for segment in valid_segments):
                    return False
                    
                # Ensure @ usernames are valid
                if '/@' in path and not re.search(r'/@[\w-]{3,}/?', path):
                    return False
                    
            elif platform == 'TikTok':
                # Must have a username
                if not path or (path.startswith('/@') and not re.search(r'/@[\w.]{3,}/?', path)):
                    return False
                    
                # If not using @ format, must still have a valid username pattern
                if not path.startswith('/@') and not re.search(r'/[a-zA-Z][\w.]{2,}/?', path):
                    return False
                    
            elif platform == 'Yelp':
                # Must be a business URL
                if not path.startswith('/biz/'):
                    return False
                    
                # Must have valid business name
                if not re.search(r'/biz/[a-zA-Z0-9_-]{3,}/?', path):
                    return False
                    
            elif platform == 'WhatsApp':
                # Check for wa.me format (most common)
                if 'wa.me' in domain:
                    return bool(re.search(r'/\d{7,}/?', path))
                    
                # Check for whatsapp.com API format
                if 'whatsapp.com' in domain:
                    return 'phone=' in parsed.query
                    
            elif platform == 'Pinterest':
                # Reject generic URLs
                if not path or path in ['/', '/login', '/explore', '/search']:
                    return False
                    
                # Must have a valid username
                if not re.search(r'/[a-zA-Z][\w-]{2,}/?', path):
                    return False
                    
                # Reject pins/boards which aren't profile URLs
                if any(segment in path for segment in ['/pin/']):
                    return False
            
            # Default to accepting if it passed all platform-specific checks
            return True
            
        except Exception as e:
            # Silent exception handling for performance
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
Extract the following business details from the text below. Return a JSON object with these keys: Business Name, Business Type, Address, Phone Number, Email, Website, Opening Time, Closing Time, Business Hours. 

For Opening Time and Closing Time, extract the standard opening and closing hours for today or the most typical day if today's hours aren't specified.

For Business Hours, extract the complete weekly schedule in a standardized format with each day of the week, like this:
"Monday: 9:00 AM - 5:00 PM; Tuesday: 9:00 AM - 5:00 PM; Wednesday: 9:00 AM - 5:00 PM; Thursday: 9:00 AM - 5:00 PM; Friday: 9:00 AM - 5:00 PM; Saturday: 10:00 AM - 3:00 PM; Sunday: Closed"

Include all seven days of the week if available. For days when the business is closed, use "Closed". For businesses open 24 hours, use "Open 24 hours". If hours for a specific day are unknown, use "Hours not available".

If a field is missing from the text, use an empty string.

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
    """Create a unique hash for a business to detect duplicates, with improved address normalization"""
    # Handle all empty inputs
    if not name and not address and not phone:
        # Generate a random hash to ensure it doesn't match anything
        return hashlib.md5(f"empty_business_{os.urandom(8).hex()}".encode()).hexdigest()
    
    # Normalize name: lowercase, strip whitespace, remove common business designations
    name_norm = clean_field(name).lower().strip()
    name_norm = re.sub(r'\b(inc|llc|ltd|corp|co|company|corporation|incorporated)\b\.?', '', name_norm)
    name_norm = re.sub(r'[^\w\s]', '', name_norm).strip()  # Remove punctuation
    
    # Normalize address: extract key parts and remove noise
    address_norm = ''
    if address:
        # Extract only street number, name and city if possible
        address_parts = clean_field(address).lower().split(',')
        if address_parts:
            # Get first part (usually street address)
            street = address_parts[0].strip()
            # Extract just numbers and letters from street address
            street = re.sub(r'[^\w\s]', '', street).strip()
            address_norm = street
            
            # Add city if available (usually the second part)
            if len(address_parts) > 1:
                city = re.sub(r'[^\w\s]', '', address_parts[1].strip())
                address_norm = f"{address_norm}_{city}"
    
    # Normalize phone: strip to digits only
    phone_norm = ''
    if phone:
        phone_norm = re.sub(r'[^\d]', '', clean_field(phone))
        # Keep last 7 digits if available (more unique than country/area codes which can be shared)
        if len(phone_norm) >= 7:
            phone_norm = phone_norm[-7:]
    
    # Create weighted hash components
    # Name is most important, then phone (which is usually unique), then address
    if name_norm:
        components = [f"name:{name_norm}"]
        if phone_norm:
            components.append(f"phone:{phone_norm}")
        if address_norm:
            components.append(f"addr:{address_norm}")
    elif phone_norm:
        # If no name, use phone as primary
        components = [f"phone:{phone_norm}"]
        if address_norm:
            components.append(f"addr:{address_norm}")
    else:
        # Last resort, use just address
        components = [f"addr:{address_norm}"]
    
    # Join components and create hash
    unique_string = "|".join(components)
    return hashlib.md5(unique_string.encode()).hexdigest()

def standardize_business_hours(business_hours):
    """
    Standardizes business hours formatting to ensure all days of the week are included
    with consistent formatting.
    """
    if not business_hours:
        return ''
        
    # Days of the week in order
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Initialize a dictionary to store hours for each day
    day_hours = {day: 'Hours not available' for day in days_of_week}
    
    # Split the input by semicolons or commas
    parts = re.split(r'[;,]', business_hours)
    
    # Process each part
    for part in parts:
        part = part.strip()
        if not part:
            continue
            
        # Try to identify which day this part refers to
        day_match = None
        for day in days_of_week:
            if part.lower().startswith(day.lower()):
                day_match = day
                # Extract hours portion (everything after the day name and colon)
                hours_match = re.search(f"{day}:?\s*(.*)", part, re.IGNORECASE)
                if hours_match:
                    hours = hours_match.group(1).strip()
                    day_hours[day] = hours
                break
    
    # Format the result with all days of the week
    formatted_hours = '; '.join([f"{day}: {day_hours[day]}" for day in days_of_week])
    return formatted_hours

class ScraperController:
    def __init__(self):
        self.stop_scrolling_requested = False
        self.stop_all_requested = False

    def request_stop_scrolling(self):
        self.stop_scrolling_requested = True

    def request_stop_all(self):
        self.stop_all_requested = True

controller = ScraperController()

async def enhanced_extract_from_website(url: str, main_context) -> Tuple[Dict[str, str], List[str]]:
    """
    Enhanced website extraction for social media links and emails with performance optimizations
    Returns: (social_media_dict, email_list)
    """
    if not is_valid_url(url):
        return {}, []
    
    # Extract domain for caching
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        # Check cache first - if we've already processed this domain, return cached results
        if domain in WEBSITE_EXTRACTION_CACHE:
            cached_result = WEBSITE_EXTRACTION_CACHE[domain]
            print(f"Using cached extraction for domain: {domain}")
            return cached_result
    except Exception as e:
        print(f"Error parsing URL for cache check: {e}")
        domain = None
    
    # Initialize results
    social_data = {platform: '' for platform in RobustSocialExtractor.SOCIAL_PATTERNS.keys()}
    emails = []
    visited_urls = set([url])  # Track visited URLs to avoid loops
    
    try:
        print(f"Extracting from: {url}")
        # Create a new browser context for website extraction in headless mode
        async with async_playwright() as p:
            website_browser = await p.chromium.launch(headless=True)  # Run website extraction in headless mode
            website_context = await website_browser.new_context()
            page = await website_context.new_page()
            
            try:
                # Configure browser for better performance
                await page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                })
                
                # Block unnecessary resources to speed up page loading
                await page.route('**/*.{png,jpg,jpeg,gif,svg,webp,mp4,webm,mp3,ogg,wav}', lambda route: route.abort())
                
                # Increased timeout for better reliability
                await page.goto(url, timeout=45000, wait_until='domcontentloaded')
                await asyncio.sleep(2)  # Short wait for dynamic content
                
                # Enhanced social icon extraction - This is critical for sites that use icon fonts or SVGs
                icon_social_links = await page.evaluate('''
                    () => {
                        const results = {};
                        const socialDomains = {
                            'facebook': ['facebook.com', 'fb.com', 'fb.me'],
                            'instagram': ['instagram.com', 'instagr.am'],
                            'twitter': ['twitter.com', 'x.com', 't.co'],
                            'linkedin': ['linkedin.com'],
                            'youtube': ['youtube.com', 'youtu.be'],
                            'tiktok': ['tiktok.com', 'vm.tiktok.com'],
                            'yelp': ['yelp.com'],
                            'whatsapp': ['wa.me', 'whatsapp.com'],
                            'pinterest': ['pinterest.com', 'pin.it']
                        };
                        
                        // Icon classes/attributes commonly used for social media
                        const iconSelectors = {
                            'facebook': ['fa-facebook', 'fa-facebook-f', 'fa-facebook-official', 'facebook', 'fb', 'icon-facebook'],
                            'instagram': ['fa-instagram', 'instagram', 'insta', 'ig', 'icon-instagram'],
                            'twitter': ['fa-twitter', 'fa-x-twitter', 'twitter', 'tweet', 'icon-twitter'],
                            'linkedin': ['fa-linkedin', 'fa-linkedin-in', 'linkedin', 'icon-linkedin'],
                            'youtube': ['fa-youtube', 'fa-youtube-play', 'youtube', 'yt', 'icon-youtube'],
                            'tiktok': ['fa-tiktok', 'tiktok', 'tt', 'icon-tiktok'],
                            'yelp': ['fa-yelp', 'yelp', 'icon-yelp'],
                            'whatsapp': ['fa-whatsapp', 'whatsapp', 'icon-whatsapp'],
                            'pinterest': ['fa-pinterest', 'fa-pinterest-p', 'pinterest', 'icon-pinterest']
                        };
                        
                        // Find all links
                        const links = document.querySelectorAll('a[href]');
                        
                        // Find social links by examining icon classes, attributes, and HTML content
                        links.forEach(link => {
                            // Skip if invalid href
                            if (!link.href || link.href.startsWith('javascript:') || link.href === '#') return;
                            
                            // Get all class names as a string
                            const classNames = Array.from(link.classList).join(' ').toLowerCase();
                            
                            // Get inner HTML
                            const innerHTML = link.innerHTML.toLowerCase();
                            
                            // Get aria-label if available (often contains platform name)
                            const ariaLabel = (link.getAttribute('aria-label') || '').toLowerCase();
                            
                            // Get title attribute if available (often contains platform name)
                            const title = (link.getAttribute('title') || '').toLowerCase();
                            
                            // Check if the URL is a social media domain
                            try {
                                const url = new URL(link.href);
                                const hostname = url.hostname.toLowerCase();
                                
                                // Direct domain match (highest confidence)
                                for (const [platform, domains] of Object.entries(socialDomains)) {
                                    if (domains.some(domain => hostname.includes(domain))) {
                                        results[platform] = link.href;
                                        continue;
                                    }
                                }
                            } catch (e) {
                                // Invalid URL, continue with other checks
                            }
                            
                            // For each social platform, check if this link might be for it
                            for (const [platform, keywords] of Object.entries(iconSelectors)) {
                                // Skip if we already found this platform
                                if (results[platform]) continue;
                                
                                // Check if any keyword matches in classes, innerHTML, aria-label, or title
                                const matchesKeyword = keywords.some(keyword => 
                                    classNames.includes(keyword) || 
                                    innerHTML.includes(keyword) || 
                                    ariaLabel.includes(keyword) || 
                                    title.includes(keyword)
                                );
                                
                                if (matchesKeyword) {
                                    // Check for icon elements inside the link
                                    const iconElement = link.querySelector('i, span.icon, .svg-icon, [class*="icon"], [class*="social"], svg');
                                    
                                    if (iconElement) {
                                        const iconClasses = Array.from(iconElement.classList).join(' ').toLowerCase();
                                        
                                        // Check if icon has a platform-specific class
                                        const hasIconClass = keywords.some(keyword => iconClasses.includes(keyword));
                                        
                                        if (hasIconClass || matchesKeyword) {
                                            results[platform] = link.href;
                                        }
                                    } else if (matchesKeyword) {
                                        // Even without an icon element, if link strongly suggests a platform
                                        results[platform] = link.href;
                                    }
                                }
                            }
                        });
                        
                        return results;
                    }
                ''')
                
                # Process icon-based social links (high confidence)
                for platform_lower, link in icon_social_links.items():
                    platform = platform_lower.capitalize()
                    if platform in social_data and link and RobustSocialExtractor._is_valid_social_url(link, platform):
                        social_data[platform] = link
                
                # Get all text content including meta tags and link tags
                all_text = await page.evaluate('''
                    () => {
                        // Get all text content
                        const getText = (el) => {
                            if (!el) return '';
                            return Array.from(el.childNodes)
                                .map(node => {
                                    if (node.nodeType === 3) return node.textContent;
                                    if (node.nodeType === 1) {
                                        const style = window.getComputedStyle(node);
                                        if (style.display === 'none' || style.visibility === 'hidden') return '';
                                        return getText(node);
                                    }
                                    return '';
                                })
                                .join(' ')
                                .replace(/\\s+/g, ' ')
                                .trim();
                        };
                        
                        // Get all meta tags content
                        const metaContent = Array.from(document.getElementsByTagName('meta'))
                            .map(meta => meta.content)
                            .join(' ');
                        
                        // Get all link tags content
                        const linkContent = Array.from(document.getElementsByTagName('link'))
                            .map(link => link.href)
                            .join(' ');
                        
                        return getText(document.body) + ' ' + metaContent + ' ' + linkContent;
                    }
                ''')
                
                # Enhanced link extraction from HTML with direct social media detection
                direct_social_links = await page.evaluate('''
                    () => {
                        const results = {};
                        const links = new Set();
                        
                        // Define domain patterns for social platforms
                        const socialDomains = {
                            'facebook': ['facebook.com', 'fb.com', 'fb.me'],
                            'instagram': ['instagram.com', 'instagr.am'],
                            'twitter': ['twitter.com', 'x.com', 't.co'],
                            'linkedin': ['linkedin.com'],
                            'youtube': ['youtube.com', 'youtu.be'],
                            'tiktok': ['tiktok.com', 'vm.tiktok.com'],
                            'yelp': ['yelp.com'],
                            'whatsapp': ['wa.me', 'whatsapp.com'],
                            'pinterest': ['pinterest.com', 'pin.it']
                        };
                        
                        // Get all links
                        const anchors = document.querySelectorAll('a[href]');
                        anchors.forEach(anchor => {
                            let href = anchor.href;
                            if (href && !href.startsWith('javascript:') && !href.startsWith('mailto:') && !href.startsWith('tel:')) {
                                try {
                                    const url = new URL(href);
                                    const hostname = url.hostname.toLowerCase();
                                    
                                    // Check if it's a social media link
                                    for (const [platform, domains] of Object.entries(socialDomains)) {
                                        if (domains.some(domain => hostname.includes(domain))) {
                                            results[platform] = url.href;
                                        }
                                    }
                                    
                                    // Add to general links
                                    links.add(url.href);
                                } catch (e) {
                                    // Skip invalid URLs
                                }
                            }
                        });
                        
                        // Get social from JSON-LD (highly reliable)
                        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                        scripts.forEach(script => {
                            try {
                                const data = JSON.parse(script.textContent);
                                if (data.sameAs && Array.isArray(data.sameAs)) {
                                    data.sameAs.forEach(url => {
                                        try {
                                            const parsedUrl = new URL(url);
                                            const hostname = parsedUrl.hostname.toLowerCase();
                                            
                                            for (const [platform, domains] of Object.entries(socialDomains)) {
                                                if (domains.some(domain => hostname.includes(domain))) {
                                                    results[platform] = url;
                                                }
                                            }
                                        } catch (e) {
                                            // Skip invalid URLs
                                        }
                                    });
                                }
                            } catch (e) {
                                // Skip invalid JSON
                            }
                        });
                        
                        // Find social media in social icons (very reliable)
                        const socialSelectors = [
                            '.social a', '.social-media a', '.social-links a',
                            '[class*="social"] a', '[id*="social"] a',
                            'footer a', '.footer a', '[class*="footer"] a'
                        ];
                        
                        socialSelectors.forEach(selector => {
                            document.querySelectorAll(selector).forEach(el => {
                                const href = el.href;
                                if (!href || href.startsWith('javascript:')) return;
                                
                                try {
                                    const url = new URL(href);
                                    const hostname = url.hostname.toLowerCase();
                                    
                                    for (const [platform, domains] of Object.entries(socialDomains)) {
                                        // Check URL domain
                                        if (domains.some(domain => hostname.includes(domain))) {
                                            results[platform] = url.href;
                                        }
                                        
                                        // Check element classes and content
                                        const elContent = el.innerHTML.toLowerCase();
                                        if (elContent.includes(platform) || 
                                            Array.from(el.classList).some(c => c.toLowerCase().includes(platform))) {
                                            for (const domain of domains) {
                                                if (hostname.includes(domain)) {
                                                    results[platform] = url.href;
                                                    break;
                                                }
                                            }
                                        }
                                        
                                        // Check for icons
                                        const img = el.querySelector('img, svg');
                                        if (img) {
                                            const alt = img.alt || '';
                                            const src = img.src || '';
                                            const classes = Array.from(img.classList).join(' ');
                                            
                                            if (alt.toLowerCase().includes(platform) || 
                                                src.toLowerCase().includes(platform) ||
                                                classes.toLowerCase().includes(platform)) {
                                                results[platform] = url.href;
                                            }
                                        }
                                    }
                                } catch (e) {
                                    // Skip invalid URLs
                                }
                            });
                        });
                        
                        return {
                            directSocial: results,
                            allLinks: Array.from(links)
                        };
                    }
                ''')
                
                # Process direct social links first (most reliable)
                for platform_lower, link in direct_social_links['directSocial'].items():
                    platform = platform_lower.capitalize()
                    if platform in social_data and link and RobustSocialExtractor._is_valid_social_url(link, platform):
                        social_data[platform] = link
                
                # Extract social media links using enhanced extractor
                text_social_data = RobustSocialExtractor.extract_social_from_text(all_text)
                
                # Merge with existing social data (don't overwrite direct findings)
                for platform, link in text_social_data.items():
                    if link and not social_data.get(platform):
                        social_data[platform] = link
                
                # Extract emails using enhanced method
                extracted_emails = RobustSocialExtractor.extract_emails_from_text(all_text)
                emails.extend(extracted_emails)
                
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
                
                # Check if we need to explore more pages
                social_count = sum(1 for v in social_data.values() if v)
                
                # Only explore secondary pages if we haven't found many social links on the main page
                # and if we found at least 1 email or social link on the main page (to confirm it's a valid business site)
                main_page_has_valid_data = social_count > 0 or len(emails) > 0
                
                if social_count < 4 and main_page_has_valid_data:
                    # Important pages to check - limit to top 3 most likely pages to have social links
                    priority_paths = [
                        '/contact', 
                        '/about',
                        '/social'
                    ]
                    
                    # Get the base URL
                    base_url = parsed_url.scheme + '://' + parsed_url.netloc
                    
                    # Keep track of how many pages we've checked
                    pages_checked = 0
                    max_pages_to_check = 2  # Limit to checking only 2 secondary pages
                    
                    # Check each important path
                    for path in priority_paths:
                        # Stop if we've found enough social links or checked enough pages
                        if sum(1 for v in social_data.values() if v) >= 4 or pages_checked >= max_pages_to_check:
                            break
                            
                        page_url = urljoin(base_url, path)
                        if page_url in visited_urls:
                            continue
                        
                        visited_urls.add(page_url)
                        pages_checked += 1
                        
                        try:
                            # Use a shorter timeout for these secondary pages
                            await page.goto(page_url, timeout=20000, wait_until='domcontentloaded')
                            await asyncio.sleep(1)
                            
                            # Extract page content
                            page_text = await page.evaluate('() => document.body.innerText')
                            
                            # Find social links in this page
                            page_social = RobustSocialExtractor.extract_social_from_text(page_text)
                            
                            # Extract direct social links
                            page_direct_social = await page.evaluate('''
                                () => {
                                    const results = {};
                                    const socialDomains = {
                                        'facebook': ['facebook.com', 'fb.com', 'fb.me'],
                                        'instagram': ['instagram.com', 'instagr.am'],
                                        'twitter': ['twitter.com', 'x.com', 't.co'],
                                        'linkedin': ['linkedin.com'],
                                        'youtube': ['youtube.com', 'youtu.be'],
                                        'tiktok': ['tiktok.com', 'vm.tiktok.com'],
                                        'yelp': ['yelp.com'],
                                        'whatsapp': ['wa.me', 'whatsapp.com'],
                                        'pinterest': ['pinterest.com', 'pin.it']
                                    };
                                    
                                    document.querySelectorAll('a[href]').forEach(anchor => {
                                        try {
                                            const href = anchor.href;
                                            if (!href || href.startsWith('javascript:')) return;
                                            
                                            const url = new URL(href);
                                            const hostname = url.hostname.toLowerCase();
                                            
                                            for (const [platform, domains] of Object.entries(socialDomains)) {
                                                if (domains.some(domain => hostname.includes(domain))) {
                                                    results[platform] = url.href;
                                                }
                                            }
                                        } catch (e) {
                                            // Skip invalid URLs
                                        }
                                    });
                                    
                                    return results;
                                }
                            ''')
                            
                            # Process direct social links
                            for platform_lower, link in page_direct_social.items():
                                platform = platform_lower.capitalize()
                                if platform in social_data and link and not social_data.get(platform):
                                    if RobustSocialExtractor._is_valid_social_url(link, platform):
                                        social_data[platform] = link
                            
                            # Add text-extracted social links
                            for platform, link in page_social.items():
                                if link and not social_data.get(platform):
                                    social_data[platform] = link
                            
                            # Extract any additional emails
                            page_emails = RobustSocialExtractor.extract_emails_from_text(page_text)
                            emails.extend(page_emails)
                            
                            # Also check for mailto links
                            page_mailto = await page.evaluate('''
                                () => {
                                    const links = document.querySelectorAll('a[href^="mailto:"]');
                                    return Array.from(links).map(link => link.href.replace('mailto:', '')).filter(email => email.includes('@'));
                                }
                            ''')
                            
                            for email in page_mailto:
                                if RobustSocialExtractor._is_valid_email(email.lower()):
                                    emails.append(email.lower())
                        
                        except Exception as e:
                            # Just continue to the next page if there's an error
                            continue
                    
                    # Check footer links directly on the main page
                    try:
                        # Go back to the main page
                        await page.goto(url, timeout=20000, wait_until='domcontentloaded')
                        
                        # Look specifically at footer links
                        footer_social = await page.evaluate('''
                            () => {
                                const results = {};
                                const socialDomains = {
                                    'facebook': ['facebook.com', 'fb.com', 'fb.me'],
                                    'instagram': ['instagram.com', 'instagr.am'],
                                    'twitter': ['twitter.com', 'x.com', 't.co'],
                                    'linkedin': ['linkedin.com'],
                                    'youtube': ['youtube.com', 'youtu.be'],
                                    'tiktok': ['tiktok.com', 'vm.tiktok.com'],
                                    'yelp': ['yelp.com'],
                                    'whatsapp': ['wa.me', 'whatsapp.com'],
                                    'pinterest': ['pinterest.com', 'pin.it']
                                };
                                
                                // Look for footer elements
                                const footers = document.querySelectorAll('footer, .footer, [class*="footer"], [id*="footer"]');
                                
                                footers.forEach(footer => {
                                    const links = footer.querySelectorAll('a[href]');
                                    links.forEach(link => {
                                        try {
                                            const href = link.href;
                                            if (!href || href.startsWith('javascript:')) return;
                                            
                                            const url = new URL(href);
                                            const hostname = url.hostname.toLowerCase();
                                            
                                            for (const [platform, domains] of Object.entries(socialDomains)) {
                                                if (domains.some(domain => hostname.includes(domain))) {
                                                    results[platform] = url.href;
                                                }
                                            }
                                        } catch (e) {
                                            // Skip invalid URLs
                                        }
                                    });
                                });
                                
                                return results;
                            }
                        ''')
                        
                        # Process footer social links
                        for platform_lower, link in footer_social.items():
                            platform = platform_lower.capitalize()
                            if platform in social_data and link and not social_data.get(platform):
                                if RobustSocialExtractor._is_valid_social_url(link, platform):
                                    social_data[platform] = link
                                    
                    except Exception as e:
                        pass  # Silent error
                
                # Remove duplicates from emails
                emails = list(set(emails))
                
                final_social_count = sum(1 for v in social_data.values() if v)
                print(f"Found: {final_social_count} social links, {len(emails)} emails")
                
                # Store in cache if we have a valid domain
                if domain:
                    WEBSITE_EXTRACTION_CACHE[domain] = (social_data, emails)
                    
                # Close the website browser
                await website_browser.close()
                    
                return social_data, emails
                
            except Exception as e:
                print(f"Error during extraction from {url}: {e}")
                try:
                    await website_browser.close()
                except:
                    pass
                return social_data, emails
                
    except Exception as e:
        print(f"Error creating page for {url}: {e}")
        return {}, []

async def scrape_google_maps(query, max_cards=200, controller=controller):
    data = []
    unique_hashes = set()
    
    # Track processed domains to avoid redundancy
    processed_website_domains = set()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Show the Google Maps browser window
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

        # Initialize tracking variables
        processed_titles = set()
        processed_cards = set()
        no_new_cards_scrolls = 0
        max_no_new_cards_scrolls = 12  # Reduced from 25 to make process faster
        consecutive_same_count = 0
        max_consecutive_same_count = 5  # Reduced from 8 to make process faster
        
        print(f'Starting incremental extraction (target: {max_cards} unique businesses)...')
        
        # Incremental extraction loop
        while len(data) < max_cards:
            if controller.stop_all_requested:
                print('All stopped by user during extraction.')
                break
                
            # Get currently visible cards
            visible_cards = await page.query_selector_all(results_selector)
            current_count = len(visible_cards)
            
            if current_count == 0:
                print("No cards found. Waiting for cards to appear...")
                await asyncio.sleep(2)
                continue
                
            print(f'Found {current_count} visible cards. Extracting new ones...')
            
            # Extract data from visible cards that haven't been processed yet
            new_cards_processed = 0
            
            for idx, card in enumerate(visible_cards):
                # Generate a unique card identifier based on position and text
                try:
                    card_id = await card.evaluate('el => el.outerHTML.length')  # Use HTML length as part of fingerprint
                    title_text = await card.evaluate('el => { const title = el.querySelector("div.fontHeadlineSmall"); return title ? title.textContent : ""; }')
                    card_fingerprint = f"{idx}_{card_id}_{title_text}"
                    
                    # Skip if we've already processed this card
                    if card_fingerprint in processed_cards:
                        continue
                        
                    # Mark as processed
                    processed_cards.add(card_fingerprint)
                    
                    # Skip if title matches something we've already processed
                    if title_text.lower() in processed_titles:
                        print(f'Skipping duplicate title: {title_text}')
                        continue
                        
                    # Process this card
                    print(f'Processing new card: {title_text}')
                    new_cards_processed += 1
                    
                    # Extraction logic - similar to the existing logic
                    try:
                        await card.click()
                        await page.wait_for_selector('h1, .fontHeadlineLarge, .DUwDvf', timeout=8000)
                        await asyncio.sleep(1)  # Reduced from 1.5 to 1 for speed
                        
                        # Get all text content for extraction
                        all_text = await page.evaluate('''
                            () => {
                                const getText = (el) => {
                                    if (!el) return '';
                                    return Array.from(el.childNodes)
                                        .map(node => {
                                            if (node.nodeType === 3) return node.textContent;
                                            if (node.nodeType === 1) {
                                                const style = window.getComputedStyle(node);
                                                if (style.display === 'none' || style.visibility === 'hidden') return '';
                                                return getText(node);
                                            }
                                            return '';
                                        })
                                        .join(' ')
                                        .replace(/\\s+/g, ' ')
                                        .trim();
                                };
                                return getText(document.body);
                            }
                        ''')
                        
                        # Try Gemini extraction first
                        gemini_data = await extract_with_gemini(all_text)
                        
                        if gemini_data:
                            name = gemini_data.get('Business Name', '')
                            business_type = gemini_data.get('Business Type', '')
                            address = gemini_data.get('Address', '')
                            phone = gemini_data.get('Phone Number', '')
                            email = gemini_data.get('Email', '')
                            website = gemini_data.get('Website', '')
                            opening_time = gemini_data.get('Opening Time', '')
                            closing_time = gemini_data.get('Closing Time', '')
                            business_hours = gemini_data.get('Business Hours', '')
                        else:
                            # Use manual extraction as fallback
                            # ... [rest of the extraction code remains unchanged]
                            name = await safe_text(page, 'h1, .fontHeadlineLarge, .DUwDvf, [data-item-id="title"]')
                            business_type = await safe_text(page, '.fontBodyMedium button[jsaction*="pane.rating.category"], .skqShb')
                            address = await safe_text(page, '[data-item-id="address"], .rogA2c, .Io6YTe.fontBodyMedium, .LrzXr')
                            phone = await safe_text(page, '[data-item-id="phone"], .Io6YTe.fontBodyMedium, .UsdlK')
                            
                            # Extract opening and closing times
                            opening_time = ''
                            closing_time = ''
                            business_hours = ''
                            
                            # Try to extract the hours information
                            hours_data = await page.evaluate('''
                                () => {
                                    try {
                                        // Find the hours container with more comprehensive selectors
                                        const hoursContainer = document.querySelector('[data-item-id="oh"], .y0skZc, .t39EBf, [aria-label*="hour"], [aria-label*="open"], .IDyq0e, [data-ved][jsaction][role="button"][data-url*="hour"], .OMl5r');
                                        
                                        if (hoursContainer) {
                                            // First check if today's hours are shown
                                            const todayHours = document.querySelector('.fontBodyMedium[aria-label*="open"], .fontBodyMedium[aria-label*="close"], .ZDu9vd, .y0skZc, .OMl5r');
                                            
                                            let openingTime = '';
                                            let closingTime = '';
                                            let workingHours = '';
                                            
                                            if (todayHours) {
                                                const hoursText = todayHours.textContent.trim();
                                                workingHours = hoursText;
                                                
                                                // Extract hours using regex
                                                const hoursMatch = hoursText.match(/(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\s*[–-]\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))/);
                                                
                                                if (hoursMatch) {
                                                    openingTime = hoursMatch[1].trim();
                                                    closingTime = hoursMatch[2].trim();
                                                } else {
                                                    // Try another regex pattern for "Opens at X"
                                                    const opensMatch = hoursText.match(/Opens\s+(?:at\s+)?(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))/i);
                                                    if (opensMatch) {
                                                        openingTime = opensMatch[1].trim();
                                                    }
                                                    
                                                    // Try another pattern for "Closes at X"
                                                    const closesMatch = hoursText.match(/Closes\s+(?:at\s+)?(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))/i);
                                                    if (closesMatch) {
                                                        closingTime = closesMatch[1].trim();
                                                    }
                                                }
                                                
                                                // Check for special cases
                                                if (hoursText.includes('24 hours') || hoursText.includes('Open 24 hours')) {
                                                    openingTime = '12:00 AM';
                                                    closingTime = '11:59 PM';
                                                    workingHours = 'Open 24 hours';
                                                } else if (hoursText.includes('Closed')) {
                                                    workingHours = 'Closed';
                                                }
                                            }
                                            
                                            // Always return shouldClick: true to get the full weekly schedule
                                            return {
                                                opening: openingTime,
                                                closing: closingTime,
                                                workingHours: workingHours,
                                                shouldClick: true // Always click to get full schedule
                                            };
                                        }
                                        
                                        // If no hours container found, still try to click for hours
                                        return { opening: '', closing: '', workingHours: '', shouldClick: true };
                                    } catch (e) {
                                        console.error('Error in initial hours detection:', e);
                                        return { opening: '', closing: '', workingHours: '', shouldClick: true };
                                    }
                                }
                            ''')
                            
                            # Initialize business hours
                            business_hours = ''
                            
                            # If we need to click the hours button to get more information
                            if hours_data.get('shouldClick', False):
                                try:
                                    # Click on hours button if it exists
                                    hours_button = await page.query_selector('[data-item-id="oh"], .y0skZc, .t39EBf, [aria-label*="hour"], [aria-label*="open"], .IDyq0e, [data-ved][jsaction][role="button"][data-url*="hour"]')
                                    if hours_button:
                                        await hours_button.click()
                                        await asyncio.sleep(0.8)  # Reduced from 1 to 0.8 for speed
                                        
                                        # Extract hours from the expanded view
                                        expanded_hours = await page.evaluate('''
                                            () => {
                                                try {
                                                    // Find the hours container with more comprehensive selectors
                                                    const daysContainer = document.querySelector('.dRgULb, [aria-label*="hour"] div[jsaction*="pane.openhours"], div[role="region"][aria-label*="hour"], .t39EBf, .MmmeYe');
                                                    if (!daysContainer) return { opening: '', closing: '', workingHours: '' };
                                                    
                                                    // Get today's date info for finding current day
                                                    const today = new Date().toLocaleDateString('en-US', { weekday: 'long' });
                                                    
                                                    // More comprehensive selector for day rows
                                                    const dayRows = Array.from(daysContainer.querySelectorAll('tr, [role="row"], .mWUmld, .y0skZc div, div[jsaction*="pane.openhours"] div, .t39EBf div, .MmmeYe div'));
                                                    
                                                    // Days of the week for standardizing output
                                                    const daysOfWeek = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
                                                    
                                                    // Build full working hours schedule with better formatting
                                                    const fullSchedule = [];
                                                    const formattedSchedule = {};
                                                    let openingTime = '';
                                                    let closingTime = '';
                                                    let targetRow = null;
                                                    
                                                    // Process each day row
                                                    for (const row of dayRows) {
                                                        const rowText = row.textContent.trim();
                                                        
                                                        // Skip rows without day information
                                                        if (!daysOfWeek.some(day => rowText.includes(day))) {
                                                            continue;
                                                        }
                                                        
                                                        // Determine which day this row represents
                                                        let currentDay = '';
                                                        for (const day of daysOfWeek) {
                                                            if (rowText.includes(day)) {
                                                                currentDay = day;
                                                                break;
                                                            }
                                                        }
                                                        
                                                        if (!currentDay) continue;
                                                        
                                                        // Extract hours using various patterns
                                                        let hours = '';
                                                        
                                                        // Pattern 1: Standard hours format (9:00 AM - 5:00 PM)
                                                        const standardHoursMatch = rowText.match(new RegExp(`${currentDay}\\s*(.+)`));
                                                        if (standardHoursMatch) {
                                                            hours = standardHoursMatch[1].trim();
                                                        }
                                                        
                                                        // Handle special cases like "Closed" or "Open 24 hours"
                                                        if (rowText.includes('Closed')) {
                                                            hours = 'Closed';
                                                        } else if (rowText.includes('Open 24 hours')) {
                                                            hours = 'Open 24 hours';
                                                        } else if (rowText.includes('24 hours')) {
                                                            hours = 'Open 24 hours';
                                                        }
                                                        
                                                        // Extract opening/closing times if this is today
                                                        if (rowText.includes(today)) {
                                                            targetRow = row;
                                                            const hoursMatch = rowText.match(/(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\s*[–-]\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))/);
                                                            if (hoursMatch) {
                                                                openingTime = hoursMatch[1].trim();
                                                                closingTime = hoursMatch[2].trim();
                                                            } else if (hours === 'Open 24 hours') {
                                                                openingTime = '12:00 AM';
                                                                closingTime = '11:59 PM';
                                                            }
                                                        }
                                                        
                                                        // Store in formatted schedule
                                                        formattedSchedule[currentDay] = hours;
                                                        
                                                        // Also add to full schedule array for backward compatibility
                                                        fullSchedule.push(`${currentDay}: ${hours}`);
                                                    }
                                                    
                                                    // If we didn't find today, use the first day as default
                                                    if (!targetRow && Object.keys(formattedSchedule).length > 0) {
                                                        const firstDay = Object.keys(formattedSchedule)[0];
                                                        const firstDayHours = formattedSchedule[firstDay];
                                                        
                                                        if (firstDayHours !== 'Closed' && firstDayHours !== 'Open 24 hours') {
                                                            const hoursMatch = firstDayHours.match(/(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\s*[–-]\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))/);
                                                            if (hoursMatch) {
                                                                openingTime = hoursMatch[1].trim();
                                                                closingTime = hoursMatch[2].trim();
                                                            }
                                                        } else if (firstDayHours === 'Open 24 hours') {
                                                            openingTime = '12:00 AM';
                                                            closingTime = '11:59 PM';
                                                        }
                                                    }
                                                    
                                                    // Format the weekly schedule in a consistent way
                                                    const formattedWeeklySchedule = daysOfWeek
                                                        .map(day => `${day}: ${formattedSchedule[day] || 'Hours not available'}`)
                                                        .join('; ');
                                                    
                                                    return {
                                                        opening: openingTime,
                                                        closing: closingTime,
                                                        workingHours: formattedWeeklySchedule || fullSchedule.join('; ')
                                                    };
                                                } catch (e) {
                                                    console.error('Error extracting hours:', e);
                                                    return { opening: '', closing: '', workingHours: '' };
                                                }
                                            }
                                        ''')
                                        
                                        opening_time = expanded_hours.get('opening', '')
                                        closing_time = expanded_hours.get('closing', '')
                                        business_hours = expanded_hours.get('workingHours', '')
                                except Exception as e:
                                    print(f"Error extracting expanded hours: {e}")
                            else:
                                # Use the hours data we already got
                                opening_time = hours_data.get('opening', '')
                                closing_time = hours_data.get('closing', '')
                                business_hours = hours_data.get('workingHours', '')
                            
                            if not phone:
                                phone_matches = re.findall(r'(\+?\d[\d\s\-().]{8,}\d)', all_text)
                                phone = phone_matches[0] if phone_matches else ''
                            
                            # Enhanced website extraction
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
                            
                            # Enhanced email extraction
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
                        opening_time = clean_field(opening_time)
                        closing_time = clean_field(closing_time)
                        business_hours = clean_field(business_hours)
                        
                        # Standardize business hours format
                        business_hours = standardize_business_hours(business_hours)
                        
                        # Skip if no business name (invalid business)
                        if not name.strip():
                            print(f'Skipping business with no name')
                            continue
                            
                        # Check early if we have a duplicate name/address (improves performance)
                        name_address_match = False
                        for existing in data:
                            if name.lower() == existing['Business Name'].lower() and (
                                not address or not existing['Address'] or 
                                address.lower() == existing['Address'].lower() or
                                (address and existing['Address'] and address.split(',')[0].lower() == existing['Address'].split(',')[0].lower())
                            ):
                                print(f'Duplicate by name/address: {name}')
                                name_address_match = True
                                break
                        
                        if name_address_match:
                            continue
                            
                        # Mark this title as processed
                        if title_text:
                            processed_titles.add(title_text.lower())
                            
                        # Generate business hash for duplicate detection
                        business_hash = create_business_hash(name, address, phone)
                        
                        # Skip if it's a duplicate (hash already exists)
                        if business_hash in unique_hashes:
                            print(f'Duplicate: {name}')
                            continue
                        
                        # Normalize website URL
                        if website and not website.startswith('http'):
                            website = normalize_url(website)
                        
                        # Get website domain for cache lookup
                        website_domain = None
                        if website:
                            try:
                                parsed_url = urlparse(website)
                                website_domain = parsed_url.netloc.lower()
                            except Exception as e:
                                print(f"Error parsing website URL: {e}")
                                website_domain = None
                        
                        # Initialize social media data
                        social_media_data = {platform: '' for platform in RobustSocialExtractor.SOCIAL_PATTERNS.keys()}
                        found_emails = [email] if email else []
                        
                        # Enhanced social media extraction from Google Maps page
                        maps_social_data = RobustSocialExtractor.extract_social_from_text(all_text)
                        for platform, link in maps_social_data.items():
                            if link:
                                social_media_data[platform] = link
                        
                        # Enhanced website extraction with caching
                        if website and is_valid_url(website):
                            # Check if we've already processed this domain before
                            if website_domain and website_domain in processed_website_domains:
                                print(f'Using cached extraction for domain: {website_domain}')
                                if website_domain in WEBSITE_EXTRACTION_CACHE:
                                    cached_social, cached_emails = WEBSITE_EXTRACTION_CACHE[website_domain]
                                    
                                    # Merge with cached social data (cached takes precedence for non-empty values)
                                    for platform, link in cached_social.items():
                                        if link and not social_media_data.get(platform):
                                            social_media_data[platform] = link
                                    
                                    # Add cached emails
                                    found_emails.extend(cached_emails)
                            else:
                                print(f'Enhanced extraction from website: {website}')
                                try:
                                    website_social_data, website_emails = await enhanced_extract_from_website(website, context)
                                    
                                    # Mark domain as processed to avoid future redundant processing
                                    if website_domain:
                                        processed_website_domains.add(website_domain)
                                    
                                    # Merge social media data (website takes precedence)
                                    for platform, link in website_social_data.items():
                                        if link and not social_media_data.get(platform):
                                            social_media_data[platform] = link
                                    
                                    # Add website emails
                                    found_emails.extend(website_emails)
                                except Exception as e:
                                    print(f'Error in website extraction: {e}')
                        
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
                            'Opening Time': opening_time,
                            'Closing Time': closing_time,
                            'Business Hours': business_hours,
                            'Facebook': social_media_data['Facebook'],
                            'Instagram': social_media_data['Instagram'],
                            'Twitter': social_media_data['Twitter'],
                            'LinkedIn': social_media_data['LinkedIn'],
                            'YouTube': social_media_data['YouTube'],
                            'TikTok': social_media_data['TikTok'],
                            'Yelp': social_media_data['Yelp'],
                            'WhatsApp': social_media_data['WhatsApp'],
                            'Pinterest': social_media_data['Pinterest'],
                        }
                        
                        # Add unique hash to set
                        unique_hashes.add(business_hash)
                        
                        # Add to data
                        data.append(business_data)
                        
                        # Count social media platforms found
                        social_count = sum(1 for platform, link in social_media_data.items() if link)
                        print(f'UNIQUE #{len(data)}/{max_cards} | {name} | Email: {bool(final_email)} | Social: {social_count}/9')
                        
                        # Break the card processing loop if we've reached the target
                        if len(data) >= max_cards:
                            print(f'Reached target of {max_cards} unique businesses!')
                            break
                            
                    except Exception as e:
                        print(f'Error processing card: {e}')
                        continue
                except Exception as e:
                    print(f'Error getting card info: {e}')
                    continue
            
            # Check if we need to scroll more
            if len(data) >= max_cards:
                print(f'Target reached: {len(data)}/{max_cards} businesses')
                break
                
            if controller.stop_scrolling_requested:
                print(f'Scrolling stopped by user. Extracted {len(data)} businesses so far.')
                break
                
            if new_cards_processed == 0:
                no_new_cards_scrolls += 1
                consecutive_same_count += 1
                print(f'No new unique cards found in this batch. Scroll attempt {no_new_cards_scrolls}/{max_no_new_cards_scrolls}')
                
                if consecutive_same_count >= max_consecutive_same_count:
                    print(f'No new cards found after {consecutive_same_count} consecutive attempts. Trying aggressive scroll...')
                    # Aggressive scroll to try to load more results
                    try:
                        for _ in range(5):
                            await page.evaluate('''
                                () => {
                                    const mainContainer = document.querySelector('div[role="main"] div[aria-label][tabindex="0"]');
                                    if (mainContainer) {
                                        mainContainer.scrollBy(0, 3000);
                                    }
                                    window.scrollBy(0, 2000);
                                }
                            ''')
                            await page.keyboard.press('End')
                            await page.mouse.wheel(0, 3000)
                            await asyncio.sleep(0.3)
                        await asyncio.sleep(1.5)
                    except Exception as e:
                        print(f"Aggressive scroll failed: {e}")
                
                if no_new_cards_scrolls >= max_no_new_cards_scrolls:
                    print(f'Maximum scroll attempts reached. Stopping with {len(data)} businesses.')
                    break
            else:
                print(f'Found {new_cards_processed} new businesses in this batch')
                no_new_cards_scrolls = 0
                consecutive_same_count = 0
            
            # Scroll to get more cards
            try:
                # Scroll down to load more results
                await page.evaluate('''
                    (selector) => {
                        const scrollable = document.querySelector(selector);
                        if (scrollable) {
                            scrollable.scrollBy(0, 1500);
                        } else {
                            window.scrollBy(0, 1500);
                        }
                    }
                ''', scrollable_selector)
                
                # Additional scrolling with key and mouse
                await page.keyboard.press('PageDown')
                await page.mouse.wheel(0, 1000)
                
                # Short wait for new cards to load
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f'Error scrolling: {e}')
                # Try alternative scrolling
                try:
                    await page.evaluate('window.scrollBy(0, 1500)')
                    await asyncio.sleep(1)
                except:
                    pass

        # Print statistics before closing
        print(f"\nExtraction complete!")
        print(f"Total unique businesses extracted: {len(data)}")
        print(f"Total domains in extraction cache: {len(WEBSITE_EXTRACTION_CACHE)}")
        print(f"Total processed website domains: {len(processed_website_domains)}")
        
        await browser.close()
        
        # Return exactly max_cards businesses or all we could find
        return data[:max_cards]

def run_scraper_from_ui(query, max_cards, status_label, button, stop_scroll_button, stop_all_button):
    def task():
        controller.stop_scrolling_requested = False
        controller.stop_all_requested = False
        try:
            requested_cards = max_cards.get()
            status_label.config(text=f'Enhanced scraping in progress for {requested_cards} businesses...')
            results = asyncio.run(scrape_google_maps(query.get(), requested_cards, controller=controller))
            export_to_excel(results, OUTPUT_FILE)
            status_label.config(text='Enhanced extraction complete!')
            
            # Show detailed success message
            social_count = sum(1 for business in results for platform in ['Facebook', 'Instagram', 'Twitter', 'LinkedIn', 'YouTube', 'TikTok', 'Yelp', 'WhatsApp', 'Pinterest'] if business.get(platform))
            email_count = sum(1 for business in results if business.get('Email'))
            website_count = sum(1 for business in results if business.get('Website'))
            
            # Check if we got exactly what the user requested
            if len(results) < requested_cards:
                message = f'''Enhanced Scraping Complete!
                
Could only extract {len(results)} unique businesses out of the {requested_cards} requested.
(This may be due to limited search results or duplicates)

• {email_count} businesses with emails ({email_count/len(results):.1%})
• {website_count} businesses with websites ({website_count/len(results):.1%})  
• {social_count} total social media links found
                
Data exported to: {OUTPUT_FILE}'''
            else:
                message = f'''Enhanced Scraping Complete!
                
Successfully extracted {len(results)} businesses - exactly as requested!
• {email_count} businesses with emails ({email_count/len(results):.1%})
• {website_count} businesses with websites ({website_count/len(results):.1%})  
• {social_count} total social media links found
                
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
    
    max_cards_label = ttk.Label(cards_frame, text=f'Number of businesses to extract (max {MAX_BUSINESSES}):')
    max_cards_label.pack(side=tk.LEFT)
    
    max_cards_var = tk.IntVar(value=80)
    max_cards_entry = ttk.Entry(cards_frame, textvariable=max_cards_var, width=10, font=('Arial', 11))
    max_cards_entry.pack(side=tk.LEFT, padx=(10,0))

    # Create a tooltip for the max cards entry
    def create_tooltip(widget, text):
        def enter(event):
            x, y, _, _ = widget.bbox("insert")
            x += widget.winfo_rootx() + 25
            y += widget.winfo_rooty() + 20
            
            # Create a toplevel window
            tooltip = tk.Toplevel(widget)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{x}+{y}")
            
            label = ttk.Label(tooltip, text=text, justify=tk.LEFT,
                             background="#ffffe0", relief="solid", borderwidth=1,
                             font=("Arial", "9", "normal"), padding=5)
            label.pack(ipadx=1)
            
            widget._tooltip = tooltip
            
        def leave(event):
            if hasattr(widget, "_tooltip"):
                widget._tooltip.destroy()
            
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)

    create_tooltip(max_cards_entry, 
                   f"Enter the EXACT number of businesses you want to extract (1-{MAX_BUSINESSES}).\n"
                   f"The scraper will attempt to extract precisely this number of unique businesses.")    # Add a slider for easier selection with integer steps
    style = ttk.Style()
    style.configure("Custom.Horizontal.TScale", sliderthickness=25)  # Increase slider thickness
    
    max_cards_slider = ttk.Scale(cards_frame, from_=1, to=MAX_BUSINESSES,
                                variable=max_cards_var, orient=tk.HORIZONTAL, length=300,  # Increased length
                                style="Custom.Horizontal.TScale")
    
    # Force integer values
    def on_slider_change(event):
        current_value = max_cards_slider.get()
        max_cards_var.set(round(current_value))  # Round to nearest integer
    
    max_cards_slider.bind("<Motion>", on_slider_change)  # Update while dragging
    max_cards_slider.bind("<ButtonRelease-1>", on_slider_change)  # Update after release
    
    max_cards_slider.pack(side=tk.LEFT, padx=(10,0), pady=10)  # Added vertical padding

    def validate_max_cards(*args):
        try:
            value = max_cards_var.get()
            if value > MAX_BUSINESSES:
                max_cards_var.set(MAX_BUSINESSES)
                messagebox.showwarning('Input Limit', f'Maximum number of businesses is {MAX_BUSINESSES}. The value has been set to {MAX_BUSINESSES}.')
            elif value < 1:
                max_cards_var.set(1)
                messagebox.showwarning('Input Limit', 'Minimum number of businesses is 1. The value has been set to 1.')
        except:
            max_cards_var.set(80)

    max_cards_var.trace('w', validate_max_cards)
    
    
    # Features section
    features_frame = ttk.LabelFrame(main_frame, text='Enhanced Features', padding=15)
    features_frame.pack(fill=tk.X, pady=(0,15))
    
    features_text = f'''✓ Advanced social media link extraction (9 platforms)
✓ Robust email detection with validation
✓ Website crawling for additional data
✓ Duplicate business detection
✓ Enhanced data validation
✓ Comprehensive Excel export with statistics
✓ Extract EXACTLY the number of businesses you specify (up to {MAX_BUSINESSES})'''
    
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
• The scraper will extract EXACTLY the number of businesses you specify (if available)
• For larger numbers (>100), the extraction process will take longer
• The application automatically filters out duplicates to ensure unique results
• You can stop scrolling early and still extract data from already loaded businesses
• Use the slider or directly enter the number of businesses you want to extract'''
    
    help_label = ttk.Label(help_frame, text=help_text, font=('Arial', 9), foreground='#666666')
    help_label.pack(anchor=tk.W)
    
    root.mainloop()

# Add the export_to_excel function that was defined in auth.py but missing in app.py
def export_to_excel(data, filename):
    """Enhanced Excel export with better formatting and duplicate prevention"""
    try:
        df = pd.DataFrame(data)
        
        # Reorder columns for better presentation
        column_order = [
            'Business Name', 'Business Type', 'Address', 'Phone Number', 
            'Email', 'Website', 'Opening Time', 'Closing Time', 'Business Hours', 'Facebook', 'Instagram', 'Twitter', 
            'LinkedIn', 'YouTube', 'TikTok', 'Yelp', 'WhatsApp', 'Pinterest'
        ]
        
        # Only include columns that exist in the data
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        # Clean and normalize data to prevent duplicates
        for col in df.columns:
            if col in ['Business Name', 'Address', 'Phone Number', 'Email', 'Website']:
                # Convert to string and clean
                df[col] = df[col].astype(str).apply(lambda x: clean_field(x))
                
                # Normalize phone numbers
                if col == 'Phone Number':
                    df[col] = df[col].apply(lambda x: re.sub(r'[^\d+]', '', x))
                
                # Normalize emails
                if col == 'Email':
                    df[col] = df[col].apply(lambda x: x.lower().strip())
                
                # Normalize websites
                if col == 'Website':
                    df[col] = df[col].apply(lambda x: normalize_url(x))
        
        # Create a composite key for duplicate detection (improved address normalization)
        df['composite_key'] = df.apply(lambda row: create_business_hash(
            row['Business Name'], 
            row['Address'], 
            row['Phone Number']
        ), axis=1)
        
        # Remove duplicates based on composite key
        initial_count = len(df)
        df = df.drop_duplicates(subset=['composite_key'], keep='first')
        removed_count = initial_count - len(df)
        
        # Remove the composite key column
        df = df.drop('composite_key', axis=1)
        
        # Add social media completeness metrics
        social_platforms = ['Facebook', 'Instagram', 'Twitter', 'LinkedIn', 'YouTube', 'TikTok', 'Yelp', 'WhatsApp', 'Pinterest']
        
        # Count social platforms per business
        df['Social_Count'] = df[social_platforms].apply(lambda row: sum(1 for x in row if x != ""), axis=1)
        
        # Export to Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Remove analysis columns before exporting
            export_df = df.drop('Social_Count', axis=1)
            export_df.to_excel(writer, index=False, sheet_name='Businesses')
            
            # Create summary sheet
            summary_data = {
                'Metric': [
                    'Total Businesses', 
                    'Duplicates Removed',
                    'Businesses with Email',
                    'Businesses with Website',
                    'Businesses with 0 Social Platforms',
                    'Businesses with 1-3 Social Platforms',
                    'Businesses with 4-6 Social Platforms',
                    'Businesses with 7+ Social Platforms',
                    'Average Social Platforms per Business'
                ],
                'Value': [
                    len(df),
                    removed_count,
                    len(df[df['Email'] != ""]),
                    len(df[df['Website'] != ""]),
                    len(df[df['Social_Count'] == 0]),
                    len(df[(df['Social_Count'] >= 1) & (df['Social_Count'] <= 3)]),
                    len(df[(df['Social_Count'] >= 4) & (df['Social_Count'] <= 6)]),
                    len(df[df['Social_Count'] >= 7]),
                    df['Social_Count'].mean()
                ],
                'Percentage': [
                    '100%',
                    f"{removed_count/initial_count:.1%}" if initial_count > 0 else "0%",
                    f"{len(df[df['Email'] != ''])/len(df):.1%}" if len(df) > 0 else "0%",
                    f"{len(df[df['Website'] != ''])/len(df):.1%}" if len(df) > 0 else "0%",
                    f"{len(df[df['Social_Count'] == 0])/len(df):.1%}" if len(df) > 0 else "0%",
                    f"{len(df[(df['Social_Count'] >= 1) & (df['Social_Count'] <= 3)])/len(df):.1%}" if len(df) > 0 else "0%",
                    f"{len(df[(df['Social_Count'] >= 4) & (df['Social_Count'] <= 6)])/len(df):.1%}" if len(df) > 0 else "0%",
                    f"{len(df[df['Social_Count'] >= 7])/len(df):.1%}" if len(df) > 0 else "0%",
                    "N/A"
                ]
            }
            
            # Platform-specific stats
            for platform in social_platforms:
                if platform in df.columns:
                    platform_count = len(df[df[platform] != ""])
                    summary_data['Metric'].append(f'Businesses with {platform}')
                    summary_data['Value'].append(platform_count)
                    summary_data['Percentage'].append(f"{platform_count/len(df):.1%}" if len(df) > 0 else "0%")
            
            # Create summary dataframe and export
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, index=False, sheet_name='Summary')
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Businesses']
            summary_worksheet = writer.sheets['Summary']
            
            # Auto-adjust column widths for main sheet
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
                
            # Format summary sheet
            for column in summary_worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 30)
                summary_worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f'Enhanced export complete: {len(df)} businesses to {filename}')
        
        # Print summary statistics
        print(f'\n=== EXTRACTION SUMMARY ===')
        print(f'Total unique businesses: {len(df)}')
        print(f'Duplicates removed: {removed_count}')
        print(f'Businesses with emails: {len(df[df["Email"] != ""])}')
        print(f'Businesses with websites: {len(df[df["Website"] != ""])}')
        
        # Social media statistics
        for platform in social_platforms:
            if platform in df.columns:
                count = len(df[df[platform] != ""])
                print(f'Businesses with {platform}: {count} ({count/len(df):.1%})')
        
        # Social count distribution
        print(f'\nSocial platform distribution:')
        print(f'0 platforms: {len(df[df["Social_Count"] == 0])} ({len(df[df["Social_Count"] == 0])/len(df):.1%})')
        print(f'1-3 platforms: {len(df[(df["Social_Count"] >= 1) & (df["Social_Count"] <= 3)])} ({len(df[(df["Social_Count"] >= 1) & (df["Social_Count"] <= 3)])/len(df):.1%})')
        print(f'4-6 platforms: {len(df[(df["Social_Count"] >= 4) & (df["Social_Count"] <= 6)])} ({len(df[(df["Social_Count"] >= 4) & (df["Social_Count"] <= 6)])/len(df):.1%})')
        print(f'7+ platforms: {len(df[df["Social_Count"] >= 7])} ({len(df[df["Social_Count"] >= 7])/len(df):.1%})')
        print(f'Average platforms per business: {df["Social_Count"].mean():.1f}')
    except Exception as e:
        print(f'Error exporting to Excel: {e}')

if __name__ == '__main__':
    print("=== Enhanced Google Maps Business Scraper v2.0 ===")
    print("Features: Advanced social media extraction, robust email detection, website crawling")
    print("Supported platforms: Facebook, Instagram, Twitter, LinkedIn, YouTube, TikTok, Yelp, WhatsApp, Pinterest")
    print("=" * 80)
    launch_ui()
