import logging
import re
import asyncio
import aiohttp
import os
from typing import Optional, Dict, List, Any
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler
from telegram.constants import ParseMode
import trafilatura

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "8465346144:AAGSHC77UkXVZZTUscbYItvJxgQbBxmFcWo")
BOT_USERNAME = os.getenv("BOT_USERNAME", "@tg_workbot")

# URL shorteners list
SHORTENERS = [
    'cutt.ly', 'spoo.me', 'amzn.to', 'amzn-to.co', 'fkrt.cc', 'bitli.in', 
    'da.gd', 'wishlink.com', 'bit.ly', 'tinyurl.com', 'short.link', 
    'ow.ly', 'is.gd', 't.co', 'goo.gl', 'rb.gy', 'tiny.cc', 'v.gd',
    'x.co', 'buff.ly', 'short.gy', 'shorte.st', 'adf.ly', 'bc.vc',
    'tinycc.com', 'shorturl.at', 'clck.ru', '0rz.tw', '1link.in'
]

# Gender detection patterns
GENDER_KEYWORDS = {
    'Men': [
        r'\bmen\b', r"\bmen's\b", r'\bmale\b', r'\bboy\b', r'\bboys\b', 
        r'\bgents\b', r'\bgentleman\b', r'\bmasculine\b', r'\bmans\b', 
        r'\bguys\b', r'\bhim\b', r'\bhis\b', r'\bfather\b', r'\bdad\b'
    ],
    'Women': [
        r'\bwomen\b', r"\bwomen's\b", r'\bfemale\b', r'\bgirl\b', r'\bgirls\b', 
        r'\bladies\b', r'\blady\b', r'\bfeminine\b', r'\bwomens\b', 
        r'\bher\b', r'\bshe\b', r'\bmother\b', r'\bmom\b'
    ],
    'Kids': [
        r'\bkids\b', r'\bchildren\b', r'\bchild\b', r'\bbaby\b', r'\binfant\b', 
        r'\btoddler\b', r'\bteen\b', r'\bteenage\b', r'\bjunior\b', r'\byouth\b'
    ]
}

# Quantity patterns
QUANTITY_PATTERNS = [
    r'pack\s+of\s+(\d+)',
    r'(\d+)\s*pack',
    r'set\s+of\s+(\d+)',
    r'(\d+)\s*pcs?',
    r'(\d+)\s*pieces?',
    r'(\d+)\s*kg',
    r'(\d+)\s*g(?:ram)?s?',
    r'(\d+)\s*ml',
    r'(\d+)\s*l(?:itr?e)?s?',
    r'combo\s+(\d+)',
    r'(\d+)\s*pair',
    r'multipack\s+(\d+)',
    r'quantity\s*:\s*(\d+)'
]

# Known brands
KNOWN_BRANDS = [
    'Lakme', 'Maybelline', 'L\'Oreal', 'MAC', 'Revlon', 'Nykaa', 'Colorbar',
    'Nike', 'Adidas', 'Puma', 'Reebok', 'Converse', 'Vans',
    'Samsung', 'Apple', 'OnePlus', 'Xiaomi', 'Realme', 'Oppo', 'Vivo',
    'Zara', 'H&M', 'Forever21', 'Mango', 'Uniqlo',
    'Mamaearth', 'Wow', 'Biotique', 'Himalaya', 'Patanjali',
    'Jockey', 'Calvin Klein', 'Tommy Hilfiger', 'Allen Solly'
]

class SmartLinkProcessor:
    """Smart link detection and processing"""

    @staticmethod
    def extract_all_links(text: str) -> List[str]:
        """Extract all URLs from text"""
        if not text:
            return []

        urls = []

        # Standard HTTP/HTTPS URLs
        standard_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+(?=[.\s]|$)'
        urls.extend(re.findall(standard_pattern, text, re.IGNORECASE))

        # URLs with www
        www_pattern = r'www\.[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\.[a-zA-Z]{2,}(?:/[^\s<>"{}|\\^`\[\]]*)?'
        potential_urls = re.findall(www_pattern, text)
        for url in potential_urls:
            if not url.startswith('http'):
                url = 'https://' + url
            urls.append(url)

        # Platform-specific domains
        domain_patterns = [
            r'(?:amazon\.in|amazon\.com)/[^\s<>"{}|\\^`\[\]]+',
            r'(?:flipkart\.com)/[^\s<>"{}|\\^`\[\]]+',
            r'(?:meesho\.com)/[^\s<>"{}|\\^`\[\]]+',
            r'(?:myntra\.com)/[^\s<>"{}|\\^`\[\]]+',
            r'(?:ajio\.com)/[^\s<>"{}|\\^`\[\]]+',
            r'(?:snapdeal\.com)/[^\s<>"{}|\\^`\[\]]+'
        ]

        for pattern in domain_patterns:
            found_urls = re.findall(pattern, text, re.IGNORECASE)
            for url in found_urls:
                if not url.startswith('http'):
                    url = 'https://' + url
                urls.append(url)

        # Shortened URLs
        short_pattern = r'(?:https?://)?(?:' + '|'.join(re.escape(s) for s in SHORTENERS) + r')/[^\s<>"{}|\\^`\[\]]+'
        shortened_urls = re.findall(short_pattern, text, re.IGNORECASE)

        for url in shortened_urls:
            if not url.startswith('http'):
                url = 'https://' + url
            urls.append(url)

        # Clean URLs
        cleaned_urls = []
        seen = set()

        for url in urls:
            url = re.sub(r'[.,;:!?\)\]]+$', '', url)
            if url and url not in seen and len(url) > 10 and '.' in url:
                cleaned_urls.append(url)
                seen.add(url)

        logger.info(f"Extracted {len(cleaned_urls)} unique URLs")
        return cleaned_urls

    @staticmethod
    def is_shortened_url(url: str) -> bool:
        """Check if URL is shortened"""
        try:
            domain = urlparse(url).netloc.lower()
            return any(shortener in domain for shortener in SHORTENERS)
        except:
            return False

    @staticmethod
    async def unshorten_url_aggressive(url: str, session: aiohttp.ClientSession) -> str:
        """Unshorten URL aggressively with robust error handling"""
        max_attempts = 3
        current_url = url

        for attempt in range(max_attempts):
            try:
                logger.info(f"Unshortening attempt {attempt + 1}: {current_url}")

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive'
                }

                # Try HEAD request first
                try:
                    async with session.head(
                        current_url, 
                        allow_redirects=True, 
                        timeout=aiohttp.ClientTimeout(total=10),
                        headers=headers
                    ) as response:
                        final_url = str(response.url)
                        if final_url != current_url and len(final_url) > len(current_url):
                            logger.info(f"HEAD unshorten successful: {final_url}")
                            current_url = final_url
                            break
                except Exception as e:
                    logger.warning(f"HEAD request failed: {e}")

                # Fallback to GET
                try:
                    async with session.get(
                        current_url,
                        allow_redirects=True,
                        timeout=aiohttp.ClientTimeout(total=15),
                        headers=headers
                    ) as response:
                        final_url = str(response.url)
                        if final_url != current_url and len(final_url) > len(current_url):
                            logger.info(f"GET unshorten successful: {final_url}")
                            current_url = final_url
                            break
                except Exception as e:
                    logger.warning(f"GET request failed: {e}")

                await asyncio.sleep(1)

            except Exception as e:
                logger.warning(f"Unshorten attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(0.5)

        return current_url

    @staticmethod
    def clean_affiliate_url_aggressive(url: str) -> str:
        """Clean affiliate parameters from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            # Amazon cleaning
            if 'amazon' in domain:
                asin_patterns = [
                    r'/dp/([A-Z0-9]{10})(?:/|$|\?)',
                    r'/product/([A-Z0-9]{10})(?:/|$|\?)',
                    r'/([A-Z0-9]{10})(?:/|$|\?)',
                    r'asin=([A-Z0-9]{10})',
                    r'/gp/product/([A-Z0-9]{10})'
                ]

                full_path = parsed.path + '?' + parsed.query
                for pattern in asin_patterns:
                    match = re.search(pattern, full_path)
                    if match:
                        asin = match.group(1)
                        return f"https://www.amazon.in/dp/{asin}"

                # Fallback clean
                query_params = parse_qs(parsed.query)
                essential_params = {}
                for key, value in query_params.items():
                    if key.lower() in ['keywords', 'field-keywords'] and value:
                        essential_params[key] = value[0]

                clean_query = urlencode(essential_params)
                return urlunparse(parsed._replace(query=clean_query))

            # Flipkart cleaning
            elif 'flipkart' in domain:
                pid_patterns = [
                    r'/p/[^/]+/([^/?]+)',
                    r'pid=([A-Z0-9]+)',
                    r'/([A-Z0-9]{16})(?:/|\?|$)'
                ]

                full_path = parsed.path + '?' + parsed.query
                for pattern in pid_patterns:
                    match = re.search(pattern, full_path)
                    if match:
                        pid = match.group(1)
                        return f"https://www.flipkart.com/p/{pid}"

                query_params = parse_qs(parsed.query)
                essential_params = {}
                for key, value in query_params.items():
                    if key.lower() in ['pid', 'lid'] and value:
                        essential_params[key] = value[0]

                clean_query = urlencode(essential_params)
                return urlunparse(parsed._replace(query=clean_query))

            # Meesho cleaning
            elif 'meesho' in domain:
                return urlunparse(parsed._replace(query=''))

            # Myntra cleaning
            elif 'myntra' in domain:
                product_match = re.search(r'/(\d+)', parsed.path)
                if product_match:
                    product_id = product_match.group(1)
                    return f"https://www.myntra.com/{product_id}"
                return urlunparse(parsed._replace(query=''))

            # Ajio cleaning
            elif 'ajio' in domain:
                return urlunparse(parsed._replace(query=''))

            # Generic cleaning
            else:
                query_params = parse_qs(parsed.query)
                affiliate_keywords = [
                    'utm_', 'ref', 'tag', 'affiliate', 'aff', 'partner', 
                    'source', 'medium', 'campaign', 'tracking', 'fbclid',
                    'gclid', 'mc_', 'zanpid', 'ranMID', 'ranEAID'
                ]

                clean_params = {}
                for key, value in query_params.items():
                    if not any(keyword in key.lower() for keyword in affiliate_keywords) and value:
                        clean_params[key] = value[0]

                clean_query = urlencode(clean_params)
                return urlunparse(parsed._replace(query=clean_query))

        except Exception as e:
            logger.warning(f"Error cleaning URL {url}: {e}")
            return url

        return url

class MessageParser:
    """Parse manual product info from messages"""

    @staticmethod
    def extract_manual_info(message: str) -> Dict[str, Any]:
        """Extract product info from message text"""
        info = {
            'title': '',
            'price': '',
            'brand': '',
            'gender': '',
            'quantity': '',
            'pin': ''
        }

        # Extract price
        price_patterns = [
            r'@\s*(\d+)\s*rs',
            r'₹\s*(\d+(?:,\d+)*)',
            r'Rs\.?\s*(\d+(?:,\d+)*)',
            r'price[:\s]+(\d+(?:,\d+)*)',
            r'(\d+)\s*rs\b'
        ]

        for pattern in price_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                price_str = match.group(1).replace(',', '')
                try:
                    price_num = int(price_str)
                    if 10 <= price_num <= 1000000:
                        info['price'] = str(price_num)
                        break
                except:
                    continue

        # Extract PIN
        pin_pattern = r'\b([1-9]\d{5})\b'
        pin_matches = re.findall(pin_pattern, message)
        for pin in pin_matches:
            if pin[0] in '123456789':
                info['pin'] = pin
                break

        # Extract brand
        message_lower = message.lower()
        for brand in KNOWN_BRANDS:
            if brand.lower() in message_lower:
                info['brand'] = brand
                break

        # Extract gender
        for gender, patterns in GENDER_KEYWORDS.items():
            for pattern in patterns:
                if re.search(pattern, message, re.IGNORECASE):
                    info['gender'] = gender
                    break
            if info['gender']:
                break

        # Extract quantity
        for pattern in QUANTITY_PATTERNS:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                if len(match.groups()) > 0:
                    quantity = match.group(1)
                else:
                    quantity = match.group(0)
                info['quantity'] = quantity.strip()
                break

        # Extract title
        title = message
        title = re.sub(r'https?://[^\s]+', '', title)
        for pattern in price_patterns:
            title = re.sub(pattern, '', title, flags=re.IGNORECASE)
        title = re.sub(r'\b\d{6}\b', '', title)
        title = ' '.join(title.split())

        if title and len(title) > 3:
            info['title'] = title[:60].strip()

        return info

class ProductScraper:
    """Product information scraper"""

    @staticmethod
    def detect_platform(url: str) -> str:
        """Detect platform from URL"""
        domain = urlparse(url).netloc.lower()

        if 'amazon' in domain:
            return 'amazon'
        elif 'flipkart' in domain:
            return 'flipkart'
        elif 'meesho' in domain:
            return 'meesho'
        elif 'myntra' in domain:
            return 'myntra'
        elif 'ajio' in domain:
            return 'ajio'
        elif 'snapdeal' in domain:
            return 'snapdeal'
        else:
            return 'generic'

    @staticmethod
    async def scrape_with_fallback(url: str, session: aiohttp.ClientSession, manual_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Scrape product info with fallbacks"""
        platform = ProductScraper.detect_platform(url)

        result = {
            'title': '',
            'price': '',
            'original_price': '',
            'discount_percent': '',
            'rating': '',
            'review_count': '',
            'delivery_info': '',
            'stock_status': '',
            'image_url': '',
            'sizes': [],
            'colors': [],
            'brand': '',
            'gender': '',
            'quantity': '',
            'pin': '',
            'platform': platform,
            'error': None
        }

        # Apply manual info first
        if manual_info:
            for key, value in manual_info.items():
                if value:
                    result[key] = value

        # Try scraping
        scraped_info = await ProductScraper._try_scraping_methods(url, session, platform)

        # Merge scraped info
        for key, value in scraped_info.items():
            if value and not result.get(key):
                result[key] = value

        # Validate result
        if not result.get('title') and not result.get('price'):
            result['error'] = 'Could not extract product information'
            if 'amazon' in url:
                result['title'] = 'Amazon Product'
            elif 'flipkart' in url:
                result['title'] = 'Flipkart Product'
            elif 'meesho' in url:
                result['title'] = 'Meesho Product'
            else:
                result['title'] = 'Product Deal'

        return result

    @staticmethod
    async def _try_scraping_methods(url: str, session: aiohttp.ClientSession, platform: str) -> Dict[str, Any]:
        """Try multiple scraping methods"""
        info = {
            'title': '',
            'price': '',
            'original_price': '',
            'discount_percent': '',
            'rating': '',
            'review_count': '',
            'delivery_info': '',
            'stock_status': '',
            'image_url': '',
            'sizes': [],
            'colors': [],
            'brand': '',
            'gender': '',
            'quantity': '',
            'pin': ''
        }

        headers_list = [
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
                'Connection': 'keep-alive'
            },
            {
                'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
        ]

        for i, headers in enumerate(headers_list):
            try:
                logger.info(f"Scraping attempt {i+1} for {platform}")

                async with session.get(
                    url, 
                    headers=headers, 
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:

                    if response.status == 200:
                        html = await response.text()

                        if len(html) > 1000:
                            extracted_info = ProductScraper._extract_from_html(html, platform, url)

                            for key, value in extracted_info.items():
                                if value and not info.get(key):
                                    info[key] = value

                            if info.get('title') or info.get('price'):
                                logger.info(f"Successfully extracted data on attempt {i+1}")
                                break
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")

            except Exception as e:
                logger.warning(f"Scraping attempt {i+1} failed: {e}")
                continue

            await asyncio.sleep(1)

        return info

    @staticmethod
    def _extract_from_html(html: str, platform: str, url: str = '') -> Dict[str, Any]:
        """Extract product info from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        info = {}

        # Enhanced title selectors for better product details
        title_selectors = {
            'amazon': [
                '#productTitle',
                'h1.a-size-large.a-spacing-none.a-color-base',
                'span#productTitle',
                '.product-title',
                'h1[data-automation-id="product-title"]',
                '.a-spacing-none.a-color-base',
                'meta[property="og:title"]'
            ],
            'flipkart': [
                '.B_NuCI',
                '._35KyD6',
                'h1.yhB1nd',
                '.fsXA5P',
                'h1._6EBuvT',
                '.x2dL8s.x2BGSF',
                'h1',
                'meta[property="og:title"]'
            ],
            'meesho': [
                '[data-testid="product-title"]',
                '.sc-bcXHqe.sc-gueYoa',
                '.product-title',
                'h1',
                '.sc-bcXHqe',
                '.product-name',
                'meta[property="og:title"]'
            ],
            'myntra': [
                '.pdp-name',
                '.pdp-title',
                'h1.pdp-name',
                '.product-brand-name',
                '.pdp-product-name-price h1',
                '.product-title',
                'meta[property="og:title"]'
            ],
            'ajio': [
                '.prod-name',
                '.product-name',
                'h1.prod-title',
                '.prod-title-section h1',
                '.product-brand-name',
                'meta[property="og:title"]'
            ],
            'snapdeal': [
                '.pdp-product-name',
                'h1.pdp-e-i-head',
                '.product-title',
                'meta[property="og:title"]'
            ],
            'generic': [
                'h1',
                '.product-name',
                '.product-title',
                '.title',
                'meta[property="og:title"]',
                'title'
            ]
        }

        selectors = title_selectors.get(platform, title_selectors['generic'])
        for selector in selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    if selector.startswith('meta'):
                        content = element.get('content')
                        if isinstance(content, str):
                            text = content.strip()
                        elif content:
                            text = str(content).strip()
                        else:
                            text = ''
                    else:
                        text = element.get_text(strip=True)

                    if text and len(text) > 5 and len(text) < 300:
                        cleaned_title = ProductScraper._clean_title(text)
                        if cleaned_title and len(cleaned_title) > 10:
                            info['title'] = cleaned_title
                            break

                if info.get('title'):
                    break
            except:
                continue

        # Enhanced price extraction with JSON-LD support
        price_found = False
        
        # First try JSON-LD structured data for accurate pricing
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                import json
                script_content = script.get_text() if script else ''
                if script_content:
                    data = json.loads(script_content)
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    
                    # Extract price from structured data
                    price = None
                    if 'offers' in data and data['offers']:
                        offer = data['offers'][0] if isinstance(data['offers'], list) else data['offers']
                        price = offer.get('price') or offer.get('lowPrice')
                    elif 'price' in data:
                        price = data['price']
                    
                    if price:
                        try:
                            price_num = float(str(price).replace(',', '').replace('₹', '').replace('Rs', '').strip())
                            if 10 <= price_num <= 1000000:
                                info['price'] = str(int(price_num))
                                price_found = True
                                break
                        except:
                            pass
            except:
                continue
        
        # Enhanced regex patterns for price extraction
        price_patterns = [
            r'[₹]\s*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'"price"[:\s]*"?(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'₹(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'Rs\.?\s*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'\bprice["\s]*[:=]\s*["\s]*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'MRP[:\s]*[₹Rs\.]*\s*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'current[_\s]*price["\s]*[:=]\s*["\s]*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'"priceAmount"[:\s]*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'"amount"[:\s]*(\d+(?:,\d+)*(?:\.\d{2})?)',
            r'selling[_\s]*price["\s]*[:=]\s*["\s]*(\d+(?:,\d+)*(?:\.\d{2})?)'
        ]

        # Enhanced price selectors with more comprehensive patterns
        price_selectors = {
            'amazon': [
                '.a-price-whole', 
                '.a-price .a-offscreen', 
                '.a-price-range',
                '#priceblock_ourprice',
                '#priceblock_dealprice',
                '.a-price.a-text-price',
                '.a-price-symbol + .a-price-whole',
                '[data-asin-price]',
                '.a-offscreen',
                '.a-price .a-price-whole',
                '#apex_desktop .a-price-whole',
                '.a-price-current .a-price-whole'
            ],
            'flipkart': [
                '._30jeq3', 
                '._1_WHN1', 
                '.CEmiEU',
                '._25b18c',
                '._16Jk6d',
                '.Nx9bqj',
                '._1vC4OE',
                '.CEmiEU ._16Jk6d',
                '._25b18c ._16Jk6d',
                '._1vC4OE ._25b18c',
                '.Nx9bqj._16Jk6d'
            ],
            'meesho': [
                '[data-testid="product-price"]',
                '.sc-bcXHqe.sc-gueYoa',
                '.price',
                '.current-price',
                '.product-price',
                '.sc-gueYoa',
                '[data-testid="current-price"]',
                '.product-price-section .price'
            ],
            'myntra': [
                '.pdp-price', 
                '.price-current',
                '.pdp-price strong',
                '.discount-price',
                '.price .rs',
                '.pdp-price .pdp-price',
                '.price-current strong'
            ],
            'ajio': [
                '.prod-price', 
                '.price-current',
                '.price .price-number',
                '.price-range .price-number',
                '.prod-price .price-number'
            ]
        }

        # Try CSS selector-based extraction if JSON-LD didn't work
        if not price_found and platform in price_selectors:
            for selector in price_selectors[platform]:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        text = element.get_text(strip=True)
                        # Enhanced price matching with decimal support
                        price_match = re.search(r'(\d+(?:,\d+)*(?:\.\d{2})?)', text)
                        if price_match:
                            price_str = price_match.group(1).replace(',', '')
                            price_num = float(price_str)
                            if 10 <= price_num <= 1000000:
                                info['price'] = str(int(price_num))
                                price_found = True
                                break
                    if price_found:
                        break
                except:
                    continue

        # Fallback to regex patterns if other methods failed
        if not price_found:
            for pattern in price_patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    try:
                        price_str = match.replace(',', '').replace('₹', '').replace('Rs', '').strip()
                        price_num = float(price_str)
                        if 10 <= price_num <= 1000000:
                            info['price'] = str(int(price_num))
                            price_found = True
                            break
                    except:
                        continue
                if price_found:
                    break

        # Extract additional product details
        ProductScraper._extract_enhanced_details(soup, info, platform, html)

        # Platform-specific extractions
        if platform == 'meesho':
            # Extract sizes
            size_patterns = [
                r'\b(XS|S|M|L|XL|XXL|XXXL|2XL|3XL)\b',
                r'\bSize[:\s]+(XS|S|M|L|XL|XXL|XXXL|2XL|3XL)\b'
            ]
            sizes = set()
            for pattern in size_patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    sizes.add(match.upper())
                    if len(sizes) >= 5:
                        break

            if sizes:
                info['sizes'] = sorted(list(sizes))

            # Extract PIN
            pin_matches = re.findall(r'\b([1-9]\d{5})\b', html)
            for pin in pin_matches[:3]:
                if pin.startswith(tuple('123456789')):
                    info['pin'] = pin
                    break

        # Extract brand from title
        if info.get('title'):
            title_lower = info['title'].lower()
            for brand in KNOWN_BRANDS:
                if brand.lower() in title_lower:
                    info['brand'] = brand
                    break

        # Extract gender from title
        if info.get('title'):
            title_lower = info['title'].lower()
            for gender, patterns in GENDER_KEYWORDS.items():
                for pattern in patterns:
                    if re.search(pattern, title_lower):
                        info['gender'] = gender
                        break
                if info.get('gender'):
                    break

        # Extract quantity from title
        if info.get('title'):
            for pattern in QUANTITY_PATTERNS:
                match = re.search(pattern, info['title'], re.IGNORECASE)
                if match:
                    if len(match.groups()) > 0:
                        info['quantity'] = match.group(1)
                    else:
                        info['quantity'] = match.group(0).strip()
                    break

        return info

    @staticmethod
    def _extract_enhanced_details(soup: BeautifulSoup, info: Dict, platform: str, html: str) -> None:
        """Extract enhanced product details like ratings, reviews, images, etc."""
        
        # Extract original price and discount
        original_price_selectors = {
            'amazon': ['.a-price.a-text-price .a-offscreen', '.a-price-base .a-offscreen'],
            'flipkart': ['._3I9_wc', '._1_WHN1', '._3auQ3N'],
            'meesho': ['.original-price', '.mrp-price'],
            'myntra': ['.pdp-mrp', '.price-strike'],
            'ajio': ['.price-mrp', '.strike-through']
        }
        
        if platform in original_price_selectors:
            for selector in original_price_selectors[platform]:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        text = element.get_text(strip=True)
                        price_match = re.search(r'(\d+(?:,\d+)*)', text)
                        if price_match:
                            original_num = int(price_match.group(1).replace(',', ''))
                            if 10 <= original_num <= 1000000:
                                info['original_price'] = str(original_num)
                                # Calculate discount if we have both prices
                                if info.get('price'):
                                    try:
                                        current = int(info['price'])
                                        discount = round(((original_num - current) / original_num) * 100)
                                        if discount > 0:
                                            info['discount_percent'] = str(discount)
                                    except:
                                        pass
                                break
                    if info.get('original_price'):
                        break
                except:
                    continue

        # Extract ratings
        rating_selectors = {
            'amazon': ['.a-icon-alt', '[data-hook="average-star-rating"]', '.a-star-medium .a-icon-alt'],
            'flipkart': ['._3LWZlK', '._1i0vuL', '.XQDdHH'],
            'meesho': ['.rating', '.star-rating'],
            'myntra': ['.ratings-container .ratings', '.product-ratingsContainer'],
            'ajio': ['.prod-rating', '.rating-count']
        }
        
        if platform in rating_selectors:
            for selector in rating_selectors[platform]:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        text = element.get_text(strip=True) if element else ''
                        alt_text = element.get('alt', '') if element else ''
                        combined_text = f"{text} {alt_text}"
                        
                        rating_match = re.search(r'(\d+\.?\d*)\s*(?:out of|stars?|★)', combined_text, re.IGNORECASE)
                        if not rating_match:
                            rating_match = re.search(r'(\d+\.?\d*)', combined_text)
                        
                        if rating_match:
                            rating = float(rating_match.group(1))
                            if 0 <= rating <= 5:
                                info['rating'] = f"{rating:.1f}"
                                break
                    if info.get('rating'):
                        break
                except:
                    continue

        # Extract review count
        review_selectors = {
            'amazon': ['[data-hook="total-review-count"]', '.a-link-normal .a-size-base'],
            'flipkart': ['._2_R_DZ', '._38sUEc', '.row .col .row span'],
            'meesho': ['.review-count', '.reviews-count'],
            'myntra': ['.ratings-count', '.product-ratingsContainer span'],
            'ajio': ['.prod-reviews', '.reviews-rating']
        }
        
        if platform in review_selectors:
            for selector in review_selectors[platform]:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        text = element.get_text(strip=True)
                        review_match = re.search(r'(\d+(?:,\d+)*)', text)
                        if review_match:
                            count = review_match.group(1).replace(',', '')
                            if int(count) > 0:
                                info['review_count'] = count
                                break
                    if info.get('review_count'):
                        break
                except:
                    continue

        # Extract main product image
        image_selectors = {
            'amazon': ['#landingImage', '.a-dynamic-image', '#imgTagWrapperId img'],
            'flipkart': ['._396cs4', '._2r_T1I', '.CXW8mj img'],
            'meesho': ['.product-image img', '.main-image img'],
            'myntra': ['.image-grid-image', '.product-sliderContainer img'],
            'ajio': ['.rilrtl-lazy-img', '.prod-image img']
        }
        
        if platform in image_selectors:
            for selector in image_selectors[platform]:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        src = element.get('src') or element.get('data-src') or element.get('data-lazy-src')
                        if src and 'http' in src and not any(skip in src for skip in ['logo', 'icon', 'sprite']):
                            info['image_url'] = src
                            break
                    if info.get('image_url'):
                        break
                except:
                    continue

        # Extract delivery information
        delivery_patterns = [
            r'delivery by (\w+ \d+)',
            r'get it by (\w+ \d+)',
            r'delivered in (\d+-?\d* days?)',
            r'free delivery',
            r'same day delivery',
            r'next day delivery'
        ]
        
        for pattern in delivery_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                info['delivery_info'] = matches[0] if isinstance(matches[0], str) else ' '.join(matches[0])
                break

        # Extract stock status
        stock_patterns = [
            r'in stock',
            r'out of stock',
            r'only \d+ left',
            r'limited stock',
            r'currently unavailable'
        ]
        
        for pattern in stock_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                info['stock_status'] = match.group(0).title()
                break

        # Extract colors
        color_patterns = [
            r'\b(red|blue|green|yellow|black|white|pink|purple|orange|brown|grey|gray|navy|maroon|olive|lime|aqua|silver|gold)\b'
        ]
        
        colors = set()
        for pattern in color_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches[:5]:  # Limit to 5 colors
                colors.add(match.title())
        
        if colors:
            info['colors'] = sorted(list(colors))

    @staticmethod
    def _clean_title(title: str) -> str:
        """Clean title text"""
        if not title:
            return ''

        noise_patterns = [
            r'\s*-\s*Amazon\.in.*$',
            r'\s*:\s*Amazon\.in.*$',
            r'\s*\|\s*Flipkart\.com.*$',
            r'\s*-\s*Buy.*$',
            r'\s*\|\s*Buy.*$',
            r'Buy\s+.*?online.*?at.*?price.*?$',
            r'Shop\s+.*?online.*?$',
            r'\s*\|\s*Myntra.*$',
            r'\s*-\s*Meesho.*$',
            r'\s*\|\s*.*\.com.*$',
            r'\s*-\s*.*\.in.*$',
            r'MRP.*?₹.*?\d+',
            r'Price.*?₹.*?\d+',
            r'₹\d+.*?off',
            r'\d+%.*?off',
            r'discount.*?\d+',
            r'save.*?₹.*?\d+',
            r'Free\s+shipping',
            r'Cash\s+on\s+delivery',
            r'EMI\s+available',
            r'Exchange\s+offer'
        ]

        clean = title
        for pattern in noise_patterns:
            clean = re.sub(pattern, '', clean, flags=re.IGNORECASE)

        # Remove extra whitespace
        clean = ' '.join(clean.split())

        # Reduce aggressive filtering to preserve more product details
        promo_words = {
            "deal", "offer", "sale", "special", "discount", "free",
            "limited", "buy", "shop", "trending", "exclusive", "hot"
        }

        words = clean.split()
        filtered_words = [w for w in words if w.lower() not in promo_words and not re.match(r'^\W+$', w)]

        # Remove duplicates while preserving order
        seen = set()
        final_words = [w for w in filtered_words if not (w.lower() in seen or seen.add(w.lower()))]

        clean_title_str = " ".join(final_words).strip()

        # Increased length limit for more detailed descriptions
        if len(clean_title_str) > 100:
            clean_title_str = clean_title_str[:100]
            if ' ' in clean_title_str:
                clean_title_str = clean_title_str.rsplit(' ', 1)[0] + '...'

        return clean_title_str

class DealFormatter:
    """Formats product information into a deal structure."""

    @staticmethod
    def format_deal(product_info: Dict[str, Any], clean_url: str, platform: str = '') -> str:
        """Format product info into enhanced deal structure with all features"""

        if not platform:
            platform = ProductScraper.detect_platform(clean_url)

        # Build comprehensive enhanced deal format
        lines = []
        
        # === FIRST LINE: Brand + Gender + Title + Price ===
        line_components = []

        # Brand (if available and not in title)
        brand = product_info.get('brand', '').strip()
        title = product_info.get('title', '').strip()

        if brand and brand.lower() not in title.lower():
            line_components.append(brand)

        # Gender
        gender = product_info.get('gender', '').strip()
        if gender:
            line_components.append(gender)

        # Title (cleaned, remove brand if already added)
        if title:
            if brand and brand.lower() in title.lower():
                title_words = title.split()
                filtered_words = []
                brand_words = brand.lower().split()

                i = 0
                while i < len(title_words):
                    brand_match = True
                    for j, brand_word in enumerate(brand_words):
                        if i + j >= len(title_words) or title_words[i + j].lower() != brand_word.lower():
                            brand_match = False
                            break
                    if brand_match:
                        i += len(brand_words)
                        continue
                    filtered_words.append(title_words[i])
                    i += 1
                title = ' '.join(filtered_words).strip()

            line_components.append(title)
        else:
            line_components.append('Product Deal')

        # Enhanced Price Display with Original Price & Discount
        price = product_info.get('price', '').strip()
        original_price = product_info.get('original_price', '').strip()
        discount_percent = product_info.get('discount_percent', '').strip()
        
        if price:
            if original_price and original_price != price and discount_percent:
                # Show: ₹2999 ₹1999 (33% off) @1999 rs
                price_display = f"₹{original_price} ₹{price} ({discount_percent}% off) @{price} rs"
            else:
                price_display = f"@{price} rs"
            line_components.append(price_display)

        # First line: Brand + Gender + Title + Price
        lines.append(' '.join(line_components))

        # === SECOND LINE: Rating & Reviews ===
        rating = product_info.get('rating', '').strip()
        review_count = product_info.get('review_count', '').strip()
        
        if rating or review_count:
            rating_line = []
            if rating:
                rating_line.append(f"⭐ {rating}")
            if review_count:
                rating_line.append(f"({review_count} reviews)")
            lines.append(' '.join(rating_line))

        # === THIRD LINE: Stock & Delivery Info ===
        stock_status = product_info.get('stock_status', '').strip()
        delivery_info = product_info.get('delivery_info', '').strip()
        
        info_line = []
        if stock_status:
            if 'out of stock' in stock_status.lower():
                info_line.append(f"❌ {stock_status}")
            elif 'only' in stock_status.lower():
                info_line.append(f"⚡ {stock_status}")
            else:
                info_line.append(f"✅ {stock_status}")
                
        if delivery_info:
            info_line.append(f"🚚 {delivery_info}")
            
        if info_line:
            lines.append(' '.join(info_line))

        # === FOURTH LINE: Colors & Variants ===
        colors = product_info.get('colors', [])
        if colors and len(colors) <= 3:
            lines.append(f"🎨 Colors: {', '.join(colors)}")
        elif colors and len(colors) > 3:
            lines.append(f"🎨 Colors: {', '.join(colors[:3])} +{len(colors)-3} more")

        # === CLEAN URL ===
        lines.append('')
        lines.append(clean_url)
        lines.append('')

        # === MEESHO-SPECIFIC INFO ===
        if platform == 'meesho' or 'meesho' in clean_url.lower():
            sizes = product_info.get('sizes', [])
            if sizes:
                if len(sizes) >= 5:
                    lines.append('Size - All')
                else:
                    lines.append(f"Size - {', '.join(sizes)}")
            else:
                lines.append('Size - All')

            pin = product_info.get('pin', '110001')
            lines.append(f"Pin - {pin}")
            lines.append('')

        # === PLATFORM BADGE ===
        platform_emojis = {
            'amazon': '📦 Amazon',
            'flipkart': '🛒 Flipkart', 
            'meesho': '🏪 Meesho',
            'myntra': '👗 Myntra',
            'ajio': '👔 Ajio',
            'snapdeal': '🏬 Snapdeal'
        }
        
        if platform in platform_emojis:
            lines.append(platform_emojis[platform])

        # === DEAL QUALITY INDICATOR ===
        if discount_percent:
            try:
                discount_num = int(discount_percent)
                if discount_num >= 50:
                    lines.append('🔥 HOT DEAL!')
                elif discount_num >= 30:
                    lines.append('💰 GOOD PRICE')
                elif discount_num >= 15:
                    lines.append('💸 FAIR DEAL')
            except:
                pass

        # === CHANNEL TAG ===
        lines.append('')
        lines.append('@reviewcheckk')

        return '\n'.join(lines)

class DealBot:
    """Main bot class"""

    def __init__(self):
        self.session = None
        self.processed_messages = set()
        self.processing_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize session"""
        if not self.session:
            connector = aiohttp.TCPConnector(
                limit=30,
                limit_per_host=10,
                ttl_dns_cache=300,
                use_dns_cache=True
            )
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
            )

    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
            self.session = None

    async def process_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process incoming messages"""
        async with self.processing_lock:
            try:
                message = update.message or update.channel_post
                if not message:
                    return

                # Prevent duplicate processing
                message_id = f"{message.chat_id}_{message.message_id}"
                if message_id in self.processed_messages:
                    return

                self.processed_messages.add(message_id)

                # Memory management
                if len(self.processed_messages) > 200:
                    old_messages = list(self.processed_messages)[:100]
                    for old_msg in old_messages:
                        self.processed_messages.discard(old_msg)

                await self.initialize()

                # Extract text
                text = message.text or message.caption or ''
                if not text or len(text.strip()) < 5:
                    return

                logger.info(f"Processing message: {text[:100]}...")

                # Extract links
                links = SmartLinkProcessor.extract_all_links(text)

                if not links:
                    logger.info("No links found")
                    return

                logger.info(f"Found {len(links)} links")

                # Process each link
                results = []
                for i, url in enumerate(links):
                    try:
                        logger.info(f"Processing link {i+1}/{len(links)}: {url}")

                        # Ensure session is available
                        if not self.session:
                            await self.initialize()
                        
                        # Unshorten if needed
                        if SmartLinkProcessor.is_shortened_url(url) and self.session:
                            logger.info(f"Unshortening URL: {url}")
                            url = await SmartLinkProcessor.unshorten_url_aggressive(url, self.session)
                            logger.info(f"Unshortened to: {url}")

                        # Clean URL
                        clean_url = SmartLinkProcessor.clean_affiliate_url_aggressive(url)
                        logger.info(f"Cleaned URL: {clean_url}")

                        # Extract manual info
                        manual_info = MessageParser.extract_manual_info(text)
                        logger.info(f"Manual info: {manual_info}")

                        # Scrape product info
                        if self.session:
                            product_info = await ProductScraper.scrape_with_fallback(
                                clean_url, 
                                self.session, 
                                manual_info
                            )
                        else:
                            product_info = {'title': 'Product Deal', 'price': '', 'platform': 'unknown'}
                        logger.info(f"Product info: {product_info}")

                        # Detect platform
                        platform = ProductScraper.detect_platform(clean_url)

                        # Format message
                        formatted_message = DealFormatter.format_deal(product_info, clean_url, platform)

                        results.append(formatted_message)

                        # Brief delay
                        if len(links) > 1 and i < len(links) - 1:
                            await asyncio.sleep(1)

                    except Exception as e:
                        logger.error(f"Error processing link {url}: {str(e)}")
                        error_msg = f"Product Deal\n{url}\n\n@reviewcheckk"
                        results.append(error_msg)
                        continue

                # Send results
                for result in results:
                    try:
                        await safe_send_message(
                            update, 
                            context, 
                            result, 
                            disable_web_page_preview=True
                        )

                        if len(results) > 1:
                            await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Failed to send result: {e}")
                        continue

            except Exception as e:
                logger.error(f"Error in process_message: {str(e)}")
                try:
                    error_msg = "❌ Error processing message\n\n@reviewcheckk"
                    await safe_send_message(update, context, error_msg)
                except:
                    pass

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle start command"""
    msg = (
        "🤖 *Deal Bot v2.0 Active!*\n\n"
        "✅ Smart link detection & processing\n"
        "✅ Automatic URL unshortening\n"
        "✅ Clean affiliate link removal\n"
        "✅ Accurate price & title extraction\n"
        "✅ Brand, gender & quantity detection\n"
        "✅ Meesho size & PIN support\n"
        "✅ Strict deal format compliance\n\n"
        "📝 *Supported Platforms:*\n"
        "• Amazon • Flipkart • Meesho\n"
        "• Myntra • Ajio • Snapdeal\n\n"
        "🔗 Send any product link and get perfectly formatted deals!\n\n"
        "@reviewcheckk"
    )
    await safe_send_message(update, context, msg, parse_mode=ParseMode.MARKDOWN)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Update {update} caused error {context.error}")

    try:
        if update and hasattr(update, 'effective_chat'):
            update_obj = update  # type: ignore
            if update_obj.effective_chat:  # type: ignore
                await safe_send_message(
                    update_obj,  # type: ignore
                    context, 
                    "❌ Sorry, an error occurred. Please try again.\n\n@reviewcheckk"
                )
    except:
        pass

async def safe_send_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    """Safe message sending"""
    if not update or not update.effective_chat:
        logger.error("Invalid update or chat")
        return

    try:
        # Ensure text length limit
        if len(text) > 4096:
            text = text[:4090] + "..."

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            **kwargs
        )
        logger.info("Message sent successfully")

    except Exception as e:
        logger.error(f"Failed to send message: {e}")

        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Error processing request\n\n@reviewcheckk"
            )
        except Exception as e2:
            logger.error(f"Failed to send fallback message: {e2}")

async def cleanup_bot_conflicts():
    """Clean up potential bot conflicts"""
    try:
        import asyncio
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            # Delete webhook to clear any conflicts
            webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
            async with session.post(webhook_url) as response:
                if response.status == 200:
                    print("✅ Cleaned up webhook conflicts")
                    
            # Wait a moment for cleanup
            await asyncio.sleep(2)
    except Exception as e:
        print(f"⚠️  Webhook cleanup failed: {e}")

def main():
    """Main function"""
    print("🚀 Starting Deal Bot v2.0...")

    try:
        # Clean up any existing conflicts first
        try:
            import requests
            webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
            response = requests.post(webhook_url, timeout=10)
            if response.status_code == 200:
                print("✅ Cleaned up webhook conflicts")
        except:
            pass
            
        # Create application
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .connect_timeout(30)
            .read_timeout(30)
            .write_timeout(30)
            .pool_timeout(30)
            .build()
        )

        # Initialize bot
        bot = DealBot()

        # Add handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(MessageHandler(
            filters.TEXT | filters.CAPTION, 
            bot.process_message
        ))

        # Add error handler
        application.add_error_handler(error_handler)

        # Setup cleanup
        import signal
        import sys

        def signal_handler(sig, frame):
            print("\n🛑 Shutting down bot...")
            asyncio.create_task(bot.cleanup())
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start bot
        print(f"✅ Bot @{BOT_USERNAME} is running...")
        print("📡 Monitoring all channels, groups, and DMs")
        print("🔗 Processing product links with enhanced accuracy")
        print("📝 Strict deal format compliance enabled")

        # Handle bot conflicts more aggressively
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"🔄 Starting bot (attempt {retry_count + 1}/{max_retries})...")
                application.run_polling(
                    allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True,
                    close_loop=False
                )
                break  # If successful, exit the retry loop
                
            except Exception as e:
                if "409" in str(e) or "Conflict" in str(e):
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"⚠️  Bot conflict detected (attempt {retry_count}). Retrying in 15 seconds...")
                        import time
                        time.sleep(15)
                        
                        # Try to force clear the webhook again
                        try:
                            import requests
                            webhook_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true"
                            requests.post(webhook_url, timeout=10)
                            time.sleep(5)
                        except:
                            pass
                    else:
                        print("❌ Unable to resolve bot conflict after multiple attempts.")
                        print("💡 Solutions:")
                        print("   1. There may be another instance running elsewhere")
                        print("   2. Check if the bot is running on another server/device")
                        print("   3. Wait 60 seconds and try again")
                        print("   4. Contact @BotFather to revoke and regenerate the token")
                        print("   5. Use a different bot token for testing")
                        return
                else:
                    print(f"❌ Bot error: {e}")
                    raise

    except Exception as e:
        print(f"❌ Failed to start bot: {e}")
        logger.error(f"Bot startup failed: {e}")

if __name__ == '__main__':
    main()