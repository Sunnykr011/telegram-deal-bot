import asyncio
import re
import logging
from urllib.parse import urlparse, parse_qs, urlunparse
from typing import Optional, List, Dict, Tuple
import requests
from bs4 import BeautifulSoup
from telegram import Update, Message
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class URLResolver:
    """Handle URL unshortening and cleaning"""

    SHORTENERS = [
        'amzn.to', 'fkrt.cc', 'spoo.me', 'wishlink.com', 'bitli.in', 
        'da.gd', 'cutt.ly', 'bit.ly', 'tinyurl.com', 'goo.gl', 't.co',
        'short.me', 'u.to', 'ow.ly', 'tiny.cc', 'is.gd'
    ]

    TRACKING_PARAMS = [
        'tag', 'ref', 'refRID', 'pf_rd_r', 'pf_rd_p', 'pf_rd_m', 
        'pf_rd_t', 'pf_rd_s', 'pf_rd_i', 'utm_source', 'utm_medium', 
        'utm_campaign', 'utm_term', 'utm_content', 'gclid', 'fbclid',
        'mc_cid', 'mc_eid', '_gl', 'igshid', 'si'
    ]

    @staticmethod
    def detect_links(text: str) -> List[str]:
        """Extract all URLs from text"""
        url_pattern = r'https?://(?:[-\w.])+(?::[0-9]+)?(?:/(?:[\w/_.])*)?(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?'
        return re.findall(url_pattern, text)

    @staticmethod
    def is_shortener(url: str) -> bool:
        """Check if URL is from a shortening service"""
        domain = urlparse(url).netloc.lower()
        return any(shortener in domain for shortener in URLResolver.SHORTENERS)

    @staticmethod
    async def unshorten_url(url: str) -> str:
        """Resolve shortened URL to final destination"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }

            def make_request():
                response = requests.get(url, headers=headers, allow_redirects=True, timeout=2.5)
                return response.url

            final_url = await asyncio.to_thread(make_request)
            return URLResolver.clean_url(final_url)

        except Exception as e:
            logger.warning(f"Failed to unshorten URL {url}: {e}")
            return URLResolver.clean_url(url)

    @staticmethod
    def clean_url(url: str) -> str:
        """Remove tracking parameters from URL"""
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)

            # Remove tracking parameters
            cleaned_params = {
                k: v for k, v in query_params.items() 
                if k not in URLResolver.TRACKING_PARAMS
            }

            # Rebuild query string
            if cleaned_params:
                query_string = '&'.join(f"{k}={v[0]}" for k, v in cleaned_params.items())
                cleaned = parsed._replace(query=query_string)
            else:
                cleaned = parsed._replace(query='')

            return urlunparse(cleaned)

        except Exception:
            return url

class TitleCleaner:
    """Extract and clean product titles"""

    FLUFF_WORDS = [
        'best offer', 'trending', 'stylish', 'buy online', 'india', 'amazon.in',
        'flipkart', 'official store', 'exclusive', 'limited time', 'deal',
        'sale', 'discount', 'offer', 'free shipping', 'cod available',
        'cash on delivery', 'lowest price', 'great indian', 'festival',
        'for parties', 'cool', 'attractive', 'beautiful', 'amazing',
        'super', 'premium', 'high quality', 'branded', 'original'
    ]

    CLOTHING_KEYWORDS = [
        'kurta', 'shirt', 'dress', 'top', 'bottom', 'jeans', 'trouser',
        'saree', 'lehenga', 'suit', 'kurti', 'palazzo', 'dupatta',
        'blouse', 'skirt', 'shorts', 'tshirt', 't-shirt', 'hoodie',
        'jacket', 'coat', 'sweater', 'cardigan', 'blazer'
    ]

    GENDER_KEYWORDS = {
        'women': ['women', 'womens', 'ladies', 'girls', 'female', 'girl'],
        'men': ['men', 'mens', 'boys', 'male', 'boy', 'gents'],
        'kids': ['kids', 'child', 'children', 'baby', 'infant'],
        'unisex': ['unisex', 'couple']
    }

    QUANTITY_PATTERNS = [
        r'pack of (\d+)', r'set of (\d+)', r'(\d+)\s*pcs?', r'(\d+)\s*pieces?',
        r'(\d+)\s*units?', r'(\d+)\s*kg', r'(\d+)\s*g\b', r'(\d+)\s*ml',
        r'(\d+)\s*l\b', r'combo of (\d+)', r'(\d+)\s*pairs?',
        r'multipack\s*(\d+)', r'(\d+)\s*in\s*1'
    ]

    NOISE_WORDS = {'http', 'https', 'www', 'com', 'in', 'co', 'share', 'the', 'and', 'or', 'at', 'to', 'for', 'of', 'extp', 'faym', 'wishlink'}

    @staticmethod
    async def extract_title_from_url(url: str) -> Optional[str]:
        """Extract title from product page with improved headers"""
        try:
            # Rotate user agents and add more headers to avoid blocking
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
            ]

            import random
            headers = {
                'User-Agent': random.choice(user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }

            def scrape_title():
                session = requests.Session()
                session.headers.update(headers)

                # Add delay to avoid rate limiting
                import time
                time.sleep(0.5)

                response = session.get(url, timeout=8, allow_redirects=True, verify=False)
                
                content = response.content
                soup = BeautifulSoup(content, 'html.parser')

                # Try multiple title extraction methods
                title_candidates = []

                # Method 1: og:title
                og_title = soup.find('meta', property='og:title')
                if og_title and og_title.get('content'):
                    title_candidates.append(og_title['content'].strip())

                # Method 2: twitter:title
                twitter_title = soup.find('meta', attrs={'name': 'twitter:title'})
                if twitter_title and twitter_title.get('content'):
                    title_candidates.append(twitter_title['content'].strip())

                # Method 3: page title
                title_tag = soup.find('title')
                if title_tag and title_tag.text:
                    title_candidates.append(title_tag.text.strip())

                # Method 4: h1 tag
                h1_tag = soup.find('h1')
                if h1_tag and h1_tag.text:
                    title_candidates.append(h1_tag.text.strip())

                # Method 5: Product-specific selectors
                domain = urlparse(url).netloc.lower()

                if 'meesho.com' in domain:
                    product_title = soup.find('span', class_='Text__StyledText-sc-oo0kvp-0')
                    if product_title and product_title.text:
                        title_candidates.append(product_title.text.strip())

                elif 'flipkart.com' in domain or 'fkrt' in domain:
                    product_title = soup.find('span', class_='B_NuCI')
                    if not product_title:
                        product_title = soup.find('h1', class_='x2cTzZ')
                    if not product_title:
                        product_title = soup.find('span', class_='VU-ZEz')
                    if product_title and product_title.text:
                        title_candidates.append(product_title.text.strip())

                elif 'amazon.in' in domain:
                    product_title = soup.find('span', id='productTitle')
                    if product_title and product_title.text:
                        title_candidates.append(product_title.text.strip())

                elif 'wishlink.com' in domain or 'extp.in' in domain or 'faym.co' in domain:
                    selectors = ['h1', '.product-title', '.title', '#title', '.product-name']
                    for sel in selectors:
                        elem = soup.select_one(sel)
                        if elem and elem.text:
                            title_candidates.append(elem.text.strip())

                # Check for error pages
                if any('denied' in t.lower() or 'error' in t.lower() or '403' in t.lower() for t in title_candidates):
                    return None

                # Return the best candidate (shortest non-empty title usually best)
                valid_titles = [t for t in title_candidates if t and len(t) > 5]
                if valid_titles:
                    return min(valid_titles, key=len)  # Return shortest valid title

                return title_candidates[0] if title_candidates else None

            return await asyncio.to_thread(scrape_title)

        except Exception as e:
            logger.warning(f"Failed to extract title from {url}: {e}")
            return None

    @staticmethod
    def extract_title_from_url_slug(url: str) -> Optional[str]:
        """Intelligently extracts product names from URL paths"""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        segments = [seg for seg in path.split('/') if seg]
        if not segments:
            return None
        slug = segments[-1]
        if len(slug) < 8 and re.match(r'[a-z0-9]+$', slug):
            return None  # Skip short codes like nm7xhr
        title = re.sub(r'[-_]', ' ', slug)
        title = re.sub(r'\d+', '', title)
        title = ' '.join(word.capitalize() for word in title.split() if len(word) > 1)
        if len(title) < 5:
            return None
        return title

    @staticmethod
    def clean_title(raw_title: str) -> str:
        """Clean and format product title according to new rules"""
        if not raw_title:
            return ""

        # Remove emojis and special characters except basic punctuation
        title = re.sub(r'[^\w\s\-&().]', ' ', raw_title)

        # Remove fluff words
        for fluff in TitleCleaner.FLUFF_WORDS:
            title = re.sub(re.escape(fluff), '', title, flags=re.IGNORECASE)

        # Normalize whitespace
        title = ' '.join(title.split())

        # Noise word filtering
        title = ' '.join(word for word in title.split() if word.lower() not in TitleCleaner.NOISE_WORDS)

        # Reject nonsense titles
        if TitleCleaner.is_nonsense_title(title):
            return ""

        # Extract components using new rules
        return TitleCleaner.format_with_new_rules(title)

    @staticmethod
    def format_with_new_rules(title: str) -> str:
        """Format title according to: [Brand] [Gender] [Quantity] [Product Name]"""
        words = title.lower().split()

        # Extract components
        brand = TitleCleaner.extract_brand(words)
        gender = TitleCleaner.extract_gender(words)
        quantity = TitleCleaner.extract_quantity(' '.join(words))
        product = TitleCleaner.extract_product(words)
        is_clothing = TitleCleaner.is_clothing_item(' '.join(words))

        # Build final title
        parts = []
        if brand:
            parts.append(brand)
        if is_clothing and gender:
            parts.append(gender)
        if quantity:
            parts.append(quantity)
        if product:
            parts.append(product)

        # Ensure max 5-8 words total
        final_parts = parts[:8]

        # Remove duplicates while preserving order
        seen = set()
        unique_parts = []
        for part in final_parts:
            part_lower = part.lower()
            if part_lower not in seen:
                seen.add(part_lower)
                unique_parts.append(part.title() if len(part) > 1 else part.upper())

        return ' '.join(unique_parts)

    @staticmethod
    def extract_gender(words: List[str]) -> Optional[str]:
        """Extract gender from words"""
        for gender, keywords in TitleCleaner.GENDER_KEYWORDS.items():
            if any(keyword in words for keyword in keywords):
                return gender.title()
        return None

    @staticmethod
    def extract_quantity(text: str) -> Optional[str]:
        """Extract quantity information"""
        for pattern in TitleCleaner.QUANTITY_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                quantity = match.group(1) if match.groups() else match.group(0)
                # Format based on pattern type
                if 'pack of' in pattern.lower():
                    return f"Pack of {quantity}"
                elif 'set of' in pattern.lower():
                    return f"Set of {quantity}"
                elif 'pcs' in pattern or 'pieces' in pattern:
                    return f"{quantity} Pcs"
                elif 'kg' in pattern:
                    return f"{quantity}kg"
                elif 'g ' in pattern:
                    return f"{quantity}g"
                elif 'ml' in pattern:
                    return f"{quantity}ml"
                elif 'l ' in pattern:
                    return f"{quantity}L"
                elif 'combo' in pattern:
                    return f"Combo of {quantity}"
                elif 'pairs' in pattern:
                    return f"{quantity} Pairs"
                elif 'multipack' in pattern:
                    return f"Multipack {quantity}"
                else:
                    return f"{quantity} Pcs"
        return None

    @staticmethod
    def extract_brand(words: List[str]) -> Optional[str]:
        """Extract brand name (usually first meaningful word)"""
        # Common brands to prioritize
        known_brands = [
            'nike', 'adidas', 'puma', 'reebok', 'boat', 'jbl', 'sony', 
            'samsung', 'apple', 'mi', 'realme', 'oneplus', 'vivo', 'oppo',
            'libas', 'aurelia', 'w', 'biba', 'global desi', 'chemistry',
            'aqualogica'
        ]

        # Look for known brands first
        for word in words:
            if word in known_brands:
                return word.title()

        # If no known brand, take first meaningful word (not gender/quantity)
        for word in words:
            if (word not in [kw for kw_list in TitleCleaner.GENDER_KEYWORDS.values() for kw in kw_list] 
                and not re.match(r'\d+', word) 
                and len(word) > 2):
                return word.title()

        return None

    @staticmethod
    def extract_product(words: List[str]) -> str:
        """Extract product name (clothing items or main product)"""
        # Find clothing keywords
        for word in words:
            if word in TitleCleaner.CLOTHING_KEYWORDS:
                return word.title()

        # If not clothing, extract meaningful product words
        product_words = []
        skip_words = ['for', 'with', 'and', 'or', 'the', 'a', 'an', 'in', 'on', 'at']

        for word in words:
            if (len(word) > 2 
                and word not in skip_words
                and word not in [kw for kw_list in TitleCleaner.GENDER_KEYWORDS.values() for kw in kw_list]
                and not re.match(r'\d+', word)):
                product_words.append(word)

        # Take last 2-3 meaningful words as product name
        return ' '.join(product_words[-3:]).title() if product_words else 'Product'

    @staticmethod
    def is_nonsense_title(title: str) -> bool:
        """Check if title is nonsense/invalid"""
        if len(title) < 3:
            return True

        # Check for lack of vowels
        vowel_count = len([c for c in title.lower() if c in 'aeiou'])
        if vowel_count < len(title) * 0.1:  # Less than 10% vowels
            return True

        # Check for repeated characters
        if re.search(r'(.)\1{4,}', title):  # Same char repeated 5+ times
            return True

        return False

    @staticmethod
    def is_clothing_item(title: str) -> bool:
        """Check if product is clothing item"""
        return any(keyword in title.lower() for keyword in TitleCleaner.CLOTHING_KEYWORDS)

class PriceExtractor:
    """Extract and format prices"""

    @staticmethod
    def extract_prices(text: str) -> List[int]:
        """Extract all prices from text"""
        price_patterns = [
            r'(?:₹|Rs?\.?\s*)(\d[\d,]*)',  # ₹1299 or Rs. 1299
            r'(\d[\d,]*)\s*(?:₹|Rs?\.?)',  # 1299₹ or 1299 Rs
            r'price\s*:?\s*(?:₹|Rs?\.?\s*)(\d[\d,]*)',  # price: ₹1299
            r'cost\s*:?\s*(?:₹|Rs?\.?\s*)(\d[\d,]*)',   # cost: ₹1299
            r'@\s*(\d[\d,]*)\s*rs',  # @1299 rs
        ]

        prices = []
        for pattern in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if match:
                    price = match.replace(',', '')
                    if price.isdigit() and int(price) > 0:
                        prices.append(int(price))
        return prices

    @staticmethod
    def format_price(price: str, is_from: bool = False) -> str:
        """Format price in ReviewCheckk style"""
        if not price:
            return "@ rs"
        if is_from:
            return f"from @{price} rs"
        return f"@{price} rs"

class PinDetector:
    """Detect PIN codes from messages"""

    @staticmethod
    def extract_pin(text: str) -> str:
        """Extract 6-digit PIN code from text"""
        pin_pattern = r'\b(\d{6})\b'
        matches = re.findall(pin_pattern, text)

        for pin in matches:
            # Validate PIN (should not be all same digits or sequential)
            if len(set(pin)) > 1 and not re.match(r'123456|654321', pin):
                return pin

        return "110001"  # Default PIN for Delhi

class ResponseBuilder:
    """Build formatted responses"""

    @staticmethod
    def build_response(title: str, url: str, price: str, is_meesho: bool = False, 
                      size: str = "All", pin: str = "110001") -> str:
        """Build final formatted response"""

        if not title:
            return "❌ Unable to extract product info"

        # Build base response
        response = f"{title} {price}\n{url}"

        # Add Meesho-specific info
        if is_meesho:
            response += f"\nSize - {size}\nPin - {pin}"

        return response

class ReviewCheckkBot:
    """Main bot class"""

    def __init__(self, token: str):
        self.application = Application.builder().token(token).build()
        self.setup_handlers()

    def setup_handlers(self):
        """Setup message handlers"""
        # Handle all messages with links or images
        self.application.add_handler(
            MessageHandler(
                filters.TEXT | filters.PHOTO | filters.FORWARDED,
                self.handle_message
            )
        )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Main message handler"""
        try:
            message = update.message

            # Get text from message or caption
            text = self.extract_text(message)

            if not text:
                if message.photo:
                    await message.reply_text("No title provided")
                return

            # Extract and process URLs
            urls = URLResolver.detect_links(text)

            if not urls:
                return  # No URLs to process

            # Process each URL
            for url in urls:
                response = await self.process_url(url, text)
                if response:
                    if message.photo:
                        await message.reply_photo(photo=message.photo[-1].file_id, caption=response)
                    else:
                        await message.reply_text(response)

        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await update.message.reply_text("❌ Unable to extract product info")

    def extract_text(self, message: Message) -> str:
        """Extract text from message or caption"""
        if message.text:
            return message.text
        elif message.caption:
            return message.caption
        elif message.forward_from and hasattr(message.forward_from, 'text'):
            return message.forward_from.text
        return ""

    async def process_url(self, url: str, message_text: str) -> Optional[str]:
        """Process a single URL and return formatted response"""
        try:
            # Unshorten URL if needed
            if URLResolver.is_shortener(url):
                final_url = await URLResolver.unshorten_url(url)
            else:
                final_url = URLResolver.clean_url(url)

            # Strategy 1: Forwarded message title patterns
            forwarded_title = self.extract_forwarded_title(message_text)
            clean_title = ""
            if forwarded_title:
                clean_title = TitleCleaner.clean_title(forwarded_title)

            # Strategy 2: Web scraping
            if not clean_title:
                scraped_title = await TitleCleaner.extract_title_from_url(final_url)
                if scraped_title:
                    clean_title = TitleCleaner.clean_title(scraped_title)

            # Strategy 3: URL slug extraction
            if not clean_title:
                slug_title = TitleCleaner.extract_title_from_url_slug(final_url)
                if slug_title:
                    clean_title = TitleCleaner.clean_title(slug_title)

            # Strategy 4: Message text cleaning as final fallback
            if not clean_title:
                message_title = self.clean_message_as_title(message_text)
                if message_title:
                    clean_title = TitleCleaner.clean_title(message_title)

            if not clean_title:
                return "❌ Unable to extract product info"

            # Extract price from message
            prices = PriceExtractor.extract_prices(message_text)
            is_from = 'from' in message_text.lower() or len(prices) > 1
            price_str = str(min(prices)) if prices else ''
            formatted_price = PriceExtractor.format_price(price_str, is_from)

            # Extract pin and size
            pin = PinDetector.extract_pin(message_text)
            domain = urlparse(final_url).netloc.lower()
            is_meesho = 'meesho.com' in domain
            size = self.extract_sizes(message_text) if is_meesho else "All"

            return ResponseBuilder.build_response(clean_title, final_url, formatted_price, is_meesho, size, pin)

        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            return None

    def extract_forwarded_title(self, text: str) -> Optional[str]:
        """Extract title from forwarded message patterns"""
        lines = text.split('\n')
        potential_titles = []
        for line in lines:
            line = line.strip()
            if line and not re.search(r'https?://', line) and len(line) > 5:
                potential_titles.append(line)
        if potential_titles:
            return ' '.join(potential_titles[:2])  # Take first 1-2 lines as title
        return None

    def clean_message_as_title(self, text: str) -> str:
        """Clean message text to use as fallback title"""
        # Remove URLs
        text = re.sub(r'https?://\S+', '', text)
        # Remove extra spaces
        text = ' '.join(text.split())
        return text

    def extract_sizes(self, text: str) -> str:
        """Extract sizes from message"""
        size_pattern = r'size\s*-\s*([\w,\s]+)'
        match = re.search(size_pattern, text, re.IGNORECASE)
        if match:
            sizes = match.group(1).strip().upper()
            if 'all' in sizes.lower():
                return 'All'
            return sizes.replace(' ', '')
        # Alternative pattern for available sizes
        sizes_found = re.findall(r'\b(x{0,3}s|x{1,3}l|m)\b', text.upper())
        if sizes_found:
            return ', '.join(sorted(set(sizes_found)))
        # If stock displayed, use that
        stock_pattern = r'stock\s*:\s*([\w,\s]+)'
        match = re.search(stock_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip().upper()
        return 'All'