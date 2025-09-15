import re
import json
import time
from urllib.parse import urlparse

def validate_keyword(keyword):
    """Validate search keyword input"""
    if not keyword or not isinstance(keyword, str):
        raise ValueError("Keyword must be a non-empty string")
    
    keyword = keyword.strip()
    if len(keyword) < 2:
        raise ValueError("Keyword must be at least 2 characters long")
    
    if len(keyword) > 100:
        raise ValueError("Keyword cannot exceed 100 characters")
    
    # Check for malicious patterns
    dangerous_patterns = ['<script', 'javascript:', 'data:', 'vbscript:', 'onload=']
    if any(pattern in keyword.lower() for pattern in dangerous_patterns):
        raise ValueError("Keyword contains invalid characters")
    
    return keyword

def validate_country(country):
    """Validate country name input"""
    if not country or not isinstance(country, str):
        raise ValueError("Country must be a non-empty string")
    
    country = country.strip()
    if len(country) < 2:
        raise ValueError("Country name must be at least 2 characters long")
    
    if len(country) > 100:
        raise ValueError("Country name cannot exceed 100 characters")
    
    # Allow only letters, spaces, hyphens, apostrophes, and parentheses
    if not re.match(r"^[a-zA-Z\s\-'()]+$", country):
        raise ValueError("Country name contains invalid characters")
    
    return country

def validate_days(days):
    """Validate trip duration input"""
    try:
        days = int(days)
    except (ValueError, TypeError):
        raise ValueError("Days must be a valid integer")
    
    if days < 1:
        raise ValueError("Trip duration must be at least 1 day")
    
    if days > 30:
        raise ValueError("Trip duration cannot exceed 30 days")
    
    return days

def validate_http_response(response):
    """Validate HTTP response from web scraping"""
    if not response:
        raise ValueError("Empty response received")
    
    # Check status code
    if response.status_code != 200:
        raise ValueError(f"HTTP request failed with status code: {response.status_code}")
    
    # Check content type
    content_type = response.headers.get('content-type', '').lower()
    if 'text/html' not in content_type:
        raise ValueError(f"Unexpected content type: {content_type}")
    
    # Check content length
    if len(response.content) < 1000:
        raise ValueError("Response content appears to be too short")
    
    # Check for error pages
    error_indicators = ['404 not found', 'page not found', 'access denied', 'blocked']
    content_text = response.text.lower()
    if any(error in content_text for error in error_indicators):
        raise ValueError("Received error page instead of valid content")
    
    return True

def validate_soup_object(soup):
    """Validate BeautifulSoup parsed content"""
    if not soup:
        raise ValueError("Failed to parse HTML content")
    
    # Check if page structure is as expected
    if not soup.find('title'):
        raise ValueError("Invalid HTML structure - no title tag found")
    
    # Check for Wikipedia-specific elements
    if 'wikipedia' not in soup.get_text().lower()[:5000]:
        raise ValueError("Content doesn't appear to be from Wikipedia")
    
    return True

def validate_url(url):
    """Validate URLs before making requests"""
    if not url or not isinstance(url, str):
        raise ValueError("URL must be a non-empty string")
    
    # Check URL format
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    if not url_pattern.match(url):
        raise ValueError("Invalid URL format")
    
    # Restrict to allowed domains for security
    allowed_domains = ['en.wikipedia.org', 'wikipedia.org']
    parsed_url = urlparse(url)
    if parsed_url.netloc not in allowed_domains:
        raise ValueError(f"URL domain not allowed: {parsed_url.netloc}")
    
    return url

def validate_heritage_site(site_data):
    """Validate individual heritage site data"""
    required_fields = ['name', 'country', 'location', 'year', 'criteria']
    
    if not isinstance(site_data, dict):
        raise ValueError("Site data must be a dictionary")
    
    # Check required fields
    for field in required_fields:
        if field not in site_data:
            raise ValueError(f"Missing required field: {field}")
    
    # Validate name
    name = site_data['name']
    if not isinstance(name, str) or len(name.strip()) < 3:
        raise ValueError("Site name must be a string with at least 3 characters")
    
    if len(name) > 200:
        raise ValueError("Site name cannot exceed 200 characters")
    
    # Validate country
    country = site_data['country']
    if not isinstance(country, str) or len(country.strip()) < 2:
        raise ValueError("Country must be a string with at least 2 characters")
    
    if len(country) > 100:
        raise ValueError("Country name cannot exceed 100 characters")
    
    # Validate year
    year = str(site_data['year'])
    if year != "Unknown":
        if not re.match(r'^(19|20)\d{2}$', year):
            raise ValueError("Year must be a valid 4-digit year between 1900-2099 or 'Unknown'")
    
    # Clean and validate text fields
    for field in ['name', 'country', 'location']:
        if field in site_data:
            site_data[field] = clean_text_field(site_data[field])
    
    return site_data

def clean_text_field(text):
    """Clean and validate text fields"""
    if not isinstance(text, str):
        return str(text)
    
    # Remove HTML tags if any
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove or replace potentially dangerous characters
    text = re.sub(r'[<>"\'\&]', '', text)
    
    return text

def validate_itinerary(itinerary):
    """Validate generated itinerary data structure"""
    required_fields = ['destination', 'country', 'location', 'duration_days', 'itinerary']
    
    if not isinstance(itinerary, dict):
        raise ValueError("Itinerary must be a dictionary")
    
    # Check required fields
    for field in required_fields:
        if field not in itinerary:
            raise ValueError(f"Missing required field in itinerary: {field}")
    
    # Validate duration
    duration = itinerary['duration_days']
    if not isinstance(duration, int) or duration < 1 or duration > 30:
        raise ValueError("Duration must be an integer between 1 and 30")
    
    # Validate itinerary list
    itinerary_list = itinerary['itinerary']
    if not isinstance(itinerary_list, list):
        raise ValueError("Itinerary must be a list")
    
    if len(itinerary_list) != duration:
        raise ValueError("Number of itinerary days doesn't match duration")
    
    # Validate each day
    for i, day_plan in enumerate(itinerary_list, 1):
        if not isinstance(day_plan, dict):
            raise ValueError(f"Day {i} plan must be a dictionary")
        
        day_required_fields = ['day', 'title', 'activities']
        for field in day_required_fields:
            if field not in day_plan:
                raise ValueError(f"Missing field '{field}' in day {i}")
        
        if day_plan['day'] != i:
            raise ValueError(f"Day number mismatch: expected {i}, got {day_plan['day']}")
        
        if not isinstance(day_plan['activities'], list) or len(day_plan['activities']) == 0:
            raise ValueError(f"Day {i} must have a non-empty list of activities")
    
    return itinerary

def validate_filename(filename):
    """Validate filename for saving JSON files"""
    if not filename or not isinstance(filename, str):
        raise ValueError("Filename must be a non-empty string")
    
    # Remove dangerous characters
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    
    # Ensure it ends with .json
    if not filename.lower().endswith('.json'):
        filename += '.json'
    
    # Check length
    if len(filename) > 255:
        raise ValueError("Filename too long")
    
    return filename

def validate_json_data(data):
    """Validate data before JSON serialization"""
    try:
        json.dumps(data)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Data is not JSON serializable: {e}")
    
    return True

def validate_request_rate():
    """Implement basic rate limiting"""
    current_time = time.time()
    if not hasattr(validate_request_rate, 'last_request'):
        validate_request_rate.last_request = current_time
        return True
    
    time_diff = current_time - validate_request_rate.last_request
    if time_diff < 1:  # Minimum 1 second between requests
        raise ValueError("Request rate too high. Please wait before making another request.")
    
    validate_request_rate.last_request = current_time
    return True

def validate_session_headers(headers):
    """Validate session headers"""
    if not isinstance(headers, dict):
        raise ValueError("Headers must be a dictionary")
    
    # Ensure User-Agent is present and reasonable
    user_agent = headers.get('User-Agent', '')
    if not user_agent or len(user_agent) < 10:
        raise ValueError("Valid User-Agent header required")
    
    return headers

def safe_get_text(element):
    """Safely extract text from HTML elements"""
    if element is None:
        return ""
    
    try:
        text = element.get_text().strip()
        # Validate extracted text
        if len(text) > 1000:  # Reasonable limit
            text = text[:1000]
        return clean_text_field(text)
    except Exception:
        return ""

def safe_find_elements(soup, selector, limit=100):
    """Safely find HTML elements with limits"""
    if not soup:
        return []
    
    try:
        elements = soup.find_all(selector)
        # Limit number of elements to prevent memory issues
        return elements[:limit]
    except Exception:
        return []
    
# Example of integrating validation into existing methods
def search_heritage_sites_validated(self, keyword, country):
    """Enhanced search method with validation"""
    # Validate inputs
    keyword = validate_keyword(keyword)
    country = validate_country(country)
    
    # Rate limiting
    validate_request_rate()
    
    # Proceed with original logic but add validation at each step
    # ... existing search logic with validation calls ...
    
    # Validate results before returning
def validate_sites(matching_sites):
    validated_sites = []
    for site in matching_sites:   # now this loops over the actual sites
        try:
            validated_site = validate_heritage_site(site)
            validated_sites.append(validated_site)
        except ValueError as e:
            print(f"Skipping invalid site data: {e}")
            continue
    
    return validated_sites

# Validation configuration
VALIDATION_CONFIG = {
    'MAX_KEYWORD_LENGTH': 100,
    'MAX_COUNTRY_LENGTH': 100,
    'MAX_SITE_NAME_LENGTH': 200,
    'MAX_DAYS': 30,
    'MIN_DAYS': 1,
    'REQUEST_DELAY': 1,  # seconds between requests
    'MAX_SITES_RETURNED': 10,
    'ALLOWED_DOMAINS': ['en.wikipedia.org', 'wikipedia.org'],
    'MAX_FILENAME_LENGTH': 255
}