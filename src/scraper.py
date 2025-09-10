import requests
from bs4 import BeautifulSoup
import json
import re
from urllib.parse import urljoin
import time

class HeritagesSiteScraper:
    def __init__(self):
        self.base_url = "https://en.wikipedia.org"
        self.heritage_sites = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def scrape_all_heritage_sites(self):
        """Scrape all heritage sites from the year-based listing page"""
        url = "https://en.wikipedia.org/wiki/List_of_World_Heritage_Sites_by_year_of_inscription"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            all_sites = []
            
            # Find all tables with heritage sites
            tables = soup.find_all('table', {'class': 'wikitable'})
            
            for table in tables:
                # Look for table headers to identify the structure
                headers = table.find('tr')
                if not headers:
                    continue
                
                header_cells = headers.find_all(['th', 'td'])
                header_texts = [cell.get_text().strip().lower() for cell in header_cells]
                
                # Find column indices
                name_idx = -1
                country_idx = -1
                criteria_idx = -1
                year_idx = -1
                
                for i, header in enumerate(header_texts):
                    if 'name' in header or 'site' in header:
                        name_idx = i
                    elif 'location' in header or 'country' in header or 'state' in header:
                        country_idx = i
                    elif 'criteria' in header:
                        criteria_idx = i
                    elif 'year' in header:
                        year_idx = i
                
                # If we can't find proper columns, use defaults
                if name_idx == -1:
                    name_idx = 0
                if country_idx == -1:
                    country_idx = 1 if len(header_texts) > 1 else 0
                
                # Process data rows
                rows = table.find_all('tr')[1:]  # Skip header row
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) > max(name_idx, country_idx):
                        try:
                            # Extract site name
                            site_name = ""
                            if name_idx < len(cells):
                                site_name = cells[name_idx].get_text().strip()
                                # Remove references and clean up
                                site_name = re.sub(r'\[.*?\]', '', site_name)
                                site_name = re.sub(r'\s+', ' ', site_name).strip()
                            
                            # Extract country/location
                            country = ""
                            if country_idx < len(cells):
                                country = cells[country_idx].get_text().strip()
                                country = re.sub(r'\[.*?\]', '', country)
                                country = re.sub(r'\s+', ' ', country).strip()
                            
                            # Extract criteria
                            criteria = "Unknown"
                            if criteria_idx >= 0 and criteria_idx < len(cells):
                                criteria = cells[criteria_idx].get_text().strip()
                            
                            # Extract year
                            year = "Unknown"
                            if year_idx >= 0 and year_idx < len(cells):
                                year = cells[year_idx].get_text().strip()
                            
                            # Only add if we have a valid site name and country
                            if site_name and len(site_name) > 3 and country and len(country) > 1:
                                all_sites.append({
                                    'name': site_name,
                                    'country': country,
                                    'location': country,
                                    'year': year,
                                    'criteria': criteria
                                })
                        
                        except (IndexError, AttributeError):
                            continue
            
            # Also try to extract from sections with year headers
            sections = soup.find_all(['h2', 'h3', 'h4'])
            current_year = None
            
            for section in sections:
                section_text = section.get_text().strip()
                # Look for year patterns
                year_match = re.search(r'\b(19|20)\d{2}\b', section_text)
                if year_match:
                    current_year = year_match.group()
                    
                    # Find the next table or list after this header
                    next_element = section.find_next_sibling()
                    while next_element and next_element.name not in ['table', 'ul', 'ol', 'h2', 'h3']:
                        next_element = next_element.find_next_sibling()
                    
                    if next_element and next_element.name == 'table':
                        # Process this table with the current year
                        table_sites = self.extract_sites_from_table(next_element, current_year)
                        all_sites.extend(table_sites)
            
            print(f"Total heritage sites found: {len(all_sites)}")
            return all_sites
            
        except requests.RequestException as e:
            print(f"Error fetching heritage sites: {e}")
            return []
    
    def extract_sites_from_table(self, table, year=None):
        """Extract heritage sites from a specific table"""
        sites = []
        
        try:
            # Get header row to understand structure
            header_row = table.find('tr')
            if not header_row:
                return sites
            
            header_cells = header_row.find_all(['th', 'td'])
            header_texts = [cell.get_text().strip().lower() for cell in header_cells]
            
            # Find column indices
            name_idx = 0  # Default to first column
            country_idx = 1  # Default to second column
            criteria_idx = -1
            
            for i, header in enumerate(header_texts):
                if any(word in header for word in ['name', 'site', 'property']):
                    name_idx = i
                elif any(word in header for word in ['location', 'country', 'state party']):
                    country_idx = i
                elif 'criteria' in header:
                    criteria_idx = i
            
            # Process data rows
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                
                if len(cells) > max(name_idx, country_idx):
                    # Extract site name
                    site_name = cells[name_idx].get_text().strip()
                    site_name = re.sub(r'\[.*?\]', '', site_name)  # Remove references
                    site_name = re.sub(r'\s+', ' ', site_name).strip()
                    
                    # Extract country
                    country = cells[country_idx].get_text().strip()
                    country = re.sub(r'\[.*?\]', '', country)
                    country = re.sub(r'\s+', ' ', country).strip()
                    
                    # Extract criteria if available
                    criteria = "Unknown"
                    if criteria_idx >= 0 and criteria_idx < len(cells):
                        criteria = cells[criteria_idx].get_text().strip()
                    
                    if site_name and len(site_name) > 3 and country:
                        sites.append({
                            'name': site_name,
                            'country': country,
                            'location': country,
                            'year': year or 'Unknown',
                            'criteria': criteria
                        })
        
        except Exception as e:
            print(f"Error extracting from table: {e}")
        
        return sites
    
    def search_heritage_sites(self, keyword, country):
        """Search for heritage sites based on keyword and country"""
        print(f"Searching for heritage sites in {country} with keyword '{keyword}'...")
        
        # Get all heritage sites from the year-based page
        if not hasattr(self, 'all_sites_cache'):
            print("Loading all UNESCO World Heritage Sites...")
            self.all_sites_cache = self.scrape_all_heritage_sites()
        
        if not self.all_sites_cache:
            print("Could not load heritage sites data.")
            return []
        
        # Filter sites by country
        country_lower = country.lower()
        country_sites = []
        
        for site in self.all_sites_cache:
            site_country = site['country'].lower()
            # Check if the country matches (exact or partial match)
            if (country_lower in site_country or 
                site_country in country_lower or
                any(country_lower == c.strip().lower() for c in site_country.split(','))):
                country_sites.append(site)
        
        if not country_sites:
            print(f"No heritage sites found for country: {country}")
            print("Available countries include:")
            countries = set()
            for site in self.all_sites_cache[:50]:  # Show sample of countries
                countries.add(site['country'])
            for c in sorted(countries):
                print(f"  - {c}")
            return []
        
        print(f"Found {len(country_sites)} sites in {country}")
        
        # Filter by keyword
        keyword_lower = keyword.lower()
        matching_sites = []
        
        for site in country_sites:
            # Search in name, location, and criteria
            search_text = f"{site['name']} {site['location']} {site['criteria']}".lower()
            if keyword_lower in search_text:
                matching_sites.append(site)
        
        # If no keyword matches, try broader search
        if not matching_sites:
            print(f"No exact matches for '{keyword}'. Trying broader search...")
            # Try partial word matches
            for site in country_sites:
                search_text = f"{site['name']} {site['location']} {site['criteria']}".lower()
                if any(word in search_text for word in keyword_lower.split()):
                    matching_sites.append(site)
        
        return matching_sites[:3]  # Return up to 3 matches
    
    def generate_itinerary(self, site, days):
        """Generate a travel itinerary for the selected heritage site"""
        itinerary = {
            "destination": site['name'],
            "country": site['country'],
            "location": site['location'],
            "duration_days": days,
            "itinerary": []
        }
        
        # Generate day-by-day itinerary
        for day in range(1, days + 1):
            if day == 1:
                day_plan = {
                    "day": day,
                    "title": f"Arrival and First Exploration",
                    "activities": [
                        "Arrive and check into accommodation",
                        f"Initial visit to {site['name']}",
                        "Orientation and site overview",
                        "Local cuisine lunch",
                        "Evening rest and preparation"
                    ]
                }
            elif day == days and days > 1:
                day_plan = {
                    "day": day,
                    "title": f"Final Exploration and Departure",
                    "activities": [
                        f"Final visit to {site['name']}",
                        "Purchase souvenirs and local crafts",
                        "Last-minute photography",
                        "Check out and prepare for departure",
                        "Departure"
                    ]
                }
            else:
                activity_types = [
                    [
                        f"In-depth exploration of {site['name']}",
                        "Guided tour of historical sections",
                        "Photography session",
                        "Local cultural activities",
                        "Traditional lunch"
                    ],
                    [
                        f"Visit surrounding areas near {site['name']}",
                        "Explore local museums",
                        "Meet with local guides/historians",
                        "Cultural workshop or demonstration",
                        "Local market visit"
                    ],
                    [
                        f"Adventure activities around {site['name']}",
                        "Nature walks or hiking",
                        "Visit nearby attractions",
                        "Local transportation experience",
                        "Traditional dinner"
                    ]
                ]
                
                day_plan = {
                    "day": day,
                    "title": f"Full Exploration Day {day-1}",
                    "activities": activity_types[(day-2) % len(activity_types)]
                }
            
            itinerary["itinerary"].append(day_plan)
        
        # Add general information
        itinerary["general_info"] = {
            "best_time_to_visit": "Check local weather and seasonal conditions",
            "estimated_budget": f"${100 * days} - ${300 * days} per person (excluding flights)",
            "recommended_accommodation": "Local hotels or guesthouses near the heritage site",
            "transportation": "Research local transportation options and book in advance",
            "important_notes": [
                "Check visa requirements for the country",
                "Respect local customs and heritage site rules",
                "Book accommodations and tours in advance",
                "Carry appropriate identification and permits",
                "Consider hiring local guides for better experience"
            ]
        }
        
        return itinerary
    
    def save_itinerary_to_json(self, itinerary, filename=None):
        """Save the itinerary to a JSON file"""
        if filename is None:
            site_name = re.sub(r'[^\w\s-]', '', itinerary['destination']).strip()
            site_name = re.sub(r'[-\s]+', '-', site_name)
            filename = f"itinerary_{site_name}_{itinerary['duration_days']}days.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(itinerary, f, indent=4, ensure_ascii=False)
            print(f"Itinerary saved to: {filename}")
            return filename
        except Exception as e:
            print(f"Error saving itinerary: {e}")
            return None

def main():
    scraper = HeritagesSiteScraper()
    
    print("=== UNESCO World Heritage Sites Travel Planner ===\n")
    
    # Get user input
    keyword = input("Enter a keyword to search for (e.g., 'temple', 'castle', 'natural'): ").strip()
    if not keyword:
        print("Keyword cannot be empty!")
        return
    
    country = input("Enter a country name: ").strip()
    if not country:
        print("Country name cannot be empty!")
        return
    
    # Search for heritage sites
    sites = scraper.search_heritage_sites(keyword, country)
    
    if not sites:
        print(f"No heritage sites found matching '{keyword}' in {country}")
        return
    
    # Display found sites
    print(f"\nFound {len(sites)} heritage site(s) matching your criteria:\n")
    for i, site in enumerate(sites, 1):
        print(f"{i}. {site['name']}")
        print(f"   Location: {site['location']}")
        print(f"   Year: {site['year']}")
        print(f"   Criteria: {site['criteria']}")
        print()
    
    # User selects a site
    while True:
        try:
            choice = int(input(f"Select a site (1-{len(sites)}): "))
            if 1 <= choice <= len(sites):
                selected_site = sites[choice - 1]
                break
            else:
                print(f"Please enter a number between 1 and {len(sites)}")
        except ValueError:
            print("Please enter a valid number")
    
    # Get number of days
    while True:
        try:
            days = int(input("Enter number of days for the trip (1-30): "))
            if 1 <= days <= 30:
                break
            else:
                print("Please enter a number between 1 and 30")
        except ValueError:
            print("Please enter a valid number")
    
    # Generate itinerary
    print(f"\nGenerating {days}-day itinerary for {selected_site['name']}...")
    itinerary = scraper.generate_itinerary(selected_site, days)
    
    # Save to JSON
    filename = scraper.save_itinerary_to_json(itinerary)
    
    if filename:
        print(f"\nItinerary successfully generated and saved!")
        print(f"Destination: {itinerary['destination']}")
        print(f"Duration: {days} days")
        print(f"File: {filename}")
    
    # Display a preview of the itinerary
    print(f"\n=== Itinerary Preview ===")
    for day_plan in itinerary['itinerary'][:2]:  # Show first 2 days
        print(f"\nDay {day_plan['day']}: {day_plan['title']}")
        for activity in day_plan['activities']:
            print(f"  • {activity}")
    
    if len(itinerary['itinerary']) > 2:
        print(f"\n... and {len(itinerary['itinerary']) - 2} more days")
    
    print(f"\nFull itinerary saved to {filename}")

if __name__ == "__main__":
    main()