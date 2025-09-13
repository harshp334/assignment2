import json
import re
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import unicodedata

class CriteriaType(Enum):
    CULTURAL = "Cultural"
    NATURAL = "Natural"
    MIXED = "Mixed"
    UNKNOWN = "Unknown"

@dataclass
class StandardizedSite:
    """Standardized heritage site data structure"""
    id: str
    name: str
    normalized_name: str
    country: str
    iso_country_code: Optional[str]
    region: Optional[str]
    continent: Optional[str]
    location: str
    coordinates: Optional[Tuple[float, float]]
    inscription_year: int
    criteria_type: CriteriaType
    criteria_numbers: List[str]
    criteria_description: str
    endangered_status: bool
    area_hectares: Optional[float]
    buffer_zone_hectares: Optional[float]
    description: Optional[str]
    tags: List[str]
    last_updated: str
    data_quality_score: float
    source_url: Optional[str]

class HeritageDataTransformationPipeline:
    """Comprehensive data transformation pipeline for heritage site data"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.country_mappings = self._load_country_mappings()
        self.criteria_mappings = self._load_criteria_mappings()
        self.region_mappings = self._load_region_mappings()
        self.transformation_stats = {
            'processed': 0,
            'transformed': 0,
            'enriched': 0,
            'errors': 0,
            'quality_scores': []
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the pipeline"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('heritage_pipeline.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def _load_country_mappings(self) -> Dict[str, Dict[str, str]]:
        """Load country name to ISO code and continent mappings"""
        return {
            'afghanistan': {'iso': 'AF', 'continent': 'Asia', 'region': 'South Asia'},
            'albania': {'iso': 'AL', 'continent': 'Europe', 'region': 'Southeast Europe'},
            'algeria': {'iso': 'DZ', 'continent': 'Africa', 'region': 'North Africa'},
            'argentina': {'iso': 'AR', 'continent': 'South America', 'region': 'South America'},
            'australia': {'iso': 'AU', 'continent': 'Oceania', 'region': 'Australia and New Zealand'},
            'austria': {'iso': 'AT', 'continent': 'Europe', 'region': 'Western Europe'},
            'bangladesh': {'iso': 'BD', 'continent': 'Asia', 'region': 'South Asia'},
            'belgium': {'iso': 'BE', 'continent': 'Europe', 'region': 'Western Europe'},
            'brazil': {'iso': 'BR', 'continent': 'South America', 'region': 'South America'},
            'cambodia': {'iso': 'KH', 'continent': 'Asia', 'region': 'Southeast Asia'},
            'canada': {'iso': 'CA', 'continent': 'North America', 'region': 'North America'},
            'china': {'iso': 'CN', 'continent': 'Asia', 'region': 'East Asia'},
            'colombia': {'iso': 'CO', 'continent': 'South America', 'region': 'South America'},
            'croatia': {'iso': 'HR', 'continent': 'Europe', 'region': 'Southeast Europe'},
            'czech republic': {'iso': 'CZ', 'continent': 'Europe', 'region': 'Eastern Europe'},
            'denmark': {'iso': 'DK', 'continent': 'Europe', 'region': 'Northern Europe'},
            'ecuador': {'iso': 'EC', 'continent': 'South America', 'region': 'South America'},
            'egypt': {'iso': 'EG', 'continent': 'Africa', 'region': 'North Africa'},
            'ethiopia': {'iso': 'ET', 'continent': 'Africa', 'region': 'East Africa'},
            'france': {'iso': 'FR', 'continent': 'Europe', 'region': 'Western Europe'},
            'germany': {'iso': 'DE', 'continent': 'Europe', 'region': 'Western Europe'},
            'greece': {'iso': 'GR', 'continent': 'Europe', 'region': 'Southern Europe'},
            'guatemala': {'iso': 'GT', 'continent': 'North America', 'region': 'Central America'},
            'india': {'iso': 'IN', 'continent': 'Asia', 'region': 'South Asia'},
            'indonesia': {'iso': 'ID', 'continent': 'Asia', 'region': 'Southeast Asia'},
            'iran': {'iso': 'IR', 'continent': 'Asia', 'region': 'Western Asia'},
            'iraq': {'iso': 'IQ', 'continent': 'Asia', 'region': 'Western Asia'},
            'italy': {'iso': 'IT', 'continent': 'Europe', 'region': 'Southern Europe'},
            'japan': {'iso': 'JP', 'continent': 'Asia', 'region': 'East Asia'},
            'jordan': {'iso': 'JO', 'continent': 'Asia', 'region': 'Western Asia'},
            'kenya': {'iso': 'KE', 'continent': 'Africa', 'region': 'East Africa'},
            'madagascar': {'iso': 'MG', 'continent': 'Africa', 'region': 'East Africa'},
            'malaysia': {'iso': 'MY', 'continent': 'Asia', 'region': 'Southeast Asia'},
            'mexico': {'iso': 'MX', 'continent': 'North America', 'region': 'North America'},
            'morocco': {'iso': 'MA', 'continent': 'Africa', 'region': 'North Africa'},
            'nepal': {'iso': 'NP', 'continent': 'Asia', 'region': 'South Asia'},
            'netherlands': {'iso': 'NL', 'continent': 'Europe', 'region': 'Western Europe'},
            'new zealand': {'iso': 'NZ', 'continent': 'Oceania', 'region': 'Australia and New Zealand'},
            'norway': {'iso': 'NO', 'continent': 'Europe', 'region': 'Northern Europe'},
            'pakistan': {'iso': 'PK', 'continent': 'Asia', 'region': 'South Asia'},
            'peru': {'iso': 'PE', 'continent': 'South America', 'region': 'South America'},
            'poland': {'iso': 'PL', 'continent': 'Europe', 'region': 'Eastern Europe'},
            'portugal': {'iso': 'PT', 'continent': 'Europe', 'region': 'Southern Europe'},
            'romania': {'iso': 'RO', 'continent': 'Europe', 'region': 'Eastern Europe'},
            'russia': {'iso': 'RU', 'continent': 'Europe', 'region': 'Eastern Europe'},
            'senegal': {'iso': 'SN', 'continent': 'Africa', 'region': 'West Africa'},
            'south africa': {'iso': 'ZA', 'continent': 'Africa', 'region': 'Southern Africa'},
            'spain': {'iso': 'ES', 'continent': 'Europe', 'region': 'Southern Europe'},
            'sri lanka': {'iso': 'LK', 'continent': 'Asia', 'region': 'South Asia'},
            'sweden': {'iso': 'SE', 'continent': 'Europe', 'region': 'Northern Europe'},
            'switzerland': {'iso': 'CH', 'continent': 'Europe', 'region': 'Western Europe'},
            'syria': {'iso': 'SY', 'continent': 'Asia', 'region': 'Western Asia'},
            'tanzania': {'iso': 'TZ', 'continent': 'Africa', 'region': 'East Africa'},
            'thailand': {'iso': 'TH', 'continent': 'Asia', 'region': 'Southeast Asia'},
            'tunisia': {'iso': 'TN', 'continent': 'Africa', 'region': 'North Africa'},
            'turkey': {'iso': 'TR', 'continent': 'Asia', 'region': 'Western Asia'},
            'ukraine': {'iso': 'UA', 'continent': 'Europe', 'region': 'Eastern Europe'},
            'united kingdom': {'iso': 'GB', 'continent': 'Europe', 'region': 'Northern Europe'},
            'united states': {'iso': 'US', 'continent': 'North America', 'region': 'North America'},
            'vietnam': {'iso': 'VN', 'continent': 'Asia', 'region': 'Southeast Asia'},
            'yemen': {'iso': 'YE', 'continent': 'Asia', 'region': 'Western Asia'},
            'zimbabwe': {'iso': 'ZW', 'continent': 'Africa', 'region': 'Southern Africa'}
        }
    
    def _load_criteria_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Load UNESCO criteria mappings"""
        return {
            'i': {
                'type': CriteriaType.CULTURAL,
                'description': 'Masterpiece of human creative genius'
            },
            'ii': {
                'type': CriteriaType.CULTURAL,
                'description': 'Important interchange of human values'
            },
            'iii': {
                'type': CriteriaType.CULTURAL,
                'description': 'Testimony to cultural tradition or civilization'
            },
            'iv': {
                'type': CriteriaType.CULTURAL,
                'description': 'Outstanding example of architectural/technological ensemble'
            },
            'v': {
                'type': CriteriaType.CULTURAL,
                'description': 'Outstanding example of human settlement/land use'
            },
            'vi': {
                'type': CriteriaType.CULTURAL,
                'description': 'Associated with events/traditions/beliefs/artistic works'
            },
            'vii': {
                'type': CriteriaType.NATURAL,
                'description': 'Contains superlative natural phenomena'
            },
            'viii': {
                'type': CriteriaType.NATURAL,
                'description': 'Outstanding example of major stages of earth history'
            },
            'ix': {
                'type': CriteriaType.NATURAL,
                'description': 'Outstanding example of ecological/biological processes'
            },
            'x': {
                'type': CriteriaType.NATURAL,
                'description': 'Contains important/significant natural habitats for biodiversity'
            }
        }
    
    def _load_region_mappings(self) -> Dict[str, str]:
        """Load region to continent mappings"""
        return {
            'South Asia': 'Asia',
            'Southeast Asia': 'Asia',
            'East Asia': 'Asia',
            'Western Asia': 'Asia',
            'Central Asia': 'Asia',
            'Western Europe': 'Europe',
            'Eastern Europe': 'Europe',
            'Southern Europe': 'Europe',
            'Northern Europe': 'Europe',
            'Southeast Europe': 'Europe',
            'North Africa': 'Africa',
            'West Africa': 'Africa',
            'East Africa': 'Africa',
            'Southern Africa': 'Africa',
            'Central Africa': 'Africa',
            'North America': 'North America',
            'Central America': 'North America',
            'South America': 'South America',
            'Australia and New Zealand': 'Oceania',
            'Melanesia': 'Oceania',
            'Micronesia': 'Oceania',
            'Polynesia': 'Oceania'
        }
    
    def transform_pipeline(self, raw_data: List[Dict[str, Any]]) -> List[StandardizedSite]:
        """Main transformation pipeline"""
        self.logger.info(f"Starting transformation pipeline for {len(raw_data)} records")
        
        standardized_sites = []
        
        for raw_site in raw_data:
            try:
                self.transformation_stats['processed'] += 1
                
                # Stage 1: Data Cleaning
                cleaned_site = self._clean_data(raw_site)
                
                # Stage 2: Data Validation
                if not self._validate_cleaned_data(cleaned_site):
                    self.transformation_stats['errors'] += 1
                    continue
                
                # Stage 3: Data Standardization
                standardized_site = self._standardize_data(cleaned_site)
                
                # Stage 4: Data Enrichment
                enriched_site = self._enrich_data(standardized_site)
                
                # Stage 5: Quality Assessment
                quality_score = self._calculate_quality_score(enriched_site)
                enriched_site.data_quality_score = quality_score
                self.transformation_stats['quality_scores'].append(quality_score)
                
                # Stage 6: Tag Generation
                enriched_site.tags = self._generate_tags(enriched_site)
                
                standardized_sites.append(enriched_site)
                self.transformation_stats['transformed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming site {raw_site.get('name', 'unknown')}: {e}")
                self.transformation_stats['errors'] += 1
        
        self.logger.info(f"Transformation completed. {len(standardized_sites)} sites processed successfully")
        return standardized_sites
    
    def _clean_data(self, raw_site: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Clean raw data"""
        cleaned = {}
        
        # Clean name
        name = raw_site.get('name', '').strip()
        name = re.sub(r'\[.*?\]', '', name)  # Remove references
        name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
        name = self._normalize_unicode(name)
        cleaned['name'] = name
        
        # Clean country
        country = raw_site.get('country', '').strip()
        country = re.sub(r'\[.*?\]', '', country)
        country = re.sub(r'\s+', ' ', country)
        country = self._normalize_unicode(country)
        cleaned['country'] = country
        
        # Clean location
        location = raw_site.get('location', country).strip()
        location = re.sub(r'\[.*?\]', '', location)
        location = re.sub(r'\s+', ' ', location)
        location = self._normalize_unicode(location)
        cleaned['location'] = location
        
        # Clean year
        year = str(raw_site.get('year', 'Unknown')).strip()
        cleaned['year'] = year
        
        # Clean criteria
        criteria = raw_site.get('criteria', 'Unknown').strip()
        criteria = re.sub(r'\[.*?\]', '', criteria)
        cleaned['criteria'] = criteria
        
        return cleaned
    
    def _normalize_unicode(self, text: str) -> str:
        """Normalize unicode characters"""
        if not text:
            return text
        
        # Normalize unicode
        text = unicodedata.normalize('NFKD', text)
        
        # Remove or replace problematic characters
        text = re.sub(r'[^\w\s\-.,()\'\"/:&]', '', text, flags=re.UNICODE)
        
        return text.strip()
    
    def _validate_cleaned_data(self, cleaned_site: Dict[str, Any]) -> bool:
        """Stage 2: Validate cleaned data"""
        # Check required fields
        if not cleaned_site.get('name') or len(cleaned_site['name']) < 3:
            self.logger.warning(f"Invalid name: {cleaned_site.get('name')}")
            return False
        
        if not cleaned_site.get('country') or len(cleaned_site['country']) < 2:
            self.logger.warning(f"Invalid country: {cleaned_site.get('country')}")
            return False
        
        return True
    
    def _standardize_data(self, cleaned_site: Dict[str, Any]) -> StandardizedSite:
        """Stage 3: Standardize data structure"""
        # Generate unique ID
        site_id = self._generate_site_id(cleaned_site['name'], cleaned_site['country'])
        
        # Standardize year
        inscription_year = self._parse_year(cleaned_site['year'])
        
        # Parse criteria
        criteria_type, criteria_numbers, criteria_description = self._parse_criteria(cleaned_site['criteria'])
        
        # Create standardized site
        standardized_site = StandardizedSite(
            id=site_id,
            name=cleaned_site['name'],
            normalized_name=self._normalize_name(cleaned_site['name']),
            country=cleaned_site['country'],
            iso_country_code=None,  # Will be enriched
            region=None,  # Will be enriched
            continent=None,  # Will be enriched
            location=cleaned_site['location'],
            coordinates=None,  # Will be enriched if available
            inscription_year=inscription_year,
            criteria_type=criteria_type,
            criteria_numbers=criteria_numbers,
            criteria_description=criteria_description,
            endangered_status=False,  # Will be enriched if available
            area_hectares=None,  # Will be enriched if available
            buffer_zone_hectares=None,  # Will be enriched if available
            description=None,  # Will be enriched if available
            tags=[],  # Will be generated
            last_updated=datetime.now().isoformat(),
            data_quality_score=0.0,  # Will be calculated
            source_url=None  # Will be enriched if available
        )
        
        return standardized_site
    
    def _generate_site_id(self, name: str, country: str) -> str:
        """Generate unique site ID"""
        # Create a normalized identifier
        normalized = f"{name}_{country}".lower()
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', '_', normalized)
        return normalized[:50]  # Limit length
    
    def _normalize_name(self, name: str) -> str:
        """Create normalized searchable name"""
        normalized = name.lower()
        # Remove common prefixes/suffixes
        prefixes = ['the ', 'historic ', 'archaeological ', 'national park of ', 'ruins of ']
        suffixes = [' national park', ' archaeological site', ' historic site']
        
        for prefix in prefixes:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
        
        for suffix in suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
        
        return normalized.strip()
    
    def _parse_year(self, year_str: str) -> int:
        """Parse inscription year"""
        if not year_str or year_str.lower() == 'unknown':
            return 0
        
        # Extract 4-digit year
        year_match = re.search(r'\b(19|20)\d{2}\b', year_str)
        if year_match:
            return int(year_match.group())
        
        return 0
    
    def _parse_criteria(self, criteria_str: str) -> Tuple[CriteriaType, List[str], str]:
        """Parse UNESCO criteria"""
        if not criteria_str or criteria_str.lower() == 'unknown':
            return CriteriaType.UNKNOWN, [], "Unknown criteria"
        
        # Extract Roman numerals
        criteria_numbers = re.findall(r'\b(?:i{1,3}v?|vi{0,3}|i?x)\b', criteria_str.lower())
        
        if not criteria_numbers:
            # Try to extract regular numbers
            numbers = re.findall(r'\b([1-9]|10)\b', criteria_str)
            criteria_numbers = [self._convert_to_roman(int(n)) for n in numbers if 1 <= int(n) <= 10]
        
        # Determine criteria type
        criteria_type = CriteriaType.UNKNOWN
        descriptions = []
        
        for criteria_num in criteria_numbers:
            if criteria_num in self.criteria_mappings:
                mapping = self.criteria_mappings[criteria_num]
                descriptions.append(mapping['description'])
                
                if criteria_type == CriteriaType.UNKNOWN:
                    criteria_type = mapping['type']
                elif criteria_type != mapping['type']:
                    criteria_type = CriteriaType.MIXED
        
        criteria_description = '; '.join(descriptions) if descriptions else criteria_str
        
        return criteria_type, criteria_numbers, criteria_description
    
    def _convert_to_roman(self, num: int) -> str:
        """Convert number to Roman numeral"""
        romans = {1: 'i', 2: 'ii', 3: 'iii', 4: 'iv', 5: 'v', 
                  6: 'vi', 7: 'vii', 8: 'viii', 9: 'ix', 10: 'x'}
        return romans.get(num, str(num))
    
    def _enrich_data(self, site: StandardizedSite) -> StandardizedSite:
        """Stage 4: Enrich data with additional information"""
        # Enrich country information
        country_key = site.country.lower().strip()
        
        # Handle multi-country entries
        if ',' in country_key:
            country_key = country_key.split(',')[0].strip()
        
        # Try different country name variations
        country_variations = [
            country_key,
            country_key.replace('the ', ''),
            country_key.replace(' of', ''),
            country_key.split('(')[0].strip() if '(' in country_key else country_key
        ]
        
        for variation in country_variations:
            if variation in self.country_mappings:
                mapping = self.country_mappings[variation]
                site.iso_country_code = mapping['iso']
                site.continent = mapping['continent']
                site.region = mapping['region']
                break
        
        # Enrich with keyword analysis for missing data
        if not site.region and site.location:
            site.region = self._infer_region_from_location(site.location)
        
        # Check for endangered status keywords
        site.endangered_status = self._check_endangered_status(site.name, site.criteria_description)
        
        self.transformation_stats['enriched'] += 1
        return site
    
    def _infer_region_from_location(self, location: str) -> Optional[str]:
        """Infer region from location text"""
        location_lower = location.lower()
        
        region_keywords = {
            'South Asia': ['india', 'pakistan', 'bangladesh', 'sri lanka', 'nepal', 'bhutan'],
            'Southeast Asia': ['thailand', 'vietnam', 'cambodia', 'laos', 'myanmar', 'malaysia', 'singapore', 'indonesia', 'philippines'],
            'East Asia': ['china', 'japan', 'korea', 'mongolia', 'taiwan'],
            'Western Asia': ['turkey', 'syria', 'iraq', 'iran', 'lebanon', 'jordan', 'israel', 'palestine'],
            'Western Europe': ['france', 'germany', 'netherlands', 'belgium', 'switzerland', 'austria'],
            'Southern Europe': ['italy', 'spain', 'greece', 'portugal', 'malta', 'croatia'],
            'Northern Europe': ['norway', 'sweden', 'denmark', 'finland', 'iceland', 'uk', 'britain'],
            'Eastern Europe': ['poland', 'czech', 'slovakia', 'hungary', 'romania', 'bulgaria', 'russia'],
            'North Africa': ['egypt', 'libya', 'tunisia', 'algeria', 'morocco', 'sudan'],
            'West Africa': ['senegal', 'mali', 'ghana', 'nigeria', 'benin', 'burkina faso'],
            'East Africa': ['kenya', 'tanzania', 'ethiopia', 'uganda', 'madagascar'],
            'Southern Africa': ['south africa', 'zimbabwe', 'botswana', 'namibia', 'zambia']
        }
        
        for region, keywords in region_keywords.items():
            if any(keyword in location_lower for keyword in keywords):
                return region
        
        return None
    
    def _check_endangered_status(self, name: str, criteria: str) -> bool:
        """Check if site might be endangered based on keywords"""
        endangered_keywords = ['endangered', 'threat', 'risk', 'damage', 'destruction', 'conflict', 'war']
        text_to_check = f"{name} {criteria}".lower()
        return any(keyword in text_to_check for keyword in endangered_keywords)
    
    def _calculate_quality_score(self, site: StandardizedSite) -> float:
        """Stage 5: Calculate data quality score (0-1)"""
        score = 0.0
        max_score = 10.0
        
        # Name quality (1 point)
        if site.name and len(site.name) >= 5:
            score += 1.0
        
        # Country data (1 point)
        if site.iso_country_code:
            score += 1.0
        
        # Geographic data (2 points)
        if site.region:
            score += 1.0
        if site.continent:
            score += 1.0
        
        # Temporal data (1 point)
        if site.inscription_year > 0:
            score += 1.0
        
        # Criteria data (2 points)
        if site.criteria_type != CriteriaType.UNKNOWN:
            score += 1.0
        if site.criteria_numbers:
            score += 1.0
        
        # Completeness (2 points)
        if site.location != site.country:  # Has specific location
            score += 1.0
        if len(site.criteria_description) > 20:  # Has detailed criteria
            score += 1.0
        
        # Freshness (1 point)
        if site.last_updated:
            score += 1.0
        
        return score / max_score
    
    def _generate_tags(self, site: StandardizedSite) -> List[str]:
        """Stage 6: Generate searchable tags"""
        tags = set()
        
        # Add criteria type
        tags.add(site.criteria_type.value.lower())
        
        # Add geographic tags
        if site.continent:
            tags.add(site.continent.lower())
        if site.region:
            tags.add(site.region.lower().replace(' ', '_'))
        if site.country:
            tags.add(site.country.lower().replace(' ', '_'))
        
        # Add time period tags
        if site.inscription_year > 0:
            decade = (site.inscription_year // 10) * 10
            tags.add(f"{decade}s")
            
            if site.inscription_year < 1980:
                tags.add("early_inscription")
            elif site.inscription_year < 2000:
                tags.add("mid_inscription")
            else:
                tags.add("recent_inscription")
        
        # Add keyword-based tags
        name_lower = site.name.lower()
        
        # Architecture tags
        arch_keywords = {
            'temple': ['temple', 'shrine', 'pagoda', 'monastery'],
            'castle': ['castle', 'fortress', 'fort', 'palace'],
            'church': ['church', 'cathedral', 'basilica', 'abbey'],
            'ancient': ['ancient', 'prehistoric', 'archaeological'],
            'historic_center': ['historic center', 'old town', 'historic city'],
            'industrial': ['industrial', 'mining', 'railway', 'factory']
        }
        
        for tag, keywords in arch_keywords.items():
            if any(keyword in name_lower for keyword in keywords):
                tags.add(tag)
        
        # Natural tags
        nature_keywords = {
            'national_park': ['national park', 'park'],
            'mountain': ['mountain', 'mount', 'peak', 'range'],
            'forest': ['forest', 'rainforest', 'woodland'],
            'marine': ['marine', 'reef', 'ocean', 'sea'],
            'desert': ['desert'],
            'volcanic': ['volcanic', 'volcano'],
            'cave': ['cave', 'cavern', 'grotto']
        }
        
        for tag, keywords in nature_keywords.items():
            if any(keyword in name_lower for keyword in keywords):
                tags.add(tag)
        
        # Status tags
        if site.endangered_status:
            tags.add('endangered')
        
        if site.data_quality_score >= 0.8:
            tags.add('high_quality_data')
        elif site.data_quality_score >= 0.6:
            tags.add('medium_quality_data')
        else:
            tags.add('low_quality_data')
        
        return sorted(list(tags))
    
    def export_to_formats(self, sites: List[StandardizedSite], base_filename: str = "heritage_sites"):
        """Export transformed data to multiple formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export to JSON
        json_filename = f"{base_filename}_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump([asdict(site) for site in sites], f, indent=2, ensure_ascii=False, default=str)
        self.logger.info(f"Exported {len(sites)} sites to {json_filename}")
        
        # Export to CSV
        csv_filename = f"{base_filename}_{timestamp}.csv"
        df = pd.DataFrame([asdict(site) for site in sites])
        
        # Flatten complex fields for CSV
        df['criteria_numbers'] = df['criteria_numbers'].apply(lambda x: ','.join(x) if x else '')
        df['tags'] = df['tags'].apply(lambda x: ','.join(x) if x else '')
        df['coordinates'] = df['coordinates'].apply(lambda x: f"{x[0]},{x[1]}" if x else '')
        
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        self.logger.info(f"Exported {len(sites)} sites to {csv_filename}")
        
        # Export summary statistics
        stats_filename = f"{base_filename}_stats_{timestamp}.json"
        stats = self._generate_summary_stats(sites)
        with open(stats_filename, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False, default=str)
        self.logger.info(f"Exported summary statistics to {stats_filename}")
        
        return {
            'json_file': json_filename,
            'csv_file': csv_filename,
            'stats_file': stats_filename
        }
    
    def _generate_summary_stats(self, sites: List[StandardizedSite]) -> Dict[str, Any]:
        """Generate summary statistics for the dataset"""
        stats = {
            'total_sites': len(sites),
            'transformation_stats': self.transformation_stats.copy(),
            'data_quality': {
                'average_quality_score': sum(self.transformation_stats['quality_scores']) / len(self.transformation_stats['quality_scores']) if self.transformation_stats['quality_scores'] else 0,
                'high_quality_sites': len([s for s in sites if s.data_quality_score >= 0.8]),
                'medium_quality_sites': len([s for s in sites if 0.6 <= s.data_quality_score < 0.8]),
                'low_quality_sites': len([s for s in sites if s.data_quality_score < 0.6])
            },
            'geographic_distribution': {},
            'criteria_distribution': {},
            'temporal_distribution': {},
            'tag_frequency': {}
        }
        
        # Geographic distribution
        continents = {}
        regions = {}
        countries = {}
        
        for site in sites:
            if site.continent:
                continents[site.continent] = continents.get(site.continent, 0) + 1
            if site.region:
                regions[site.region] = regions.get(site.region, 0) + 1
            if site.country:
                countries[site.country] = countries.get(site.country, 0) + 1
        
        stats['geographic_distribution'] = {
            'by_continent': dict(sorted(continents.items(), key=lambda x: x[1], reverse=True)),
            'by_region': dict(sorted(regions.items(), key=lambda x: x[1], reverse=True)[:20]),  # Top 20
            'by_country': dict(sorted(countries.items(), key=lambda x: x[1], reverse=True)[:20])  # Top 20
        }
        
        # Criteria distribution
        criteria_types = {}
        for site in sites:
            criteria_types[site.criteria_type.value] = criteria_types.get(site.criteria_type.value, 0) + 1
        
        stats['criteria_distribution'] = dict(sorted(criteria_types.items(), key=lambda x: x[1], reverse=True))
        
        # Temporal distribution
        decades = {}
        for site in sites:
            if site.inscription_year > 0:
                decade = (site.inscription_year // 10) * 10
                decade_label = f"{decade}s"
                decades[decade_label] = decades.get(decade_label, 0) + 1
        
        stats['temporal_distribution'] = dict(sorted(decades.items()))
        
        # Tag frequency
        all_tags = []
        for site in sites:
            all_tags.extend(site.tags)
        
        tag_counts = {}
        for tag in all_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        stats['tag_frequency'] = dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:30])  # Top 30
        
        return stats
    
    def create_data_catalog(self, sites: List[StandardizedSite]) -> Dict[str, Any]:
        """Create a data catalog for the transformed dataset"""
        catalog = {
            'metadata': {
                'dataset_name': 'UNESCO World Heritage Sites',
                'version': '1.0',
                'created_date': datetime.now().isoformat(),
                'total_records': len(sites),
                'data_pipeline_version': '1.0',
                'source': 'Wikipedia - List of World Heritage Sites',
                'license': 'CC BY-SA 4.0',
                'contact': 'heritage-data-pipeline@example.com'
            },
            'schema': {
                'fields': [
                    {
                        'name': 'id',
                        'type': 'string',
                        'description': 'Unique identifier for the heritage site',
                        'example': 'angkor_wat_cambodia'
                    },
                    {
                        'name': 'name',
                        'type': 'string',
                        'description': 'Official name of the heritage site',
                        'example': 'Angkor Wat'
                    },
                    {
                        'name': 'normalized_name',
                        'type': 'string',
                        'description': 'Normalized name for searching',
                        'example': 'angkor wat'
                    },
                    {
                        'name': 'country',
                        'type': 'string',
                        'description': 'Country where the site is located',
                        'example': 'Cambodia'
                    },
                    {
                        'name': 'iso_country_code',
                        'type': 'string',
                        'description': 'ISO 3166-1 alpha-2 country code',
                        'example': 'KH'
                    },
                    {
                        'name': 'region',
                        'type': 'string',
                        'description': 'Geographic region',
                        'example': 'Southeast Asia'
                    },
                    {
                        'name': 'continent',
                        'type': 'string',
                        'description': 'Continent where the site is located',
                        'example': 'Asia'
                    },
                    {
                        'name': 'location',
                        'type': 'string',
                        'description': 'Detailed location information',
                        'example': 'Siem Reap Province, Cambodia'
                    },
                    {
                        'name': 'coordinates',
                        'type': 'tuple',
                        'description': 'Latitude and longitude coordinates',
                        'example': '[13.4125, 103.8667]'
                    },
                    {
                        'name': 'inscription_year',
                        'type': 'integer',
                        'description': 'Year the site was inscribed as World Heritage Site',
                        'example': 1992
                    },
                    {
                        'name': 'criteria_type',
                        'type': 'enum',
                        'description': 'Type of UNESCO criteria (Cultural/Natural/Mixed/Unknown)',
                        'example': 'Cultural'
                    },
                    {
                        'name': 'criteria_numbers',
                        'type': 'list',
                        'description': 'List of UNESCO criteria numbers (i-x)',
                        'example': ['i', 'ii', 'iii', 'iv']
                    },
                    {
                        'name': 'criteria_description',
                        'type': 'string',
                        'description': 'Description of the UNESCO criteria',
                        'example': 'Masterpiece of human creative genius; Important interchange of human values'
                    },
                    {
                        'name': 'endangered_status',
                        'type': 'boolean',
                        'description': 'Whether the site is considered endangered',
                        'example': False
                    },
                    {
                        'name': 'area_hectares',
                        'type': 'float',
                        'description': 'Area of the site in hectares',
                        'example': 401.0
                    },
                    {
                        'name': 'buffer_zone_hectares',
                        'type': 'float',
                        'description': 'Area of the buffer zone in hectares',
                        'example': 1200.0
                    },
                    {
                        'name': 'description',
                        'type': 'string',
                        'description': 'Detailed description of the site',
                        'example': 'Ancient temple complex...'
                    },
                    {
                        'name': 'tags',
                        'type': 'list',
                        'description': 'Searchable tags for the site',
                        'example': ['temple', 'cultural', 'southeast_asia', 'ancient']
                    },
                    {
                        'name': 'last_updated',
                        'type': 'datetime',
                        'description': 'Timestamp of last data update',
                        'example': '2024-12-19T10:30:00'
                    },
                    {
                        'name': 'data_quality_score',
                        'type': 'float',
                        'description': 'Data quality score (0-1)',
                        'example': 0.85
                    },
                    {
                        'name': 'source_url',
                        'type': 'string',
                        'description': 'Source URL for the data',
                        'example': 'https://en.wikipedia.org/wiki/Angkor_Wat'
                    }
                ]
            },
            'quality_metrics': {
                'completeness': {
                    'name': self._calculate_field_completeness(sites, 'name'),
                    'country': self._calculate_field_completeness(sites, 'country'),
                    'iso_country_code': self._calculate_field_completeness(sites, 'iso_country_code'),
                    'region': self._calculate_field_completeness(sites, 'region'),
                    'continent': self._calculate_field_completeness(sites, 'continent'),
                    'inscription_year': len([s for s in sites if s.inscription_year > 0]) / len(sites) if sites else 0,
                    'criteria_type': len([s for s in sites if s.criteria_type != CriteriaType.UNKNOWN]) / len(sites) if sites else 0
                },
                'data_types': {
                    'valid_years': len([s for s in sites if 1900 <= s.inscription_year <= 2030]) / len(sites) if sites else 0,
                    'valid_criteria': len([s for s in sites if s.criteria_numbers]) / len(sites) if sites else 0
                },
                'consistency': {
                    'country_iso_match': len([s for s in sites if s.country and s.iso_country_code]) / len([s for s in sites if s.country]) if sites else 0
                }
            },
            'usage_examples': [
                {
                    'description': 'Find all cultural sites in Asia',
                    'query': "filter(criteria_type='Cultural' AND continent='Asia')"
                },
                {
                    'description': 'Get sites inscribed in the 1990s',
                    'query': "filter(inscription_year >= 1990 AND inscription_year < 2000)"
                },
                {
                    'description': 'Search for temple sites',
                    'query': "filter('temple' in tags)"
                },
                {
                    'description': 'Find high-quality data records',
                    'query': "filter(data_quality_score >= 0.8)"
                }
            ]
        }
        
        return catalog
    
    def _calculate_field_completeness(self, sites: List[StandardizedSite], field: str) -> float:
        """Calculate completeness ratio for a specific field"""
        if not sites:
            return 0.0
        
        complete_count = 0
        for site in sites:
            value = getattr(site, field, None)
            if value is not None and value != '' and value != 'Unknown':
                complete_count += 1
        
        return complete_count / len(sites)
    
    def validate_transformed_data(self, sites: List[StandardizedSite]) -> Dict[str, Any]:
        """Validate the transformed dataset"""
        validation_results = {
            'total_sites': len(sites),
            'validation_passed': True,
            'errors': [],
            'warnings': [],
            'quality_checks': {}
        }
        
        # Check for duplicates
        site_ids = [site.id for site in sites]
        duplicates = [id for id in site_ids if site_ids.count(id) > 1]
        if duplicates:
            validation_results['errors'].append(f"Duplicate site IDs found: {set(duplicates)}")
            validation_results['validation_passed'] = False
        
        # Check required fields
        required_fields = ['id', 'name', 'country']
        for field in required_fields:
            missing_count = len([s for s in sites if not getattr(s, field, None)])
            if missing_count > 0:
                validation_results['errors'].append(f"Missing {field} in {missing_count} records")
                validation_results['validation_passed'] = False
        
        # Data quality warnings
        low_quality_count = len([s for s in sites if s.data_quality_score < 0.5])
        if low_quality_count > len(sites) * 0.2:  # More than 20% low quality
            validation_results['warnings'].append(f"High number of low-quality records: {low_quality_count}")
        
        # Geographic data completeness
        no_region_count = len([s for s in sites if not s.region])
        if no_region_count > len(sites) * 0.3:  # More than 30% missing region
            validation_results['warnings'].append(f"Many sites missing region data: {no_region_count}")
        
        # Year validation
        invalid_years = len([s for s in sites if s.inscription_year > 0 and (s.inscription_year < 1900 or s.inscription_year > 2030)])
        if invalid_years > 0:
            validation_results['errors'].append(f"Invalid inscription years in {invalid_years} records")
            validation_results['validation_passed'] = False
        
        # Quality checks summary
        validation_results['quality_checks'] = {
            'duplicate_ids': len(set(duplicates)),
            'missing_required_fields': sum([len([s for s in sites if not getattr(s, field, None)]) for field in required_fields]),
            'low_quality_sites': low_quality_count,
            'missing_region_data': no_region_count,
            'invalid_years': invalid_years,
            'average_quality_score': sum([s.data_quality_score for s in sites]) / len(sites) if sites else 0
        }
        
        return validation_results

# Example usage and integration
def main():
    """Example usage of the transformation pipeline"""
    
    # Initialize pipeline
    pipeline = HeritageDataTransformationPipeline()
    
    # Sample raw data (this would come from your scraper)
    sample_raw_data = [
        {
            'name': 'Angkor Archaeological Park',
            'country': 'Cambodia',
            'location': 'Siem Reap Province',
            'year': '1992',
            'criteria': 'Cultural criteria (i)(ii)(iii)(iv)'
        },
        {
            'name': 'Great Wall of China',
            'country': 'China',
            'location': 'Northern China',
            'year': '1987',
            'criteria': 'Cultural criteria (i)(ii)(iii)(iv)(vi) and Natural criteria (viii)'
        },
        {
            'name': 'Yellowstone National Park',
            'country': 'United States',
            'location': 'Wyoming, Montana, Idaho',
            'year': '1978',
            'criteria': 'Natural criteria (vii)(viii)(ix)(x)'
        }
    ]
    
    try:
        # Run transformation pipeline
        print("Starting data transformation pipeline...")
        standardized_sites = pipeline.transform_pipeline(sample_raw_data)
        
        # Validate results
        print("Validating transformed data...")
        validation_results = pipeline.validate_transformed_data(standardized_sites)
        print(f"Validation passed: {validation_results['validation_passed']}")
        
        if validation_results['errors']:
            print("Errors found:")
            for error in validation_results['errors']:
                print(f"  - {error}")
        
        if validation_results['warnings']:
            print("Warnings:")
            for warning in validation_results['warnings']:
                print(f"  - {warning}")
        
        # Export data
        print("Exporting transformed data...")
        export_files = pipeline.export_to_formats(standardized_sites)
        print("Export completed:")
        for file_type, filename in export_files.items():
            print(f"  {file_type}: {filename}")
        
        # Create data catalog
        print("Creating data catalog...")
        catalog = pipeline.create_data_catalog(standardized_sites)
        catalog_filename = f"data_catalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(catalog_filename, 'w', encoding='utf-8') as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False, default=str)
        print(f"Data catalog created: {catalog_filename}")
        
        # Print summary
        print(f"\nTransformation Summary:")
        print(f"  Total sites processed: {len(standardized_sites)}")
        print(f"  Average quality score: {sum([s.data_quality_score for s in standardized_sites]) / len(standardized_sites):.2f}")
        print(f"  Countries represented: {len(set([s.country for s in standardized_sites]))}")
        print(f"  Continents represented: {len(set([s.continent for s in standardized_sites if s.continent]))}")
        
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        logging.error(f"Pipeline execution failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()