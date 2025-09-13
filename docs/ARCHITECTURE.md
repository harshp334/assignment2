# Technical Design Decisions - Heritage Sites Scraper

## Architecture Decisions

### Single-Class Design
**Decision**: All functionality in one `HeritagesSiteScraper` class.
**Rationale**: Simplicity for educational purposes, centralized state management.
**Trade-off**: Easy to understand but harder to scale or unit test.

### Synchronous Processing
**Decision**: Sequential requests without threading.
**Rationale**: Respects Wikipedia servers, simpler error handling.
**Trade-off**: Slower execution but more reliable and respectful.

### In-Memory Storage
**Decision**: Store data in Python lists/dictionaries during runtime.
**Rationale**: No database setup required, fast filtering operations.
**Trade-off**: Memory limitations but zero configuration overhead.

## Data Processing Decisions

### Flexible Table Parsing
**Decision**: Dynamic column detection for various Wikipedia table formats.
**Rationale**: Wikipedia pages have inconsistent structures.
**Trade-off**: Complex parsing logic but handles diverse formats.

### Regex Data Cleaning
**Decision**: Use regex to remove Wikipedia citations and normalize text.
**Rationale**: Efficiently handles Wikipedia markup.
**Trade-off**: Fast processing but brittle to markup changes.

### Graceful Degradation
**Decision**: Fall back to alternative parsing methods when tables fail.
**Rationale**: Maximizes data extraction success.
**Trade-off**: Higher success rates but potential lower-quality fallback data.

## User Experience Decisions

### Command-Line Interface
**Decision**: Interactive prompts rather than web interface.
**Rationale**: Universal compatibility, no GUI dependencies.
**Trade-off**: Simple deployment but less user-friendly.

### Limited Results (3 sites max)
**Decision**: Show maximum 3 heritage sites for selection.
**Rationale**: Reduces decision fatigue, keeps interface manageable.
**Trade-off**: Better user experience but may hide relevant options.

## Performance Decisions

### Caching Strategy
**Decision**: Cache all heritage sites after first load.
**Rationale**: Avoid re-scraping, faster subsequent searches.
**Trade-off**: Memory usage but much better performance.

### Session Reuse
**Decision**: Maintain persistent HTTP session.
**Rationale**: Reuses connections, consistent headers.
**Trade-off**: Slightly more complex but better performance.

## Output Decisions

### JSON Format
**Decision**: Export itineraries as JSON files.
**Rationale**: Structured data, interoperable, human-readable.
**Trade-off**: Programming-friendly but less user-friendly than PDF.

### Dynamic Filenames
**Decision**: Generate filenames from site name and duration.
**Rationale**: Clear identification, prevents overwrites.
**Trade-off**: Self-documenting but potentially long filenames.

## Design Philosophy

The codebase prioritizes simplicity, reliability, and educational value over advanced features. Key principles:
- Minimal dependencies and setup complexity
- Graceful operation when components fail
- Respectful resource usage
- Clear, understandable code structure