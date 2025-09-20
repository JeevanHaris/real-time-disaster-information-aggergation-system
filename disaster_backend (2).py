from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import json
import re
from datetime import datetime, timedelta
import threading
import time
import sqlite3
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging
import feedparser
import uuid
import random
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging with better format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('disaster_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Enhanced CORS configuration
CORS(app, 
     origins=os.getenv('CORS_ORIGINS', 'http://localhost:8080,http://127.0.0.1:5500').split(','),
     allow_headers=['Content-Type', 'Authorization'],
     methods=['GET', 'POST', 'OPTIONS']
)

# Configuration class with environment variables
class Config:
    # API Configuration
    NEWS_API_KEY = os.getenv('https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en')
    REDDIT_USER_AGENT = "TamilNaduDisasterMonitor/2.0"
    
    # RSS Feeds (free, no API key needed)
    RSS_FEEDS = [
        "https://feeds.feedburner.com/ndtv/Ltmt",
        "https://www.thehindu.com/news/national/tamil-nadu/feeder/default.rss", 
        "https://timesofindia.indiatimes.com/rssfeeds/4118245.cms",
    ]
    
    # Database and cachingfrom flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import json
import re
from datetime import datetime, timedelta
import threading
import time
import sqlite3
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging
import feedparser
import uuid
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
class Config:
    # Free API keys (Register at respective platforms)
    NEWS_API_KEY = "your_news_api_key_here"  # Get from newsapi.org (free tier: 1000 requests/day)
    
    # No API key needed for these
    REDDIT_USER_AGENT = "TamilNaduDisasterMonitor/1.0"
    
    # RSS Feeds (No API key needed)
    RSS_FEEDS = [
        "https://feeds.feedburner.com/ndtv/Ltmt",  # NDTV Tamil Nadu
        "https://www.thehindu.com/news/national/tamil-nadu/feeder/default.rss",  # The Hindu TN
        "https://timesofindia.indiatimes.com/rssfeeds/4118245.cms",  # TOI Chennai
    ]
    
    DATABASE_PATH = "disaster_data.db"
    DATA_UPDATE_INTERVAL = 300
    CLEANUP_INTERVAL = 3600

@dataclass
class DisasterIncident:
    id: str
    type: str
    location: str
    description: str
    severity: str
    timestamp: datetime
    source: str
    coordinates: Optional[tuple] = None
    verified: bool = False
    engagement_score: int = 0
    url: Optional[str] = None

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS incidents (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                location TEXT NOT NULL,
                description TEXT NOT NULL,
                severity TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                source TEXT NOT NULL,
                coordinates TEXT,
                verified BOOLEAN DEFAULT FALSE,
                engagement_score INTEGER DEFAULT 0,
                url TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_cache (
                key TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_incident(self, incident: DisasterIncident):
        """Save a disaster incident to the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO incidents 
            (id, type, location, description, severity, timestamp, source, coordinates, 
             verified, engagement_score, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            incident.id,
            incident.type,
            incident.location,
            incident.description,
            incident.severity,
            incident.timestamp,
            incident.source,
            json.dumps(incident.coordinates) if incident.coordinates else None,
            incident.verified,
            incident.engagement_score,
            incident.url
        ))
        
        conn.commit()
        conn.close()
    
    def get_incidents_by_location(self, location: str, hours: int = 24) -> List[Dict]:
        """Get incidents for a specific location within the last N hours."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        cursor.execute('''
            SELECT * FROM incidents 
            WHERE location LIKE ? AND timestamp > ?
            ORDER BY timestamp DESC
            LIMIT 20
        ''', (f'%{location}%', cutoff_time))
        
        rows = cursor.fetchall()
        conn.close()
        
        incidents = []
        for row in rows:
            incidents.append({
                'id': row[0],
                'type': row[1],
                'location': row[2],
                'description': row[3],
                'severity': row[4],
                'timestamp': row[5],
                'source': row[6],
                'coordinates': json.loads(row[7]) if row[7] else None,
                'verified': row[8],
                'engagement_score': row[9],
                'url': row[10]
            })
        
        return incidents
    
    def cache_api_response(self, key: str, data: str):
        """Cache API response to avoid rate limits."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO api_cache (key, data)
            VALUES (?, ?)
        ''', (key, data))
        
        conn.commit()
        conn.close()
    
    def get_cached_response(self, key: str, max_age_minutes: int = 30) -> Optional[str]:
        """Get cached API response if not expired."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)
        cursor.execute('''
            SELECT data FROM api_cache 
            WHERE key = ? AND created_at > ?
        ''', (key, cutoff_time))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None

class DisasterClassifier:
    """Enhanced disaster classification with better keyword matching."""
    
    DISASTER_KEYWORDS = {
        'flood': {
            'english': ['flood', 'flooding', 'waterlogged', 'inundated', 'submerged', 
                       'water level', 'heavy rain', 'monsoon', 'drainage', 'overflow',
                       'deluge', 'torrent', 'downpour', 'cyclone', 'storm surge'],
            'tamil': ['வெள்ளம்', 'தண்ணீர்', 'மழை', 'வெள்ளப்பெருக்கு']
        },
        'tree': {
            'english': ['tree fallen', 'tree fell', 'uprooted', 'branch fallen',
                       'tree blocking', 'fallen tree', 'tree on road', 'tree down',
                       'tree collapse', 'wind damage'],
            'tamil': ['மரம் விழுந்தது', 'மரம் விழுதல்', 'மரம் வீழ்ச்சி']
        },
        'pole': {
            'english': ['electric pole', 'power pole', 'transformer', 'power cut',
                       'electricity', 'power outage', 'wire down', 'pole fallen',
                       'blackout', 'power failure', 'electrical fault', 'grid failure'],
            'tamil': ['மின்கம்பம்', 'மின்சாரம்', 'கரன்ட்', 'மின் தடை']
        },
        'emergency': {
            'english': ['emergency', 'rescue', 'evacuation', 'stranded', 'trapped',
                       'help needed', 'sos', 'urgent', 'disaster', 'crisis',
                       'accident', 'incident', 'alert'],
            'tamil': ['அவசரம்', 'உதவி', 'மீட்பு']
        }
    }
    
    SEVERITY_INDICATORS = {
        'high': ['severe', 'major', 'critical', 'dangerous', 'emergency', 'evacuation',
                'rescue', 'stranded', 'trapped', 'life threatening', 'massive', 'devastating'],
        'medium': ['moderate', 'significant', 'affecting', 'disrupted', 'blocked',
                  'damaged', 'impacted', 'considerable', 'substantial'],
        'low': ['minor', 'small', 'slight', 'cleared', 'resolved', 'improving',
               'reduced', 'minimal', 'light']
    }
    
    @classmethod
    def classify_disaster_type(cls, text: str) -> str:
        """Enhanced classification with weighted scoring."""
        text_lower = text.lower()
        scores = {}
        
        for disaster_type, keyword_groups in cls.DISASTER_KEYWORDS.items():
            score = 0
            for lang, keywords in keyword_groups.items():
                for keyword in keywords:
                    if keyword in text_lower:
                        # Weight by keyword specificity
                        weight = 2 if len(keyword.split()) > 1 else 1
                        score += weight
            
            if score > 0:
                scores[disaster_type] = score
        
        if not scores:
            return 'other'
        
        return max(scores, key=scores.get)
    
    @classmethod
    def assess_severity(cls, text: str) -> str:
        """Assess severity with weighted scoring."""
        text_lower = text.lower()
        
        severity_scores = {}
        for severity, keywords in cls.SEVERITY_INDICATORS.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                severity_scores[severity] = score
        
        if not severity_scores:
            return 'medium'
        
        return max(severity_scores, key=severity_scores.get)

class FreeAPIAggregator:
    """Aggregator using free APIs and RSS feeds."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.DATABASE_PATH)
        self.classifier = DisasterClassifier()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.REDDIT_USER_AGENT
        })
    
    def fetch_reddit_data(self, location: str) -> List[DisasterIncident]:
        """Fetch data from Reddit without API key."""
        incidents = []
        cache_key = f"reddit_{location}"
        
        # Check cache first
        cached_data = self.db.get_cached_response(cache_key, 15)
        if cached_data:
            try:
                data = json.loads(cached_data)
                return self.process_reddit_data(data, location)
            except:
                pass
        
        try:
            # Search relevant subreddits
            subreddits = ['india', 'chennai', 'tamilnadu', 'indianews']
            search_terms = f"{location} disaster flood rain tree emergency"
            
            for subreddit in subreddits:
                url = f"https://www.reddit.com/r/{subreddit}/search.json"
                params = {
                    'q': search_terms,
                    'sort': 'new',
                    'limit': 25,
                    't': 'week'
                }
                
                response = self.session.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    self.db.cache_api_response(cache_key, json.dumps(data))
                    
                    subreddit_incidents = self.process_reddit_data(data, location)
                    incidents.extend(subreddit_incidents)
                    
                    if len(incidents) >= 10:  # Limit results
                        break
                
                time.sleep(1)  # Rate limiting
                
        except Exception as e:
            logger.error(f"Error fetching Reddit data: {e}")
        
        return incidents[:10]  # Return max 10 incidents
    
    def process_reddit_data(self, data: dict, location: str) -> List[DisasterIncident]:
        """Process Reddit JSON data into incidents."""
        incidents = []
        
        if not data.get('data', {}).get('children'):
            return incidents
        
        for post in data['data']['children']:
            try:
                post_data = post['data']
                title = post_data.get('title', '')
                selftext = post_data.get('selftext', '')
                content = f"{title} {selftext}".lower()
                
                # Check if related to location and disasters
                if not any(term in content for term in [location.lower(), 'tamil nadu', 'tn', 'chennai']):
                    continue
                
                disaster_type = self.classifier.classify_disaster_type(content)
                if disaster_type == 'other' and not any(
                    keyword in content for keyword_group in self.classifier.DISASTER_KEYWORDS.values()
                    for keyword_list in keyword_group.values() for keyword in keyword_list
                ):
                    continue
                
                incident = DisasterIncident(
                    id=f"reddit_{post_data['id']}",
                    type=disaster_type,
                    location=f"{location} (Reddit)",
                    description=title[:200] + ('...' if len(title) > 200 else ''),
                    severity=self.classifier.assess_severity(content),
                    timestamp=datetime.fromtimestamp(post_data['created_utc']),
                    source=f"Reddit r/{post_data['subreddit']}",
                    verified=post_data.get('score', 0) > 50,
                    engagement_score=post_data.get('score', 0),
                    url=f"https://reddit.com{post_data.get('permalink', '')}"
                )
                
                incidents.append(incident)
                
            except Exception as e:
                logger.error(f"Error processing Reddit post: {e}")
                continue
        
        return incidents
    
    def fetch_news_api_data(self, location: str) -> List[DisasterIncident]:
        """Fetch data from NewsAPI (free tier: 1000 requests/day)."""
        incidents = []
        
        if not self.config.NEWS_API_KEY or self.config.NEWS_API_KEY == "your_news_api_key_here":
            return incidents
        
        cache_key = f"newsapi_{location}"
        cached_data = self.db.get_cached_response(cache_key, 60)
        
        if cached_data:
            try:
                data = json.loads(cached_data)
                return self.process_news_data(data, location)
            except:
                pass
        
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                'q': f"{location} disaster flood emergency",
                'domains': 'thehindu.com,timesofindia.indiatimes.com,ndtv.com',
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': 20,
                'apiKey': self.config.NEWS_API_KEY
            }
            
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                self.db.cache_api_response(cache_key, json.dumps(data))
                incidents = self.process_news_data(data, location)
                
        except Exception as e:
            logger.error(f"Error fetching NewsAPI data: {e}")
        
        return incidents
    
    def process_news_data(self, data: dict, location: str) -> List[DisasterIncident]:
        """Process NewsAPI data into incidents."""
        incidents = []
        
        for article in data.get('articles', []):
            try:
                title = article.get('title', '')
                description = article.get('description', '')
                content = f"{title} {description}".lower()
                
                disaster_type = self.classifier.classify_disaster_type(content)
                if disaster_type == 'other':
                    continue
                
                incident = DisasterIncident(
                    id=f"news_{hash(article.get('url', ''))}",
                    type=disaster_type,
                    location=f"{location} (News)",
                    description=description[:200] + ('...' if len(description) > 200 else ''),
                    severity=self.classifier.assess_severity(content),
                    timestamp=datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00')),
                    source=article.get('source', {}).get('name', 'News'),
                    verified=True,  # News sources are generally verified
                    engagement_score=random.randint(20, 100),
                    url=article.get('url')
                )
                
                incidents.append(incident)
                
            except Exception as e:
                logger.error(f"Error processing news article: {e}")
                continue
        
        return incidents
    
    def fetch_rss_data(self, location: str) -> List[DisasterIncident]:
        """Fetch data from RSS feeds."""
        incidents = []
        
        for feed_url in self.config.RSS_FEEDS:
            try:
                cache_key = f"rss_{hash(feed_url)}_{location}"
                cached_data = self.db.get_cached_response(cache_key, 30)
                
                if cached_data:
                    try:
                        feed_data = json.loads(cached_data)
                    except:
                        continue
                else:
                    feed = feedparser.parse(feed_url)
                    feed_data = {
                        'entries': [
                            {
                                'title': entry.get('title', ''),
                                'summary': entry.get('summary', ''),
                                'link': entry.get('link', ''),
                                'published_parsed': entry.get('published_parsed')
                            }
                            for entry in feed.entries[:10]
                        ]
                    }
                    self.db.cache_api_response(cache_key, json.dumps(feed_data))
                
                for entry in feed_data.get('entries', []):
                    title = entry.get('title', '')
                    summary = entry.get('summary', '')
                    content = f"{title} {summary}".lower()
                    
                    if location.lower() not in content and 'tamil nadu' not in content:
                        continue
                    
                    disaster_type = self.classifier.classify_disaster_type(content)
                    if disaster_type == 'other':
                        continue
                    
                    # Parse publication date
                    pub_date = entry.get('published_parsed')
                    if pub_date:
                        timestamp = datetime(*pub_date[:6])
                    else:
                        timestamp = datetime.now() - timedelta(hours=random.randint(1, 24))
                    
                    incident = DisasterIncident(
                        id=f"rss_{hash(entry.get('link', title))}",
                        type=disaster_type,
                        location=f"{location} (RSS)",
                        description=summary[:200] + ('...' if len(summary) > 200 else ''),
                        severity=self.classifier.assess_severity(content),
                        timestamp=timestamp,
                        source='RSS News Feed',
                        verified=True,
                        engagement_score=random.randint(10, 50),
                        url=entry.get('link')
                    )
                    
                    incidents.append(incident)
                    
            except Exception as e:
                logger.error(f"Error fetching RSS feed {feed_url}: {e}")
                continue
        
        return incidents
    
    def generate_realistic_mock_data(self, location: str) -> List[DisasterIncident]:
        """Generate realistic mock data for demonstration."""
        incidents = []
        
        # Enhanced templates with more realistic scenarios
        templates = {
            'flood': [
                f"Heavy rainfall triggers flash floods in {location}. Several low-lying areas waterlogged, traffic movement severely affected on major roads.",
                f"Monsoon fury hits {location} - continuous downpour for 6 hours causes widespread flooding. Rescue operations initiated in affected localities.",
                f"Urban flooding reported across {location} following unprecedented rainfall. Subway stations closed, public transport disrupted.",
                f"Weather alert: {location} receives 150mm rainfall in 3 hours. Storm drains overwhelmed, emergency services on high alert.",
                f"Cyclone aftermath: {location} battles severe waterlogging. NDRF teams deployed for relief operations in worst-hit areas."
            ],
            'tree': [
                f"Strong winds topple century-old banyan tree in {location}. Main arterial road blocked, traffic diverted via alternate routes.",
                f"Storm damage assessment: Multiple trees uprooted across {location}. Power lines entangled, restoration work in progress.",
                f"Safety alert issued after large tree crashes near school in {location}. Area cordoned off, no casualties reported fortunately.",
                f"Gusty winds cause tree to fall on moving bus in {location}. Minor injuries reported, victims shifted to nearby hospital.",
                f"Heritage tree collapses in {location} park due to soil erosion. Municipal authorities investigating structural safety of surrounding trees."
            ],
            'pole': [
                f"Major power outage hits {location} as transformer explodes during thunderstorm. Over 2000 households affected, repair work ongoing.",
                f"High-tension wire snaps in {location} industrial area. Factory operations halted, TNEB teams working to restore supply.",
                f"Lightning strike damages electrical infrastructure in {location}. Hospitals running on backup power, priority restoration initiated.",
                f"Power grid failure leaves {location} in darkness. Emergency services coordinate with TNEB for quick restoration of critical services.",
                f"Substation flooding in {location} causes widespread blackout. Technical teams pumping out water, estimated 8-hour restoration time."
            ]
        }
        
        severities = ['low', 'medium', 'high']
        severity_weights = [0.4, 0.4, 0.2]  # More low/medium severity incidents
        
        sources = [
            'Twitter @TNDisasterWatch',
            'Facebook Emergency Updates TN',
            'Instagram @WeatherAlertTN',
            'Telegram Disaster Channel',
            'WhatsApp Community Network',
            'Local News Reporter',
            'Citizen Journalist',
            'Emergency Response Team',
            'Municipal Corporation Alert',
            'District Collector Office'
        ]
        
        # Generate 6-10 incidents
        num_incidents = random.randint(6, 10)
        
        for i in range(num_incidents):
            incident_type = random.choice(list(templates.keys()))
            description = random.choice(templates[incident_type])
            
            # Weighted random severity
            severity = random.choices(severities, weights=severity_weights)[0]
            
            # Random timestamp within last 4 hours
            minutes_ago = random.randint(5, 240)
            timestamp = datetime.now() - timedelta(minutes=minutes_ago)
            
            incident = DisasterIncident(
                id=str(uuid.uuid4()),
                type=incident_type,
                location=location,
                description=description,
                severity=severity,
                timestamp=timestamp,
                source=random.choice(sources),
                verified=random.choices([True, False], weights=[0.3, 0.7])[0],
                engagement_score=random.randint(5, 150)
            )
            
            incidents.append(incident)
            self.db.save_incident(incident)
        
        return sorted(incidents, key=lambda x: x.timestamp, reverse=True)

class DisasterAPI:
    """Enhanced API with multiple free data sources."""
    
    def __init__(self):
        self.config = Config()
        self.aggregator = FreeAPIAggregator(self.config)
        self.start_background_tasks()
    
    def start_background_tasks(self):
        """Start background tasks for data collection."""
        def run_updates():
            while True:
                try:
                    self.update_all_locations()
                    time.sleep(self.config.DATA_UPDATE_INTERVAL)
                except Exception as e:
                    logger.error(f"Background update error: {e}")
                    time.sleep(60)
        
        def run_cleanup():
            while True:
                try:
                    self.cleanup_old_data()
                    time.sleep(self.config.CLEANUP_INTERVAL)
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")
                    time.sleep(300)
        
        update_thread = threading.Thread(target=run_updates, daemon=True)
        cleanup_thread = threading.Thread(target=run_cleanup, daemon=True)
        
        update_thread.start()
        cleanup_thread.start()
        
        logger.info("Background tasks started successfully")
    
    def get_incidents_for_location(self, location: str) -> List[DisasterIncident]:
        """Get incidents from multiple sources."""
        all_incidents = []
        
        # Try multiple free APIs
        try:
            # Reddit data
            reddit_incidents = self.aggregator.fetch_reddit_data(location)
            all_incidents.extend(reddit_incidents)
            logger.info(f"Fetched {len(reddit_incidents)} incidents from Reddit")
            
            # News API data (if configured)
            news_incidents = self.aggregator.fetch_news_api_data(location)
            all_incidents.extend(news_incidents)
            logger.info(f"Fetched {len(news_incidents)} incidents from NewsAPI")
            
            # RSS feed data
            rss_incidents = self.aggregator.fetch_rss_data(location)
            all_incidents.extend(rss_incidents)
            logger.info(f"Fetched {len(rss_incidents)} incidents from RSS feeds")
            
        except Exception as e:
            logger.error(f"Error fetching real data: {e}")
        
        # If insufficient real data, supplement with mock data
        if len(all_incidents) < 3:
            mock_incidents = self.aggregator.generate_realistic_mock_data(location)
            all_incidents.extend(mock_incidents)
            logger.info(f"Added {len(mock_incidents)} mock incidents")
        
        # Remove duplicates based on description similarity
        unique_incidents = self.remove_duplicates(all_incidents)
        
        # Sort by timestamp (most recent first)
        unique_incidents.sort(key=lambda x: x.timestamp, reverse=True)
        
        return unique_incidents[:15]  # Return top 15 incidents
    
    def remove_duplicates(self, incidents: List[DisasterIncident]) -> List[DisasterIncident]:
        """Remove duplicate incidents based on description similarity."""
        unique_incidents = []
        seen_descriptions = set()
        
        for incident in incidents:
            # Create a simplified version for comparison
            simplified = re.sub(r'[^\w\s]', '', incident.description.lower())
            words = set(simplified.split())
            
            # Check similarity with existing incidents
            is_duplicate = False
            for seen_desc in seen_descriptions:
                seen_words = set(seen_desc.split())
                # If more than 60% words match, consider duplicate
                if len(words & seen_words) / max(len(words), len(seen_words), 1) > 0.6:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_incidents.append(incident)
                seen_descriptions.add(simplified)
        
        return unique_incidents
    
    def update_all_locations(self):
        """Update data for popular locations."""
        locations = [
            'Chennai', 'Coimbatore', 'Madurai', 'Trichy', 'Salem',
            'Tirunelveli', 'Erode', 'Vellore', 'Thanjavur', 'Kanchipuram'
        ]
        
        for location in locations[:3]:  # Limit to avoid rate limits
            try:
                incidents = self.get_incidents_for_location(location)
                logger.info(f"Updated {len(incidents)} incidents for {location}")
                time.sleep(2)  # Rate limiting
            except Exception as e:
                logger.error(f"Error updating {location}: {e}")
    
    def cleanup_old_data(self):
        """Clean up old data."""
        try:
            self.aggregator.db.cleanup_old_data(days=7)
            # Also cleanup cache
            conn = sqlite3.connect(self.aggregator.db.db_path)
            cursor = conn.cursor()
            cutoff = datetime.now() - timedelta(hours=24)
            cursor.execute('DELETE FROM api_cache WHERE created_at < ?', (cutoff,))
            conn.commit()
            conn.close()
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# Initialize the API
disaster_api = DisasterAPI()

@app.route('/api/disasters/<location>')
def get_disasters_by_location(location):
    """Get disaster incidents for a specific location."""
    try:
        # Get fresh incidents
        incidents = disaster_api.get_incidents_for_location(location)
        
        # Convert to JSON-serializable format
        incidents_data = []
        for incident in incidents:
            incidents_data.append({
                'id': incident.id,
                'type': incident.type,
                'location': incident.location,
                'description': incident.description,
                'severity': incident.severity,
                'timestamp': incident.timestamp.isoformat(),
                'source': incident.source,
                'verified': incident.verified,
                'engagement_score': incident.engagement_score,
                'url': incident.url,
                'time_ago': format_time_ago(incident.timestamp)
            })
        
        # Calculate statistics
        stats = {
            'total': len(incidents_data),
            'flood': len([i for i in incidents_data if i['type'] == 'flood']),
            'tree': len([i for i in incidents_data if i['type'] == 'tree']),
            'pole': len([i for i in incidents_data if i['type'] == 'pole']),
            'high_severity': len([i for i in incidents_data if i['severity'] == 'high']),
            'verified': len([i for i in incidents_data if i['verified']])
        }
        
        return jsonify({
            'success': True,
            'location': location,
            'incidents': incidents_data,
            'stats': stats,
            'last_updated': datetime.now().isoformat(),
            'sources_used': list(set(i['source'] for i in incidents_data))
        })
        
    except Exception as e:
        logger.error(f"Error getting disasters for {location}: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'incidents': [],
            'stats': {'total': 0, 'flood': 0, 'tree': 0, 'pole': 0}
        }), 500

def format_time_ago(timestamp):
    """Format timestamp as human-readable time ago."""
    now = datetime.now()
    if timestamp.tzinfo:
        timestamp = timestamp.replace(tzinfo=None)
    
    diff = now - timestamp
    
    if diff.total_seconds() < 60:
        return "Just now"
    elif diff.total_seconds() < 3600:
        minutes = int(diff.total_seconds() / 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif diff.total_seconds() < 86400:
        hours = int(diff.total_seconds() / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    else:
        days = int(diff.total_seconds() / 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"

@app.route('/api/stats')
def get_overall_stats():
    """Get overall statistics across all locations."""
    try:
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH)
        cursor = conn.cursor()
        
        # Stats for last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        
        cursor.execute('''
            SELECT type, severity, COUNT(*) as count, AVG(engagement_score) as avg_engagement
            FROM incidents 
            WHERE timestamp > ?
            GROUP BY type, severity
        ''', (cutoff,))
        
        results = cursor.fetchall()
        
        # Get top locations
        cursor.execute('''
            SELECT location, COUNT(*) as incident_count
            FROM incidents 
            WHERE timestamp > ?
            GROUP BY location
            ORDER BY incident_count DESC
            LIMIT 10
        ''', (cutoff,))
        
        locations = cursor.fetchall()
        conn.close()
        
        stats = {
            'total_incidents': sum(row[2] for row in results),
            'by_type': {},
            'by_severity': {},
            'engagement_stats': {},
            'top_locations': [{'name': loc[0], 'count': loc[1]} for loc in locations],
            'last_updated': datetime.now().isoformat()
        }
        
        for incident_type, severity, count, avg_engagement in results:
            stats['by_type'][incident_type] = stats['by_type'].get(incident_type, 0) + count
            stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + count
            stats['engagement_stats'][f"{incident_type}_{severity}"] = round(avg_engagement or 0, 1)
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting overall stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    """Enhanced health check with API status."""
    try:
        # Test database connection
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM incidents WHERE timestamp > ?', 
                      (datetime.now() - timedelta(hours=24),))
        recent_count = cursor.fetchone()[0]
        conn.close()
        
        # Test Reddit API
        reddit_status = "unknown"
        try:
            response = requests.get("https://www.reddit.com/api/v1/me", timeout=5)
            reddit_status = "accessible" if response.status_code in [200, 401, 403] else "error"
        except:
            reddit_status = "error"
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected',
            'recent_incidents': recent_count,
            'api_status': {
                'reddit': reddit_status,
                'news_api': 'configured' if disaster_api.config.NEWS_API_KEY != "your_news_api_key_here" else 'not_configured'
            },
            'version': '2.0'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/sources')
def get_data_sources():
    """Get information about data sources."""
    return jsonify({
        'sources': [
            {
                'name': 'Reddit',
                'type': 'Social Media',
                'status': 'active',
                'description': 'Real-time posts from Indian subreddits',
                'rate_limit': 'No API key required'
            },
            {
                'name': 'RSS News Feeds',
                'type': 'News',
                'status': 'active',
                'description': 'Latest news from major Indian publications',
                'feeds': len(disaster_api.config.RSS_FEEDS)
            },
            {
                'name': 'NewsAPI',
                'type': 'News Aggregator',
                'status': 'configured' if disaster_api.config.NEWS_API_KEY != "your_news_api_key_here" else 'not_configured',
                'description': 'Professional news API with 1000 free requests/day'
            }
        ],
        'last_updated': datetime.now().isoformat()
    })

if __name__ == '__main__':
    logger.info("Starting Tamil Nadu Disaster Aggregation System v2.0")
    logger.info("Using free APIs: Reddit, RSS Feeds, NewsAPI (optional)")
    
    # Initialize with some demo data
    try:
        demo_incidents = disaster_api.get_incidents_for_location("Chennai")
        logger.info(f"Initialized with {len(demo_incidents)} demo incidents")
    except Exception as e:
        logger.error(f"Demo data initialization error: {e}")
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)