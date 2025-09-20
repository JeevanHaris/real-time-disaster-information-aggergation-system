from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import json
from datetime import datetime, timedelta
import threading
import time
import sqlite3
import schedule
import os
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging
import hashlib
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET
from contextlib import contextmanager
import pytz
from urllib.parse import quote

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
class Config:
    DATABASE_PATH = "disaster_data.db"
    DATA_UPDATE_INTERVAL = 300  # 5 minutes
    CLEANUP_INTERVAL = 3600     # 1 hour
    RELIEFWEB_ENDPOINT = "https://api.reliefweb.int/v1/reports"
    GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
    GDACS_RSS = "https://www.gdacs.org/rss.aspx?profile=RSS"
    REQUEST_TIMEOUT = 15
    MAX_RETRIES = 3

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

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            with self.lock:
                conn = sqlite3.connect(
                    self.db_path, 
                    timeout=30,
                    isolation_level=None  # Autocommit mode
                )
                conn.row_factory = sqlite3.Row
                yield conn
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def init_database(self):
        """Initialize database with proper error handling"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS incidents (
                        id TEXT PRIMARY KEY,
                        type TEXT NOT NULL,
                        location TEXT NOT NULL,
                        description TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        source TEXT NOT NULL,
                        coordinates TEXT,
                        verified INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON incidents(location)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON incidents(timestamp)')
                logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def save_incident(self, incident: DisasterIncident) -> bool:
        """Save incident with proper error handling"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO incidents 
                    (id, type, location, description, severity, timestamp, source, coordinates, verified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    incident.id,
                    incident.type,
                    incident.location,
                    incident.description[:500],  # Limit description length
                    incident.severity,
                    incident.timestamp.isoformat(),
                    incident.source,
                    json.dumps(incident.coordinates) if incident.coordinates else None,
                    1 if incident.verified else 0
                ))
                return True
        except Exception as e:
            logger.error(f"Failed to save incident {incident.id}: {e}")
            return False

    def get_incidents_by_location(self, location: str, hours: int = 24) -> List[Dict]:
        """Get incidents with improved error handling"""
        incidents = []
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cutoff_time = datetime.now() - timedelta(hours=hours)
                cursor.execute('''
                    SELECT * FROM incidents 
                    WHERE location LIKE ? AND timestamp > ?
                    ORDER BY timestamp DESC
                    LIMIT 100
                ''', (f'%{location}%', cutoff_time.isoformat()))
                
                rows = cursor.fetchall()
                for row in rows:
                    try:
                        incidents.append({
                            'id': row['id'],
                            'type': row['type'],
                            'location': row['location'],
                            'description': row['description'],
                            'severity': row['severity'],
                            'timestamp': row['timestamp'],
                            'source': row['source'],
                            'coordinates': json.loads(row['coordinates']) if row['coordinates'] else None,
                            'verified': bool(row['verified'])
                        })
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Skipping malformed incident: {e}")
                        continue
        except Exception as e:
            logger.error(f"Failed to fetch incidents for {location}: {e}")
        
        return incidents

    def cleanup_old_data(self, days: int = 7) -> bool:
        """Cleanup old data with proper error handling"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cutoff_time = datetime.now() - timedelta(days=days)
                cursor.execute('DELETE FROM incidents WHERE timestamp < ?', (cutoff_time.isoformat(),))
                deleted_rows = cursor.rowcount
                logger.info(f"Cleaned up {deleted_rows} old records")
                return True
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return False

class DisasterClassifier:
    DISASTER_KEYWORDS = {
        'flood': ['flood', 'flooding', 'waterlogged', 'inundated', 'submerged', 'deluge'],
        'tree': ['tree fallen', 'tree fell', 'uprooted', 'fallen tree', 'tree blocking'],
        'pole': ['electric pole', 'power pole', 'power cut', 'electricity', 'outage', 'blackout'],
        'emergency': ['emergency', 'rescue', 'evacuation', 'sos', 'disaster']
    }
    
    SEVERITY_KEYWORDS = {
        'high': ['severe', 'critical', 'dangerous', 'life threatening', 'catastrophic', 'extreme'],
        'medium': ['moderate', 'significant', 'blocked', 'damaged', 'affected'],
        'low': ['minor', 'small', 'cleared', 'resolved', 'light']
    }

    @classmethod
    def classify_disaster_type(cls, text: str) -> str:
        """Classify disaster type with improved scoring"""
        if not text:
            return 'other'
            
        text_lower = text.lower()
        scores = {}
        
        for dtype, keywords in cls.DISASTER_KEYWORDS.items():
            score = sum(2 if kw in text_lower else 0 for kw in keywords)
            if score > 0:
                scores[dtype] = score
                
        return max(scores, key=scores.get) if scores else 'other'

    @classmethod
    def assess_severity(cls, text: str) -> str:
        """Assess severity with improved logic"""
        if not text:
            return 'medium'
            
        text_lower = text.lower()
        for severity, keywords in cls.SEVERITY_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                return severity
        return 'medium'

class SocialMediaAggregator:
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.DATABASE_PATH)
        self.classifier = DisasterClassifier()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; DisasterMonitor/1.0)'
        })

    def _safe_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Make safe HTTP requests with retries"""
        for attempt in range(self.config.MAX_RETRIES):
            try:
                response = self.session.get(
                    url, 
                    timeout=self.config.REQUEST_TIMEOUT,
                    **kwargs
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed for {url}: {e}")
                if attempt == self.config.MAX_RETRIES - 1:
                    logger.error(f"All attempts failed for {url}")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        return None

    def _safe_parse_timestamp(self, text: str) -> datetime:
        """Safely parse timestamp with timezone awareness"""
        if not text:
            return datetime.now(pytz.UTC)
            
        try:
            # Try ISO format first
            return datetime.fromisoformat(text.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Try email date format
                return parsedate_to_datetime(text)
            except Exception:
                logger.warning(f"Could not parse timestamp: {text}")
                return datetime.now(pytz.UTC)

    def _safe_parse_xml(self, content: bytes) -> Optional[ET.Element]:
        """Safely parse XML content"""
        try:
            # Basic XML validation
            if not content or len(content) > 10 * 1024 * 1024:  # 10MB limit
                return None
            return ET.fromstring(content)
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected XML error: {e}")
            return None

    def fetch_reliefweb_reports(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        """Fetch ReliefWeb reports with improved error handling"""
        incidents: List[DisasterIncident] = []
        try:
            params = {
                "appname": "tn-disaster-monitor",
                "profile": "full",
                "limit": min(limit, 50),  # Cap the limit
                "sort[]": "date:desc",
                "filter": json.dumps({
                    "operator": "AND",
                    "conditions": [
                        {"field": "country.name", "value": "India"},
                        {"field": "title", "value": location}
                    ]
                })
            }
            
            response = self._safe_request(self.config.RELIEFWEB_ENDPOINT, params=params)
            if not response:
                return incidents
                
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error("Invalid JSON response from ReliefWeb")
                return incidents
                
            for item in data.get('data', [])[:limit]:
                try:
                    fields = item.get('fields', {})
                    title = fields.get('title', 'ReliefWeb Report')
                    summary = fields.get('body', title)
                    date_published = fields.get('date', {}).get('posted')
                    
                    timestamp = self._safe_parse_timestamp(date_published)
                    text = f"{title} - {location}. {summary}"
                    dtype = self.classifier.classify_disaster_type(text)
                    severity = self.classifier.assess_severity(text)
                    
                    incident = DisasterIncident(
                        id=f"reliefweb_{item.get('id', 'unknown')}",
                        type=dtype,
                        location=f"{location} (ReliefWeb)",
                        description=title[:200] + ('...' if len(title) > 200 else ''),
                        severity=severity,
                        timestamp=timestamp,
                        source="ReliefWeb",
                        verified=True
                    )
                    
                    incidents.append(incident)
                    self.db.save_incident(incident)
                    
                except (KeyError, TypeError) as e:
                    logger.warning(f"Skipping malformed ReliefWeb item: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"ReliefWeb fetch error: {e}")
            
        return incidents

    def fetch_google_news(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        """Fetch Google News with improved error handling"""
        incidents: List[DisasterIncident] = []
        try:
            keywords = [
                "flood OR flooding OR waterlogging",
                "tree fallen OR uprooted",
                "electric pole",
                "water shortage",
                "heavy rainfall",
                "power cut OR electricity OR outage",
                "storm OR cyclone"
            ]
            
            query = quote(f"({ ' OR '.join(keywords) }) {location} when:1d")
            url = self.config.GOOGLE_NEWS_RSS.format(query=query)
            
            response = self._safe_request(url)
            if not response:
                return incidents
                
            root = self._safe_parse_xml(response.content)
            if not root:
                return incidents
                
            channel = root.find('channel')
            if channel is None:
                return incidents
                
            items = channel.findall('item')[:limit]
            for item in items:
                try:
                    title = item.findtext('title', 'News Item')
                    desc = item.findtext('description', title)
                    pubdate = item.findtext('pubDate')
                    
                    timestamp = self._safe_parse_timestamp(pubdate)
                    text = f"{title}. {desc}"
                    dtype = self.classifier.classify_disaster_type(text)
                    severity = self.classifier.assess_severity(text)
                    
                    if dtype != 'other':  # Only save relevant incidents
                        incident = DisasterIncident(
                            id="gnews_" + hashlib.md5(title.encode('utf-8')).hexdigest(),
                            type=dtype,
                            location=f"{location} (News)",
                            description=title[:200] + ('...' if len(title) > 200 else ''),
                            severity=severity,
                            timestamp=timestamp,
                            source="Google News",
                            verified=False
                        )
                        
                        incidents.append(incident)
                        self.db.save_incident(incident)
                        
                except Exception as e:
                    logger.warning(f"Skipping malformed news item: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Google News fetch error: {e}")
            
        return incidents

    def fetch_gdacs_alerts(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        """Fetch GDACS alerts with improved error handling"""
        incidents: List[DisasterIncident] = []
        try:
            response = self._safe_request(self.config.GDACS_RSS)
            if not response:
                return incidents
                
            root = self._safe_parse_xml(response.content)
            if not root:
                return incidents
                
            channel = root.find('channel')
            if channel is None:
                return incidents
                
            loc_lower = location.lower()
            processed = 0
            
            for item in channel.findall('item'):
                if processed >= limit:
                    break
                    
                try:
                    title = item.findtext('title', '').strip()
                    desc = item.findtext('description', '').strip()
                    combined = f"{title}. {desc}".lower()
                    
                    if 'india' not in combined and loc_lower not in combined:
                        continue
                        
                    pubdate = item.findtext('pubDate')
                    timestamp = self._safe_parse_timestamp(pubdate)
                    dtype = self.classifier.classify_disaster_type(combined)
                    severity = self.classifier.assess_severity(combined)
                    
                    if dtype != 'other':
                        incident = DisasterIncident(
                            id="gdacs_" + hashlib.md5((title + desc).encode('utf-8')).hexdigest(),
                            type=dtype,
                            location="India (GDACS)",
                            description=(title[:200] if title else desc[:200]) + ('...' if len(title + desc) > 200 else ''),
                            severity=severity,
                            timestamp=timestamp,
                            source="GDACS",
                            verified=True
                        )
                        
                        incidents.append(incident)
                        self.db.save_incident(incident)
                        processed += 1
                        
                except Exception as e:
                    logger.warning(f"Skipping malformed GDACS item: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"GDACS fetch error: {e}")
            
        return incidents

class DisasterAPI:
    def __init__(self):
        self.config = Config()
        self.aggregator = SocialMediaAggregator(self.config)
        self.is_running = False
        self.start_background_tasks()

    def start_background_tasks(self):
        """Start background tasks with proper error handling"""
        if self.is_running:
            return
            
        def run_scheduler():
            self.is_running = True
            schedule.every(5).minutes.do(self.update_all_locations)
            schedule.every(1).hour.do(self.cleanup_old_data)
            
            while self.is_running:
                try:
                    schedule.run_pending()
                    time.sleep(60)
                except Exception as e:
                    logger.error(f"Scheduler error: {e}")
                    time.sleep(60)
                    
        threading.Thread(target=run_scheduler, daemon=True).start()
        logger.info("Background tasks started")

    def stop_background_tasks(self):
        """Stop background tasks gracefully"""
        self.is_running = False
        schedule.clear()
        logger.info("Background tasks stopped")

    def update_all_locations(self):
        """Update all locations with improved error handling"""
        locations = ['Chennai', 'Coimbatore', 'Madurai', 'Trichy', 'Salem', 'Tirunelveli']
        
        for location in locations:
            try:
                incidents = []
                incidents.extend(self.aggregator.fetch_reliefweb_reports(location, 10))
                incidents.extend(self.aggregator.fetch_google_news(location, 15))
                incidents.extend(self.aggregator.fetch_gdacs_alerts(location, 10))
                logger.info(f"Updated {len(incidents)} incidents for {location}")
            except Exception as e:
                logger.error(f"Error updating {location}: {e}")

    def cleanup_old_data(self):
        """Cleanup old data with proper error handling"""
        try:
            success = self.aggregator.db.cleanup_old_data(days=7)
            if success:
                logger.info("Old data cleanup completed")
            else:
                logger.warning("Old data cleanup failed")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# Initialize API
try:
    disaster_api = DisasterAPI()
except Exception as e:
    logger.critical(f"Failed to initialize DisasterAPI: {e}")
    raise

@app.route('/api/disasters/<location>')
def get_disasters_by_location(location):
    """Get disasters by location with comprehensive error handling"""
    try:
        # Input validation
        if not location or len(location.strip()) == 0:
            return jsonify({
                'success': False, 
                'error': 'Location parameter is required',
                'incidents': [], 
                'stats': {}
            }), 400
        
        location = location.strip()[:100]  # Limit location length
        
        incidents = disaster_api.aggregator.db.get_incidents_by_location(location, hours=24)
        
        # If no recent incidents, fetch new ones
        if not incidents:
            try:
                new_incidents: List[DisasterIncident] = []
                new_incidents.extend(disaster_api.aggregator.fetch_reliefweb_reports(location, 5))
                new_incidents.extend(disaster_api.aggregator.fetch_google_news(location, 10))
                new_incidents.extend(disaster_api.aggregator.fetch_gdacs_alerts(location, 5))
                
                incidents = [
                    {
                        'id': inc.id,
                        'type': inc.type,
                        'location': inc.location,
                        'description': inc.description,
                        'severity': inc.severity,
                        'timestamp': inc.timestamp.isoformat(),
                        'source': inc.source,
                        'verified': inc.verified
                    }
                    for inc in new_incidents
                ]
            except Exception as e:
                logger.error(f"Error fetching new incidents: {e}")
        
        # Add time_ago field safely
        for incident in incidents:
            try:
                timestamp = datetime.fromisoformat(str(incident['timestamp']).replace('Z', '+00:00'))
                if timestamp.tzinfo:
                    timestamp = timestamp.replace(tzinfo=None)
                    
                diff = datetime.now() - timestamp
                total_seconds = diff.total_seconds()
                
                if total_seconds < 3600:
                    incident['time_ago'] = f"{max(1, int(total_seconds/60))} minutes ago"
                elif total_seconds < 86400:
                    incident['time_ago'] = f"{int(total_seconds/3600)} hours ago"
                else:
                    incident['time_ago'] = f"{int(total_seconds/86400)} days ago"
            except Exception as e:
                logger.warning(f"Error calculating time_ago: {e}")
                incident['time_ago'] = "Unknown time"
        
        # Calculate stats safely
        stats = {
            'total': len(incidents),
            'flood': len([i for i in incidents if i.get('type') == 'flood']),
            'tree': len([i for i in incidents if i.get('type') == 'tree']),
            'pole': len([i for i in incidents if i.get('type') == 'pole']),
            'emergency': len([i for i in incidents if i.get('type') == 'emergency']),
            'high_severity': len([i for i in incidents if i.get('severity') == 'high']),
            'verified': len([i for i in incidents if i.get('verified')])
        }
        
        return jsonify({
            'success': True,
            'location': location,
            'incidents': incidents[:20],  # Limit results
            'stats': stats,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Get disasters error for location {location}: {e}")
        return jsonify({
            'success': False, 
            'error': 'Internal server error',
            'incidents': [], 
            'stats': {},
            'last_updated': datetime.now().isoformat()
        }), 500

@app.route('/api/health')
def health():
    """Health check endpoint with detailed status"""
    try:
        db_status = 'connected' if os.path.exists(disaster_api.config.DATABASE_PATH) else 'disconnected'
        
        # Test database connection
        try:
            with disaster_api.aggregator.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM incidents')
                incident_count = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Health check database error: {e}")
            db_status = 'error'
            incident_count = 0
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': db_status,
            'incident_count': incident_count,
            'background_tasks': disaster_api.is_running
        })
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500

@app.route('/api/stats')
def get_global_stats():
    """Get global statistics"""
    try:
        with disaster_api.aggregator.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get recent stats
            cutoff_time = datetime.now() - timedelta(hours=24)
            cursor.execute('''
                SELECT type, severity, COUNT(*) as count
                FROM incidents 
                WHERE timestamp > ?
                GROUP BY type, severity
            ''', (cutoff_time.isoformat(),))
            
            results = cursor.fetchall()
            stats = {
                'total_24h': sum(row['count'] for row in results),
                'by_type': {},
                'by_severity': {}
            }
            
            for row in results:
                stats['by_type'][row['type']] = stats['by_type'].get(row['type'], 0) + row['count']
                stats['by_severity'][row['severity']] = stats['by_severity'].get(row['severity'], 0) + row['count']
            
            return jsonify({
                'success': True,
                'stats': stats,
                'last_updated': datetime.now().isoformat()
            })
            
    except Exception as e:
        logger.error(f"Global stats error: {e}")
        return jsonify({'success': False, 'error': 'Failed to fetch stats'}), 500

# Graceful shutdown
@app.teardown_appcontext
def close_db(error):
    """Clean up database connections"""
    pass

if __name__ == '__main__':
    try:
        logger.info("Starting Disaster Aggregation System")
        # Initial data fetch
        disaster_api.update_all_locations()
        
        # Start the Flask app
        app.run(
            debug=False,  # Don't use debug in production
            host='0.0.0.0', 
            port=int(os.environ.get('PORT', 5000)),
            threaded=True
        )
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        disaster_api.stop_background_tasks()
    except Exception as e:
        logger.critical(f"Failed to start application: {e}")
        raise