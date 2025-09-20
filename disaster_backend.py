from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import json
import re
from datetime import datetime, timedelta
import threading
import time
import sqlite3
import schedule
import os
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
class Config:
    # Database
    DATABASE_PATH = "disaster_data.db"
    
    # Update intervals
    DATA_UPDATE_INTERVAL = 300  # 5 minutes
    CLEANUP_INTERVAL = 3600     # 1 hour
    
    # External sources
    RELIEFWEB_ENDPOINT = "https://api.reliefweb.int/v1/reports"
    GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
    GDACS_RSS = "https://www.gdacs.org/rss.aspx?profile=RSS"

@dataclass
class DisasterIncident:
    id: str
    type: str  # flood, tree, pole, other
    location: str
    description: str
    severity: str  # low, medium, high
    timestamp: datetime
    source: str
    coordinates: Optional[tuple] = None
    verified: bool = False

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
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                name TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
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
            (id, type, location, description, severity, timestamp, source, coordinates, verified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            incident.id,
            incident.type,
            incident.location,
            incident.description,
            incident.severity,
            incident.timestamp,
            incident.source,
            json.dumps(incident.coordinates) if incident.coordinates else None,
            incident.verified
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
                'verified': row[8]
            })
        
        return incidents
    
    def cleanup_old_data(self, days: int = 7):
        """Remove incidents older than specified days."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(days=days)
        cursor.execute('DELETE FROM incidents WHERE timestamp < ?', (cutoff_time,))
        
        conn.commit()
        conn.close()

class DisasterClassifier:
    """Classify social media posts into disaster categories."""
    
    DISASTER_KEYWORDS = {
        'flood': [
            'flood', 'flooding', 'waterlogged', 'inundated', 'submerged',
            'water level', 'heavy rain', 'monsoon', 'drainage', 'overflow',
            'வெள்ளம்', 'தண்ணீர்', 'மழை'  # Tamil keywords
        ],
        'tree': [
            'tree fallen', 'tree fell', 'uprooted', 'branch fallen',
            'tree blocking', 'fallen tree', 'tree on road',
            'மரம் விழுந்தது', 'மரம் விழுதல்'  # Tamil keywords
        ],
        'pole': [
            'electric pole', 'power pole', 'transformer', 'power cut',
            'electricity', 'power outage', 'wire down', 'pole fallen',
            'மின்கம்பம்', 'மின்சாரம்', 'கரன்ट்'  # Tamil keywords
        ],
        'emergency': [
            'emergency', 'rescue', 'evacuation', 'stranded', 'trapped',
            'help needed', 'sos', 'urgent', 'disaster', 'crisis',
            'அவசரம்', 'உதவி'  # Tamil keywords
        ]
    }
    
    SEVERITY_KEYWORDS = {
        'high': [
            'severe', 'major', 'critical', 'dangerous', 'emergency',
            'evacuation', 'rescue', 'stranded', 'trapped', 'life threatening'
        ],
        'medium': [
            'moderate', 'significant', 'affecting', 'disrupted',
            'blocked', 'damaged', 'impacted'
        ],
        'low': [
            'minor', 'small', 'slight', 'cleared', 'resolved',
            'improving', 'reduced'
        ]
    }
    
    @classmethod
    def classify_disaster_type(cls, text: str) -> str:
        """Classify the type of disaster from text."""
        text_lower = text.lower()
        
        scores = {}
        for disaster_type, keywords in cls.DISASTER_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                scores[disaster_type] = score
        
        if not scores:
            return 'other'
        
        return max(scores, key=scores.get)
    
    @classmethod
    def assess_severity(cls, text: str) -> str:
        """Assess the severity of the incident from text."""
        text_lower = text.lower()
        
        for severity, keywords in cls.SEVERITY_KEYWORDS.items():
            if any(keyword in text_lower for keyword in keywords):
                return severity
        
        return 'medium'  # default severity

class SocialMediaAggregator:
    """Aggregate disaster data from social media platforms."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.DATABASE_PATH)
        self.classifier = DisasterClassifier()
        
    def _safe_parse_timestamp(self, text: str) -> datetime:
        try:
            return datetime.fromisoformat(text.replace('Z', '+00:00'))
        except Exception:
            return datetime.now()
    
    def fetch_reliefweb_reports(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        """Fetch disaster-related reports from ReliefWeb (free, no key)."""
        incidents: List[DisasterIncident] = []
        try:
            params = {
                "appname": "tn-disaster-monitor",
                "profile": "full",
                "limit": limit,
                "sort[]": "date:desc",
                "filter": json.dumps({
                    "operator": "AND",
                    "conditions": [
                        {"field": "country.name", "value": "India"},
                        {"field": "primary_country.name", "value": "India"},
                        {"field": "title", "value": location}
                    ]
                })
            }
            resp = requests.get(self.config.RELIEFWEB_ENDPOINT, params=params, timeout=15)
            if resp.status_code != 200:
                return incidents
            data = resp.json()
            for item in data.get('data', [])[:limit]:
                fields = item.get('fields', {})
                title = fields.get('title') or 'ReliefWeb Report'
                summary = fields.get('body-html') or fields.get('body') or title
                date_published = fields.get('date', {}).get('posted') or fields.get('date', {}).get('created')
                timestamp = self._safe_parse_timestamp(date_published) if date_published else datetime.now()
                text = f"{title} - {location}. {summary if isinstance(summary, str) else ''}"
                disaster_type = self.classifier.classify_disaster_type(text)
                severity = self.classifier.assess_severity(text)
                incident = DisasterIncident(
                    id=f"reliefweb_{item.get('id')}",
                    type=disaster_type,
                    location=f"{location} (ReliefWeb)",
                    description=(title[:180] + "...") if len(title) > 180 else title,
                    severity=severity,
                    timestamp=timestamp,
                    source="ReliefWeb",
                    verified=True
                )
                incidents.append(incident)
                self.db.save_incident(incident)
        except Exception as e:
            logger.error(f"Error fetching ReliefWeb reports: {e}")
        return incidents
    
    def fetch_google_news(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        """Fetch Google News RSS for disaster keywords around the location (free)."""
        import xml.etree.ElementTree as ET
        incidents: List[DisasterIncident] = []
        try:
            keywords = [
                "flood OR flooding OR waterlogging",
                "tree fallen OR uprooted",
                "power cut OR electricity OR outage",
                "storm OR cyclone OR rain"
            ]
            # Combine queries to reduce requests; Google News supports quoted terms
            query = requests.utils.quote(f"({ ' OR '.join(keywords) }) {location} Tamil Nadu when:1d")
            url = self.config.GOOGLE_NEWS_RSS.format(query=query)
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                return incidents
            root = ET.fromstring(resp.content)
            # Namespace handling not necessary for simple parsing of items
            channel = root.find('channel')
            if channel is None:
                return incidents
            for item in channel.findall('item')[:limit]:
                title_el = item.find('title')
                desc_el = item.find('description')
                pubdate_el = item.find('pubDate')
                title = title_el.text if title_el is not None else 'News Item'
                description = desc_el.text if desc_el is not None else title
                timestamp = datetime.now()
                try:
                    # RFC2822 date parsing
                    timestamp = datetime.strptime(pubdate_el.text, '%a, %d %b %Y %H:%M:%S %Z') if pubdate_el is not None else datetime.now()
                except Exception:
                    pass
                text = f"{title}. {description}"
                disaster_type = self.classifier.classify_disaster_type(text)
                severity = self.classifier.assess_severity(text)
                incident = DisasterIncident(
                    id=f"gnews_{hash(title)}",
                    type=disaster_type,
                    location=f"{location} (News)",
                    description=(title[:200] + "...") if len(title) > 200 else title,
                    severity=severity,
                    timestamp=timestamp,
                    source="Google News",
                    verified=False
                )
                # Only include if looks disaster-related
                if disaster_type != 'other':
                    incidents.append(incident)
                    self.db.save_incident(incident)
        except Exception as e:
            logger.error(f"Error fetching Google News RSS: {e}")
        return incidents
    
    def fetch_gdacs_alerts(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        """Fetch GDACS RSS alerts and filter to India/location (free)."""
        import xml.etree.ElementTree as ET
        incidents: List[DisasterIncident] = []
        try:
            resp = requests.get(self.config.GDACS_RSS, timeout=15)
            if resp.status_code != 200:
                return incidents
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            if channel is None:
                return incidents
            loc_lower = location.lower()
            for item in channel.findall('item'):
                title_el = item.find('title')
                desc_el = item.find('description')
                pubdate_el = item.find('pubDate')
                title = (title_el.text or '').strip() if title_el is not None else ''
                description = (desc_el.text or '').strip() if desc_el is not None else ''
                combined = f"{title}. {description}".lower()
                # Filter for India or specific location
                if 'india' not in combined and loc_lower not in combined:
                    continue
                timestamp = datetime.now()
                try:
                    timestamp = datetime.strptime(pubdate_el.text, '%a, %d %b %Y %H:%M:%S %Z') if pubdate_el is not None else datetime.now()
                except Exception:
                    pass
                disaster_type = self.classifier.classify_disaster_type(combined)
                severity = self.classifier.assess_severity(combined)
                incident = DisasterIncident(
                    id=f"gdacs_{hash(title + description)}",
                    type=disaster_type,
                    location=f"{location} (GDACS)",
                    description=(title[:200] + "...") if len(title) > 200 else (title or description[:200]),
                    severity=severity,
                    timestamp=timestamp,
                    source="GDACS",
                    verified=True
                )
                if len(incidents) < limit and disaster_type != 'other':
                    incidents.append(incident)
                    self.db.save_incident(incident)
        except Exception as e:
            logger.error(f"Error fetching GDACS RSS: {e}")
        return incidents

class DisasterAPI:
    """Main API class for the disaster aggregation system."""
    
    def __init__(self):
        self.config = Config()
        self.aggregator = SocialMediaAggregator(self.config)
        self.start_background_tasks()
    
    def start_background_tasks(self):
        """Start background tasks for data collection and cleanup."""
        def run_scheduler():
            schedule.every(5).minutes.do(self.update_all_locations)
            schedule.every(1).hour.do(self.cleanup_old_data)
            
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("Background tasks started")
    
    def update_all_locations(self):
        """Update disaster data for all tracked locations."""
        # Common Tamil Nadu locations
        locations = [
            'Chennai', 'Coimbatore', 'Madurai', 'Trichy', 'Salem',
            'Tirunelveli', 'Erode', 'Vellore', 'Thanjavur', 'Kanchipuram'
        ]
        
        for location in locations:
            try:
                # Aggregate from free real sources
                incidents = []
                incidents.extend(self.aggregator.fetch_reliefweb_reports(location))
                incidents.extend(self.aggregator.fetch_google_news(location))
                incidents.extend(self.aggregator.fetch_gdacs_alerts(location))
                
                logger.info(f"Updated {len(incidents)} incidents for {location}")
                
            except Exception as e:
                logger.error(f"Error updating {location}: {e}")
    
    def cleanup_old_data(self):
        """Clean up old incident data."""
        try:
            self.aggregator.db.cleanup_old_data(days=7)
            logger.info("Old data cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Initialize the disaster API
disaster_api = DisasterAPI()

@app.route('/api/disasters/<location>')
def get_disasters_by_location(location):
    """Get disaster incidents for a specific location."""
    try:
        # Get incidents from database
        incidents = disaster_api.aggregator.db.get_incidents_by_location(location, hours=24)
        
        # If no recent incidents, try to fetch new data from free sources
        if not incidents:
            new_incidents: List[DisasterIncident] = []
            new_incidents.extend(disaster_api.aggregator.fetch_reliefweb_reports(location))
            new_incidents.extend(disaster_api.aggregator.fetch_google_news(location))
            new_incidents.extend(disaster_api.aggregator.fetch_gdacs_alerts(location))
            
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
        
        # Format timestamps for frontend
        for incident in incidents:
            if isinstance(incident['timestamp'], str):
                timestamp = datetime.fromisoformat(incident['timestamp'].replace('Z', '+00:00'))
            else:
                timestamp = datetime.fromisoformat(incident['timestamp'])
            
            # Calculate time difference
            time_diff = datetime.now() - timestamp.replace(tzinfo=None)
            
            if time_diff.total_seconds() < 3600:  # Less than 1 hour
                minutes = int(time_diff.total_seconds() / 60)
                incident['time_ago'] = f"{minutes} minutes ago"
            elif time_diff.total_seconds() < 86400:  # Less than 24 hours
                hours = int(time_diff.total_seconds() / 3600)
                incident['time_ago'] = f"{hours} hours ago"
            else:
                days = int(time_diff.total_seconds() / 86400)
                incident['time_ago'] = f"{days} days ago"
        
        # Calculate statistics
        stats = {
            'total': len(incidents),
            'flood': len([i for i in incidents if i['type'] == 'flood']),
            'tree': len([i for i in incidents if i['type'] == 'tree']),
            'pole': len([i for i in incidents if i['type'] == 'pole']),
            'high_severity': len([i for i in incidents if i['severity'] == 'high']),
            'verified': len([i for i in incidents if i['verified']])
        }
        
        return jsonify({
            'success': True,
            'location': location,
            'incidents': incidents[:20],  # Limit to 20 most recent
            'stats': stats,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting disasters for {location}: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'incidents': [],
            'stats': {'total': 0, 'flood': 0, 'tree': 0, 'pole': 0}
        }), 500

@app.route('/api/stats')
def get_overall_stats():
    """Get overall statistics for all locations."""
    try:
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get stats for last 24 hours
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        cursor.execute('''
            SELECT type, severity, COUNT(*) as count
            FROM incidents 
            WHERE timestamp > ?
            GROUP BY type, severity
        ''', (cutoff_time,))
        
        results = cursor.fetchall()
        conn.close()
        
        stats = {
            'total_incidents': sum(row[2] for row in results),
            'by_type': {},
            'by_severity': {},
            'last_updated': datetime.now().isoformat()
        }
        
        for incident_type, severity, count in results:
            if incident_type not in stats['by_type']:
                stats['by_type'][incident_type] = 0
            stats['by_type'][incident_type] += count
            
            if severity not in stats['by_severity']:
                stats['by_severity'][severity] = 0
            stats['by_severity'][severity] += count
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting overall stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/locations')
def get_tracked_locations():
    """Get list of tracked locations with recent activity."""
    try:
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        cursor.execute('''
            SELECT location, COUNT(*) as incident_count, MAX(timestamp) as last_incident
            FROM incidents 
            WHERE timestamp > ?
            GROUP BY location
            ORDER BY incident_count DESC
        ''', (cutoff_time,))
        
        results = cursor.fetchall()
        conn.close()
        
        locations = []
        for location, count, last_incident in results:
            locations.append({
                'name': location,
                'incident_count': count,
                'last_incident': last_incident
            })
        
        return jsonify({
            'success': True,
            'locations': locations
        })
        
    except Exception as e:
        logger.error(f"Error getting tracked locations: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/verify/<incident_id>', methods=['POST'])
def verify_incident(incident_id):
    """Mark an incident as verified."""
    try:
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE incidents 
            SET verified = TRUE 
            WHERE id = ?
        ''', (incident_id,))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Incident verified'})
        
    except Exception as e:
        logger.error(f"Error verifying incident {incident_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected' if os.path.exists(disaster_api.config.DATABASE_PATH) else 'disconnected'
    })

if __name__ == '__main__':
    # Initialize database and start background tasks
    logger.info("Starting Tamil Nadu Disaster Aggregation System")
    
    # Create some initial data
    disaster_api.update_all_locations()
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
        