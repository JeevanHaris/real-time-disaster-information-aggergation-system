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

# Configure logging
logging.basicConfig(level=logging.INFO)
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
        self.init_database()

    def init_database(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
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
        conn.commit()
        conn.close()

    def save_incident(self, incident: DisasterIncident):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
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
            incident.timestamp.isoformat(),
            incident.source,
            json.dumps(incident.coordinates) if incident.coordinates else None,
            1 if incident.verified else 0
        ))
        conn.commit()
        conn.close()

    def get_incidents_by_location(self, location: str, hours: int = 24) -> List[Dict]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        cursor = conn.cursor()
        cutoff_time = datetime.now() - timedelta(hours=hours)
        cursor.execute('''
            SELECT * FROM incidents 
            WHERE (location LIKE ? OR location LIKE '%Tamil Nadu%' OR location LIKE '%India%') AND timestamp > ?
            ORDER BY timestamp DESC
        ''', (f'%{location}%', cutoff_time.isoformat()))
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
                'verified': bool(row[8])
            })
        return incidents

    def cleanup_old_data(self, days: int = 7):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        cursor = conn.cursor()
        cutoff_time = datetime.now() - timedelta(days=days)
        cursor.execute('DELETE FROM incidents WHERE timestamp < ?', (cutoff_time.isoformat(),))
        conn.commit()
        conn.close()

class DisasterClassifier:
    DISASTER_KEYWORDS = {
        'flood': ['flood', 'flooding', 'waterlogged', 'inundated', 'submerged'],
        'tree': ['tree fallen', 'tree fell', 'uprooted', 'fallen tree'],
        'pole': ['electric pole', 'power pole', 'power cut', 'electricity', 'outage'],
        'emergency': ['emergency', 'rescue', 'evacuation', 'sos']
    }
    SEVERITY_KEYWORDS = {
        'high': ['severe', 'critical', 'dangerous', 'life threatening'],
        'medium': ['moderate', 'significant', 'blocked', 'damaged'],
        'low': ['minor', 'small', 'cleared', 'resolved']
    }

    @classmethod
    def classify_disaster_type(cls, text: str) -> str:
        text_lower = text.lower()
        scores = {dtype: sum(1 for kw in kws if kw in text_lower)
                  for dtype, kws in cls.DISASTER_KEYWORDS.items()}
        scores = {k: v for k, v in scores.items() if v > 0}
        return max(scores, key=scores.get) if scores else 'other'

    @classmethod
    def assess_severity(cls, text: str) -> str:
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

    def _safe_parse_timestamp(self, text: str) -> datetime:
        try:
            return datetime.fromisoformat(text.replace('Z', '+00:00'))
        except Exception:
            return datetime.now()

    def fetch_reliefweb_reports(self, location: str, limit: int = 20) -> List[DisasterIncident]:
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
                title = fields.get('title', 'ReliefWeb Report')
                summary = fields.get('body', title)
                date_published = fields.get('date', {}).get('posted')
                timestamp = self._safe_parse_timestamp(date_published) if date_published else datetime.now()
                text = f"{title} - {location}. {summary}"
                dtype = self.classifier.classify_disaster_type(text)
                severity = self.classifier.assess_severity(text)
                incident = DisasterIncident(
                    id=f"reliefweb_{item.get('id')}",
                    type=dtype,
                    location=f"{location} (ReliefWeb)",
                    description=title[:200],
                    severity=severity,
                    timestamp=timestamp,
                    source="ReliefWeb",
                    verified=True
                )
                incidents.append(incident)
                self.db.save_incident(incident)
        except Exception as e:
            logger.error(f"ReliefWeb error: {e}")
        return incidents

    def fetch_google_news(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        import xml.etree.ElementTree as ET
        incidents: List[DisasterIncident] = []
        try:
            keywords = [
                "flood OR flooding OR waterlogging",
                "tree fallen OR uprooted",
                "electric pole",
                "water clog and water shortage",
                "heavy rainfall",
                "high temperature and windspeed",
                "power cut OR electricity OR outage",
                "storm OR cyclone OR rain"
            ]
            query = requests.utils.quote(f"({ ' OR '.join(keywords) }) {location} when:1d")
            url = self.config.GOOGLE_NEWS_RSS.format(query=query)
            resp = requests.get(url, timeout=15)
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            if channel is None or not channel.findall('item'):
                # fallback to Tamil Nadu
                fallback_query = requests.utils.quote(f"({ ' OR '.join(keywords) }) Tamil Nadu when:1d")
                url = self.config.GOOGLE_NEWS_RSS.format(query=fallback_query)
                resp = requests.get(url, timeout=15)
                root = ET.fromstring(resp.content)
                channel = root.find('channel')
            for item in channel.findall('item')[:limit]:
                title = item.findtext('title', 'News Item')
                desc = item.findtext('description', title)
                pubdate = item.findtext('pubDate')
                try:
                    timestamp = parsedate_to_datetime(pubdate) if pubdate else datetime.now()
                except Exception:
                    timestamp = datetime.now()
                text = f"{title}. {desc}"
                dtype = self.classifier.classify_disaster_type(text)
                severity = self.classifier.assess_severity(text)
                incident = DisasterIncident(
                    id="gnews_" + hashlib.md5(title.encode()).hexdigest(),
                    type=dtype,
                    location=f"{location} (News)",
                    description=title[:200],
                    severity=severity,
                    timestamp=timestamp,
                    source="Google News",
                    verified=False
                )
                if dtype != 'other':
                    incidents.append(incident)
                    self.db.save_incident(incident)
        except Exception as e:
            logger.error(f"Google News error: {e}")
        return incidents

    def fetch_gdacs_alerts(self, location: str, limit: int = 20) -> List[DisasterIncident]:
        import xml.etree.ElementTree as ET
        incidents: List[DisasterIncident] = []
        try:
            resp = requests.get(self.config.GDACS_RSS, timeout=15)
            root = ET.fromstring(resp.content)
            channel = root.find('channel')
            loc_lower = location.lower()
            for item in channel.findall('item'):
                title = item.findtext('title', '').strip()
                desc = item.findtext('description', '').strip()
                combined = f"{title}. {desc}".lower()
                if 'india' not in combined and loc_lower not in combined:
                    continue
                pubdate = item.findtext('pubDate')
                try:
                    timestamp = parsedate_to_datetime(pubdate) if pubdate else datetime.now()
                except Exception:
                    timestamp = datetime.now()
                dtype = self.classifier.classify_disaster_type(combined)
                severity = self.classifier.assess_severity(combined)
                incident = DisasterIncident(
                    id="gdacs_" + hashlib.md5((title+desc).encode()).hexdigest(),
                    type=dtype,
                    location="India (GDACS)",
                    description=title[:200] or desc[:200],
                    severity=severity,
                    timestamp=timestamp,
                    source="GDACS",
                    verified=True
                )
                if len(incidents) < limit and dtype != 'other':
                    incidents.append(incident)
                    self.db.save_incident(incident)
        except Exception as e:
            logger.error(f"GDACS error: {e}")
        return incidents

class DisasterAPI:
    def __init__(self):
        self.config = Config()
        self.aggregator = SocialMediaAggregator(self.config)
        self.start_background_tasks()

    def start_background_tasks(self):
        def run_scheduler():
            schedule.every(5).minutes.do(self.update_all_locations)
            schedule.every(1).hour.do(self.cleanup_old_data)
            while True:
                schedule.run_pending()
                time.sleep(60)
        threading.Thread(target=run_scheduler, daemon=True).start()
        logger.info("Background tasks started")

    def update_all_locations(self):
        locations = ['Chennai', 'Coimbatore', 'Madurai', 'Trichy']
        for location in locations:
            try:
                incidents = []
                incidents.extend(self.aggregator.fetch_reliefweb_reports(location))
                incidents.extend(self.aggregator.fetch_google_news(location))
                incidents.extend(self.aggregator.fetch_gdacs_alerts(location))
                logger.info(f"Updated {len(incidents)} incidents for {location}")
            except Exception as e:
                logger.error(f"Error updating {location}: {e}")

    def cleanup_old_data(self):
        try:
            self.aggregator.db.cleanup_old_data(days=7)
            logger.info("Old data cleaned")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# Initialize API
disaster_api = DisasterAPI()

@app.route('/api/disasters/<location>')
def get_disasters_by_location(location):
    try:
        incidents = disaster_api.aggregator.db.get_incidents_by_location(location, hours=24)
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
        for incident in incidents:
            try:
                timestamp = datetime.fromisoformat(str(incident['timestamp']).replace('Z', '+00:00'))
            except Exception:
                timestamp = datetime.now()
            diff = datetime.now() - timestamp.replace(tzinfo=None)
            if diff.total_seconds() < 3600:
                incident['time_ago'] = f"{int(diff.total_seconds()/60)} minutes ago"
            elif diff.total_seconds() < 86400:
                incident['time_ago'] = f"{int(diff.total_seconds()/3600)} hours ago"
            else:
                incident['time_ago'] = f"{int(diff.total_seconds()/86400)} days ago"
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
            'incidents': incidents[:20],
            'stats': stats,
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Get disasters error: {e}")
        return jsonify({'success': False, 'error': str(e), 'incidents': [], 'stats': {}}), 500

@app.route('/api/health')
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected' if os.path.exists(disaster_api.config.DATABASE_PATH) else 'disconnected'
    })


@app.route('/api/stats')
def get_overall_stats():
    try:
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH, check_same_thread=False, timeout=30)
        cursor = conn.cursor()
        cursor.execute("SELECT type, severity, verified FROM incidents WHERE timestamp > ?", 
                       ((datetime.now() - timedelta(hours=24)).isoformat(),))
        rows = cursor.fetchall()
        conn.close()

        total = len(rows)
        flood = len([r for r in rows if r[0] == 'flood'])
        tree = len([r for r in rows if r[0] == 'tree'])
        pole = len([r for r in rows if r[0] == 'pole'])
        high_severity = len([r for r in rows if r[1] == 'high'])
        verified = len([r for r in rows if r[2]])

        return jsonify({
            'success': True,
            'total_incidents': total,
            'flood': flood,
            'tree': tree,
            'pole': pole,
            'high_severity': high_severity,
            'verified': verified,
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Get overall stats error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    logger.info("Starting Disaster Aggregation System")
    disaster_api.update_all_locations()
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
