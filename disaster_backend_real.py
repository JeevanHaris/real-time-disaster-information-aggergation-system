from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import json
import re
from datetime import datetime, timedelta
import threading
import time
import sqlite3
from textblob import TextBlob
import schedule
import os
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import xml.etree.ElementTree as ET

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
    
    # API URLs (All free, no API keys required)
    USGS_EARTHQUAKE_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson"
    GDACS_RSS_URL = "https://www.gdacs.org/rss.xml"
    RELIEFWEB_API_URL = "https://api.reliefweb.int/v1/disasters"
    
    # Location coordinates for major cities
    CITY_COORDINATES = {
        'Chennai': (13.0827, 80.2707),
        'Mumbai': (19.0760, 72.8777),
        'Delhi': (28.7041, 77.1025),
        'Bangalore': (12.9716, 77.5946),
        'Hyderabad': (17.3850, 78.4867),
        'Ahmedabad': (23.0225, 72.5714),
        'Pune': (18.5204, 73.8567),
        'Surat': (21.1702, 72.8311),
        'Jaipur': (26.9124, 75.7873),
        'Lucknow': (26.8467, 80.9462),
        'Kanpur': (26.4499, 80.3319),
        'Nagpur': (21.1458, 79.0882),
        'Indore': (22.7196, 75.8577),
        'Thane': (19.2183, 72.9781),
        'Bhopal': (23.2599, 77.4126),
        'Visakhapatnam': (17.6868, 83.2185),
        'Coimbatore': (11.0168, 76.9558),
        'Agra': (27.1767, 78.0081),
        'Kochi': (9.9312, 76.2673),
        'Patna': (25.5941, 85.1376),
        'Kolkata': (22.5726, 88.3639),
        # US Cities
        'New York': (40.7128, -74.0060),
        'Los Angeles': (34.0522, -118.2437),
        'Chicago': (41.8781, -87.6298),
        'Houston': (29.7604, -95.3698),
        'Phoenix': (33.4484, -112.0740),
        'Philadelphia': (39.9526, -75.1652),
        'San Antonio': (29.4241, -98.4936),
        'San Diego': (32.7157, -117.1611),
        'Dallas': (32.7767, -96.7970),
        'San Jose': (37.3382, -121.8863),
        'Austin': (30.2672, -97.7431),
        'Jacksonville': (30.3322, -81.6557),
        'San Francisco': (37.7749, -122.4194),
        'Miami': (25.7617, -80.1918),
        'Seattle': (47.6062, -122.3321)
    }

@dataclass
class DisasterIncident:
    id: str
    type: str  # earthquake, flood, wildfire, cyclone, drought, volcano, other
    location: str
    description: str
    severity: str  # low, medium, high
    timestamp: datetime
    source: str
    coordinates: Optional[tuple] = None
    verified: bool = False
    magnitude: Optional[float] = None
    affected_population: Optional[int] = None
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
                magnitude REAL,
                affected_population INTEGER,
                url TEXT,
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
            (id, type, location, description, severity, timestamp, source, coordinates, verified, magnitude, affected_population, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            incident.magnitude,
            incident.affected_population,
            incident.url
        ))
        
        conn.commit()
        conn.close()
    
    def get_incidents_by_location(self, location: str, radius_km: float = 100, hours: int = 168) -> List[Dict]:
        """Get incidents for a specific location within radius and time range."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Get all incidents within time range
        cursor.execute('''
            SELECT * FROM incidents 
            WHERE timestamp > ?
            ORDER BY timestamp DESC
        ''', (cutoff_time,))
        
        rows = cursor.fetchall()
        conn.close()
        
        # Filter by location proximity if coordinates are available
        target_coords = Config.CITY_COORDINATES.get(location.title())
        filtered_incidents = []
        
        for row in rows:
            incident_data = {
                'id': row[0],
                'type': row[1],
                'location': row[2],
                'description': row[3],
                'severity': row[4],
                'timestamp': row[5],
                'source': row[6],
                'coordinates': json.loads(row[7]) if row[7] else None,
                'verified': row[8],
                'magnitude': row[9],
                'affected_population': row[10],
                'url': row[11]
            }
            
            # Include incident if location matches or within radius
            should_include = False
            
            # Check if location name contains the search location
            if location.lower() in incident_data['location'].lower():
                should_include = True
            
            # Check geographic proximity if both coordinates are available
            elif target_coords and incident_data['coordinates']:
                try:
                    distance = geodesic(target_coords, tuple(incident_data['coordinates'])).kilometers
                    if distance <= radius_km:
                        should_include = True
                except:
                    pass
            
            if should_include:
                filtered_incidents.append(incident_data)
        
        return filtered_incidents[:50]  # Limit results
    
    def cleanup_old_data(self, days: int = 30):
        """Remove incidents older than specified days."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = datetime.now() - timedelta(days=days)
        cursor.execute('DELETE FROM incidents WHERE timestamp < ?', (cutoff_time,))
        
        conn.commit()
        conn.close()

class RealDisasterDataAggregator:
    """Aggregate real disaster data from multiple free APIs."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.DATABASE_PATH)
        self.geolocator = Nominatim(user_agent="disaster_monitor")
    
    def fetch_usgs_earthquakes(self) -> List[DisasterIncident]:
        """Fetch earthquake data from USGS."""
        incidents = []
        
        try:
            # Fetch significant earthquakes from the last day
            response = requests.get(self.config.USGS_EARTHQUAKE_URL, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            for feature in data.get('features', []):
                properties = feature.get('properties', {})
                geometry = feature.get('geometry', {})
                
                if geometry.get('coordinates'):
                    coords = geometry['coordinates']  # [longitude, latitude, depth]
                    
                    # Convert timestamp from milliseconds
                    timestamp = datetime.fromtimestamp(properties.get('time', 0) / 1000)
                    
                    # Determine severity based on magnitude
                    magnitude = properties.get('mag', 0)
                    if magnitude >= 7.0:
                        severity = 'high'
                    elif magnitude >= 5.0:
                        severity = 'medium'
                    else:
                        severity = 'low'
                    
                    incident = DisasterIncident(
                        id=f"usgs_{properties.get('id', '')}",
                        type='earthquake',
                        location=properties.get('place', 'Unknown location'),
                        description=f"Magnitude {magnitude} earthquake. {properties.get('title', '')}",
                        severity=severity,
                        timestamp=timestamp,
                        source='USGS Earthquake Hazards Program',
                        coordinates=(coords[1], coords[0]),  # lat, lon
                        verified=True,
                        magnitude=magnitude,
                        url=properties.get('url', '')
                    )
                    
                    incidents.append(incident)
                    self.db.save_incident(incident)
            
            logger.info(f"Fetched {len(incidents)} earthquakes from USGS")
            
        except Exception as e:
            logger.error(f"Error fetching USGS earthquake data: {e}")
        
        return incidents
    
    def fetch_gdacs_disasters(self) -> List[DisasterIncident]:
        """Fetch disaster data from GDACS RSS feed."""
        incidents = []
        
        try:
            response = requests.get(self.config.GDACS_RSS_URL, timeout=10)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            for item in root.findall('.//item'):
                title = item.find('title')
                description = item.find('description')
                link = item.find('link')
                pub_date = item.find('pubDate')
                
                if title is not None and description is not None:
                    title_text = title.text
                    desc_text = description.text or ''
                    
                    # Extract disaster type and severity from title
                    disaster_type = self.extract_disaster_type_from_gdacs(title_text)
                    severity = self.extract_severity_from_gdacs(title_text, desc_text)
                    
                    # Parse publication date
                    timestamp = self.parse_rss_date(pub_date.text if pub_date is not None else '')
                    
                    incident = DisasterIncident(
                        id=f"gdacs_{hash(title_text)}",
                        type=disaster_type,
                        location=self.extract_location_from_gdacs(title_text),
                        description=desc_text[:300] + "..." if len(desc_text) > 300 else desc_text,
                        severity=severity,
                        timestamp=timestamp,
                        source='GDACS - Global Disaster Alert and Coordination System',
                        verified=True,
                        url=link.text if link is not None else ''
                    )
                    
                    incidents.append(incident)
                    self.db.save_incident(incident)
            
            logger.info(f"Fetched {len(incidents)} disasters from GDACS")
            
        except Exception as e:
            logger.error(f"Error fetching GDACS data: {e}")
        
        return incidents
    
    def fetch_reliefweb_disasters(self) -> List[DisasterIncident]:
        """Fetch disaster data from ReliefWeb API."""
        incidents = []
        
        try:
            params = {
                'appname': 'disaster_monitor',
                'limit': 20,
                'sort[]': 'date.created:desc',
                'filter[field]': 'date.created',
                'filter[value][from]': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
            }
            
            response = requests.get(self.config.RELIEFWEB_API_URL, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            for disaster in data.get('data', []):
                fields = disaster.get('fields', {})
                
                # Extract disaster information
                disaster_name = fields.get('name', 'Unknown disaster')
                disaster_type = fields.get('type', [{}])[0].get('name', 'other') if fields.get('type') else 'other'
                
                # Get country information
                countries = fields.get('country', [])
                location = ', '.join([country.get('name', '') for country in countries[:3]])
                
                # Parse date
                date_str = fields.get('date', {}).get('created', '')
                timestamp = self.parse_iso_date(date_str)
                
                # Determine severity (basic heuristic based on description)
                description = fields.get('description', '')
                severity = self.assess_severity_from_text(description)
                
                incident = DisasterIncident(
                    id=f"reliefweb_{disaster.get('id', '')}",
                    type=self.normalize_disaster_type(disaster_type),
                    location=location or 'Unknown location',
                    description=f"{disaster_name}. {description[:200]}..." if len(description) > 200 else f"{disaster_name}. {description}",
                    severity=severity,
                    timestamp=timestamp,
                    source='ReliefWeb',
                    verified=True,
                    url=fields.get('url', '')
                )
                
                incidents.append(incident)
                self.db.save_incident(incident)
            
            logger.info(f"Fetched {len(incidents)} disasters from ReliefWeb")
            
        except Exception as e:
            logger.error(f"Error fetching ReliefWeb data: {e}")
        
        return incidents
    
    def extract_disaster_type_from_gdacs(self, title: str) -> str:
        """Extract disaster type from GDACS title."""
        title_lower = title.lower()
        
        if any(word in title_lower for word in ['earthquake', 'quake']):
            return 'earthquake'
        elif any(word in title_lower for word in ['flood', 'flooding']):
            return 'flood'
        elif any(word in title_lower for word in ['cyclone', 'hurricane', 'typhoon', 'storm']):
            return 'cyclone'
        elif any(word in title_lower for word in ['wildfire', 'fire']):
            return 'wildfire'
        elif any(word in title_lower for word in ['drought']):
            return 'drought'
        elif any(word in title_lower for word in ['volcano', 'volcanic']):
            return 'volcano'
        else:
            return 'other'
    
    def extract_severity_from_gdacs(self, title: str, description: str) -> str:
        """Extract severity from GDACS content."""
        text = f"{title} {description}".lower()
        
        if any(word in text for word in ['red', 'severe', 'major', 'critical', 'extreme']):
            return 'high'
        elif any(word in text for word in ['orange', 'moderate', 'significant']):
            return 'medium'
        else:
            return 'low'
    
    def extract_location_from_gdacs(self, title: str) -> str:
        """Extract location from GDACS title."""
        # Simple extraction - look for text after common patterns
        patterns = [r'in (.+?)(?:\s*-|\s*\(|$)', r'near (.+?)(?:\s*-|\s*\(|$)']
        
        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return title
    
    def normalize_disaster_type(self, disaster_type: str) -> str:
        """Normalize disaster types from different sources."""
        disaster_type_lower = disaster_type.lower()
        
        if any(word in disaster_type_lower for word in ['earthquake', 'seismic']):
            return 'earthquake'
        elif any(word in disaster_type_lower for word in ['flood', 'inundation']):
            return 'flood'
        elif any(word in disaster_type_lower for word in ['cyclone', 'hurricane', 'typhoon', 'tropical storm']):
            return 'cyclone'
        elif any(word in disaster_type_lower for word in ['wildfire', 'forest fire', 'fire']):
            return 'wildfire'
        elif any(word in disaster_type_lower for word in ['drought']):
            return 'drought'
        elif any(word in disaster_type_lower for word in ['volcano', 'volcanic']):
            return 'volcano'
        else:
            return 'other'
    
    def assess_severity_from_text(self, text: str) -> str:
        """Assess severity from descriptive text."""
        text_lower = text.lower()
        
        high_severity_words = ['severe', 'major', 'critical', 'extreme', 'catastrophic', 'devastating', 'massive']
        medium_severity_words = ['moderate', 'significant', 'considerable', 'substantial']
        
        if any(word in text_lower for word in high_severity_words):
            return 'high'
        elif any(word in text_lower for word in medium_severity_words):
            return 'medium'
        else:
            return 'low'
    
    def parse_rss_date(self, date_str: str) -> datetime:
        """Parse RSS date format."""
        try:
            # Try common RSS date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %Z',
                '%a, %d %b %Y %H:%M:%S %z',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
            
            # If all formats fail, return current time
            return datetime.now()
            
        except Exception:
            return datetime.now()
    
    def parse_iso_date(self, date_str: str) -> datetime:
        """Parse ISO date format."""
        try:
            # Remove timezone info if present for simple parsing
            date_str = re.sub(r'[+\-]\d{2}:?\d{2}$', '', date_str)
            date_str = date_str.replace('T', ' ').replace('Z', '')
            
            return datetime.strptime(date_str[:19], '%Y-%m-%d %H:%M:%S')
        except Exception:
            return datetime.now()

class DisasterAPI:
    """Main API class for the real disaster aggregation system."""
    
    def __init__(self):
        self.config = Config()
        self.aggregator = RealDisasterDataAggregator(self.config)
        self.start_background_tasks()
    
    def start_background_tasks(self):
        """Start background tasks for data collection and cleanup."""
        def run_scheduler():
            # Initial data fetch
            self.update_all_disaster_data()
            
            schedule.every(30).minutes.do(self.update_all_disaster_data)
            schedule.every(2).hours.do(self.cleanup_old_data)
            
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("Background tasks started")
    
    def update_all_disaster_data(self):
        """Update disaster data from all sources."""
        try:
            logger.info("Starting disaster data update from all sources")
            
            # Fetch from all sources
            usgs_incidents = self.aggregator.fetch_usgs_earthquakes()
            gdacs_incidents = self.aggregator.fetch_gdacs_disasters()
            reliefweb_incidents = self.aggregator.fetch_reliefweb_disasters()
            
            total_incidents = len(usgs_incidents) + len(gdacs_incidents) + len(reliefweb_incidents)
            logger.info(f"Updated {total_incidents} total incidents from all sources")
            
        except Exception as e:
            logger.error(f"Error updating disaster data: {e}")
    
    def cleanup_old_data(self):
        """Clean up old incident data."""
        try:
            self.aggregator.db.cleanup_old_data(days=30)
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
        radius_km = request.args.get('radius', 200, type=int)  # Default 200km radius
        hours = request.args.get('hours', 168, type=int)  # Default 1 week
        
        incidents = disaster_api.aggregator.db.get_incidents_by_location(location, radius_km, hours)
        
        # Format timestamps for frontend
        for incident in incidents:
            if isinstance(incident['timestamp'], str):
                timestamp = datetime.fromisoformat(incident['timestamp'])
            else:
                timestamp = datetime.fromisoformat(incident['timestamp'])
            
            # Calculate time difference
            time_diff = datetime.now() - timestamp
            
            if time_diff.total_seconds() < 3600:  # Less than 1 hour
                minutes = int(time_diff.total_seconds() / 60)
                incident['time_ago'] = f"{minutes} minutes ago" if minutes > 0 else "Just now"
            elif time_diff.total_seconds() < 86400:  # Less than 24 hours
                hours = int(time_diff.total_seconds() / 3600)
                incident['time_ago'] = f"{hours} hour{'s' if hours != 1 else ''} ago"
            else:
                days = int(time_diff.total_seconds() / 86400)
                incident['time_ago'] = f"{days} day{'s' if days != 1 else ''} ago"
        
        # Calculate statistics
        stats = {
            'total': len(incidents),
            'earthquake': len([i for i in incidents if i['type'] == 'earthquake']),
            'flood': len([i for i in incidents if i['type'] == 'flood']),
            'cyclone': len([i for i in incidents if i['type'] == 'cyclone']),
            'wildfire': len([i for i in incidents if i['type'] == 'wildfire']),
            'high_severity': len([i for i in incidents if i['severity'] == 'high']),
            'verified': len([i for i in incidents if i['verified']])
        }
        
        return jsonify({
            'success': True,
            'location': location,
            'incidents': incidents[:30],  # Limit to 30 most recent
            'stats': stats,
            'last_updated': datetime.now().isoformat(),
            'data_sources': ['USGS', 'GDACS', 'ReliefWeb']
        })
        
    except Exception as e:
        logger.error(f"Error getting disasters for {location}: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'incidents': [],
            'stats': {'total': 0, 'earthquake': 0, 'flood': 0, 'cyclone': 0, 'wildfire': 0}
        }), 500

@app.route('/api/stats')
def get_overall_stats():
    """Get overall statistics for all disasters."""
    try:
        conn = sqlite3.connect(disaster_api.config.DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get stats for last 7 days
        cutoff_time = datetime.now() - timedelta(days=7)
        
        cursor.execute('''
            SELECT type, severity, source, COUNT(*) as count
            FROM incidents 
            WHERE timestamp > ?
            GROUP BY type, severity, source
            ORDER BY count DESC
        ''', (cutoff_time,))
        
        results = cursor.fetchall()
        conn.close()
        
        stats = {
            'total_incidents': sum(row[3] for row in results),
            'by_type': {},
            'by_severity': {},
            'by_source': {},
            'last_updated': datetime.now().isoformat()
        }
        
        for incident_type, severity, source, count in results:
            # By type
            if incident_type not in stats['by_type']:
                stats['by_type'][incident_type] = 0
            stats['by_type'][incident_type] += count
            
            # By severity
            if severity not in stats['by_severity']:
                stats['by_severity'][severity] = 0
            stats['by_severity'][severity] += count
            
            # By source
            source_short = source.split(' ')[0]  # Get first word of source
            if source_short not in stats['by_source']:
                stats['by_source'][source_short] = 0
            stats['by_source'][source_short] += count
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting overall stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/locations')
def get_tracked_locations():
    """Get list of available locations with coordinates."""
    return jsonify({
        'success': True,
        'locations': [
            {'name': name, 'coordinates': coords} 
            for name, coords in Config.CITY_COORDINATES.items()
        ]
    })

@app.route('/api/refresh')
def refresh_data():
    """Manually refresh disaster data from all sources."""
    try:
        disaster_api.update_all_disaster_data()
        return jsonify({
            'success': True,
            'message': 'Data refresh initiated',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected' if os.path.exists(disaster_api.config.DATABASE_PATH) else 'disconnected',
        'data_sources': {
            'usgs': 'https://earthquake.usgs.gov',
            'gdacs': 'https://www.gdacs.org',
            'reliefweb': 'https://api.reliefweb.int'
        }
    })

if __name__ == '__main__':
    # Initialize database and start background tasks
    logger.info("Starting Real Disaster Data Aggregation System")
    
    # Install required packages note
    print("\nIMPORTANT: Make sure you have installed the required packages:")
    print("pip install flask flask-cors requests geopy schedule")
    print("\nStarting server...")
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)