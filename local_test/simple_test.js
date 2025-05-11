/**
 * Einfacher Test für Bazaraki mit axios/cheerio statt Puppeteer
 * Benötigt keine Chrome-Installation
 */

const axios = require('axios');
const { JSDOM } = require('jsdom');
const fs = require('fs');

// Bazaraki-Konfiguration
const BASE_URL = 'https://www.bazaraki.com';
const GEO_LOCATIONS = {
  'paphos': {
    lat: 34.797537264230336,
    lng: 32.434836385742194,
    radius: 20000  // 20km Radius
  }
};

// Einfache Funktion zum Extrahieren von Links
async function extractLinks() {
  console.log('Starte einfachen Bazaraki-Test...');
  
  // Konfiguration
  const config = {
    location: 'paphos',
    dealType: 'rent',
    propertyType: 'apartments',
    minPrice: 500,
    maxPrice: 1500
  };
  
  // URL erstellen
  let urlPath = config.dealType === 'rent' 
    ? '/real-estate-to-rent/' 
    : '/real-estate-to-buy/';
  
  urlPath += config.propertyType === 'apartments'
    ? 'apartments-flats/'
    : 'houses/';
  
  // Parameter
  const params = new URLSearchParams();
  
  // Geo-Koordinaten
  if (GEO_LOCATIONS[config.location.toLowerCase()]) {
    const geo = GEO_LOCATIONS[config.location.toLowerCase()];
    params.append('lat', geo.lat);
    params.append('lng', geo.lng);
    params.append('radius', geo.radius);
  }
  
  // Preisfilter
  if (config.minPrice) params.append('price_min', config.minPrice);
  if (config.maxPrice) params.append('price_max', config.maxPrice);
  
  const url = `${BASE_URL}${urlPath}?${params.toString()}`;
  console.log(`Teste URL: ${url}`);
  
  try {
    // Seite mit realistischem User-Agent abrufen
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
      }
    });
    
    // HTML-Inhalt speichern
    fs.writeFileSync('bazaraki_response.html', response.data);
    console.log('HTML-Antwort in bazaraki_response.html gespeichert');
    
    // HTML mit JSDOM parsen
    const dom = new JSDOM(response.data);
    const document = dom.window.document;
    
    // Nach Anzeigenlinks suchen
    const adLinks = document.querySelectorAll('a[href*="/adv/"]');
    console.log(`Gefunden: ${adLinks.length} potenzielle Anzeigenlinks`);
    
    // Anzeigenlinks extrahieren und filtern
    const listings = Array.from(adLinks)
      .map(link => {
        const href = link.getAttribute('href');
        // Nur Links zu tatsächlichen Anzeigen (nicht zu Benutzer- oder Kategorieseiten)
        if (href && !href.includes('/user/') && href.match(/\/adv\/\d+/)) {
          return {
            url: href.startsWith('/') ? `${BASE_URL}${href}` : href,
            id: href.match(/\/adv\/(\d+)/)?.[1] || '',
            title: link.getAttribute('title') || link.textContent || 'Keine Beschreibung'
          };
        }
        return null;
      })
      .filter(item => item !== null);
    
    // Duplikate entfernen
    const uniqueListings = {};
    listings.forEach(listing => {
      if (listing.id) {
        uniqueListings[listing.id] = listing;
      }
    });
    
    const finalListings = Object.values(uniqueListings);
    console.log(`Nach Entfernen von Duplikaten: ${finalListings.length} einzigartige Anzeigen`);
    
    // Details ausgeben
    console.log('\nErste 5 gefundene Anzeigen:');
    finalListings.slice(0, 5).forEach((listing, index) => {
      console.log(`${index + 1}. ID: ${listing.id}, Titel: ${listing.title.substring(0, 50)}`);
      console.log(`   URL: ${listing.url}`);
    });
    
    // Prüfen, ob JavaScript-Daten vorhanden sind
    const scriptTags = document.querySelectorAll('script');
    let hasJSData = false;
    
    scriptTags.forEach(script => {
      const content = script.textContent || '';
      if (content.includes('window.__INITIAL_STATE__') || 
          content.includes('window.__STATE__') || 
          content.includes('window.bazaraki')) {
        hasJSData = true;
        console.log('\nGefundene JavaScript-Daten im HTML:');
        console.log(content.substring(0, 200) + '...');
      }
    });
    
    if (!hasJSData) {
      console.log('\nKeine eingebetteten JavaScript-Daten gefunden. Die Seite nutzt möglicherweise AJAX-Anfragen.');
    }
    
    return finalListings;
  } catch (error) {
    console.error(`Fehler beim Testen: ${error.message}`);
    if (error.response) {
      console.error(`Statuscode: ${error.response.status}`);
      fs.writeFileSync('error_response.html', error.response.data);
      console.log('Fehlerantwort in error_response.html gespeichert');
    }
    return [];
  }
}

// Ausführen
extractLinks().catch(error => console.error('Unbehandelter Fehler:', error));
