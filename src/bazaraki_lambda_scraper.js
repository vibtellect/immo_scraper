/**
 * Bazaraki Lambda Scraper
 * 
 * Effiziente Lambda-Funktion zum Scrapen von Bazaraki-Immobilienanzeigen
 * Verwendet axios/jsdom statt Puppeteer für bessere Lambda-Kompatibilität
 */

const axios = require('axios');
const { JSDOM } = require('jsdom');
const AWS = require('aws-sdk');

// Überprüfen, ob wir uns im Testmodus befinden
const isLocalTest = process.env.NODE_ENV === 'test' || !process.env.AWS_LAMBDA_FUNCTION_NAME;

// S3-Client initialisieren (mit Mock für lokales Testen)
let s3;
if (isLocalTest) {
  console.log('Lokaler Testmodus: Verwende S3-Mock-Funktionalität');
  // Mock S3 für lokale Tests
  const localS3Storage = {};
  
  s3 = {
    getObject: (params) => ({
      promise: () => {
        return new Promise((resolve, reject) => {
          const key = `${params.Bucket}/${params.Key}`;
          if (localS3Storage[key]) {
            resolve({
              Body: Buffer.from(localS3Storage[key])
            });
          } else {
            reject(new Error('Die Datei existiert nicht.'));
          }
        });
      }
    }),
    putObject: (params) => ({
      promise: () => {
        return new Promise((resolve) => {
          const key = `${params.Bucket}/${params.Key}`;
          localS3Storage[key] = params.Body.toString();
          console.log(`[S3-MOCK] Datei gespeichert: ${key}`);
          resolve({ ETag: 'mock-etag' });
        });
      }
    })
  };
} else {
  // Echter S3-Client für Lambda
  s3 = new AWS.S3();
}

// Konfiguration aus Umgebungsvariablen
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME || 'bazaraki-scraper-results';
const RESULTS_PREFIX = process.env.RESULTS_PREFIX || 'results/';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

// Preisfilter-Konfiguration (Standard: 1500€)
const DEFAULT_PRICE_MAX = process.env.DEFAULT_PRICE_MAX || '1500';
const DEBUG_MODE = process.env.DEBUG_MODE === 'true';

// Bazaraki-Konfiguration
const BASE_URL = 'https://www.bazaraki.com';
const GEO_LOCATIONS = {
  'paphos': {
    lat: 34.797537264230336,
    lng: 32.434836385742194,
    radius: 20000  // 20km Radius
  },
  'limassol': {
    lat: 34.684422,
    lng: 33.037085,
    radius: 20000
  }
};

// Basisfilter für Immobiliensuche
const DEFAULT_FILTERS = {
  propertyType: 'apartments-flats',  // Art der Immobilie (apartments-flats, houses, etc.)
  district: 'pafos-district-paphos', // Bezirk/Stadt
  radius: '20',                      // Umkreis in km
  price_max: DEFAULT_PRICE_MAX,      // Maximaler Preis (konfigurierbar)
  bedrooms: '2-3'                    // Anzahl Schlafzimmer (Bereich oder exakt)
};

/**
 * Lädt die vorherigen Ergebnisse aus dem S3-Bucket
 */
async function loadPreviousResults(filterKey = '') {
  const today = new Date().toISOString().split('T')[0];
  const key = filterKey 
    ? `${RESULTS_PREFIX}${filterKey}/${today}.json`
    : `${RESULTS_PREFIX}${today}.json`;
  
  let previousResults = { listings: [] };
  let isFirstRun = false;
  let previousFileFound = false;
  
  try {
    // Suche nach der neuesten Datei im S3 Bucket mit dem gleichen Filter
    const filterPrefix = filterKey ? `${RESULTS_PREFIX}${filterKey}/` : RESULTS_PREFIX;
    
    const listParams = {
      Bucket: S3_BUCKET_NAME,
      Prefix: filterPrefix
    };
    
    console.log(`Suche nach vorherigen Ergebnissen mit Prefix: ${filterPrefix}`);
    const listedObjects = await s3.listObjectsV2(listParams).promise();
    
    if (listedObjects.Contents.length === 0) {
      // Keine vorherigen Dateien gefunden
      console.log('S3-Bucket ist leer oder enthält keine Dateien mit diesem Filter. Dies ist der erste Lauf.');
      isFirstRun = true;
    } else {
      // Sortiere Dateien nach LastModified (neueste zuerst)
      const sortedObjects = [...listedObjects.Contents].sort((a, b) => 
        new Date(b.LastModified) - new Date(a.LastModified)
      );
      
      // Aktuelle Datei überspringen, falls sie bereits existiert
      let latestFile = sortedObjects[0];
      if (latestFile.Key === key && sortedObjects.length > 1) {
        latestFile = sortedObjects[1]; // Nimm die zweitneueste, wenn die neueste die aktuelle ist
      }
      
      if (latestFile && latestFile.Key !== key) {
        // Lade die Daten aus der letzten Datei
        const previousData = await s3.getObject({
          Bucket: S3_BUCKET_NAME,
          Key: latestFile.Key
        }).promise();
        
        if (previousData && previousData.Body) {
          previousResults = JSON.parse(previousData.Body.toString());
          previousFileFound = true;
          console.log(`Vorherige Ergebnisse geladen aus ${latestFile.Key}: ${previousResults.listings.length} Anzeigen`);
        }
      } else {
        console.log('Keine geeignete vorherige Datei gefunden.');
      }
    }
  } catch (error) {
    console.error(`Fehler beim Laden vorheriger Ergebnisse: ${error.message}`);
  }
  
  // Wenn keine vorherige Datei gefunden wurde, noch einmal prüfen, ob es wirklich der erste Lauf ist
  if (!previousFileFound && !isFirstRun) {
    try {
      const checkParams = {
        Bucket: S3_BUCKET_NAME,
        Prefix: RESULTS_PREFIX
      };
      
      const allObjects = await s3.listObjectsV2(checkParams).promise();
      if (allObjects.Contents.length === 0) {
        console.log('Bestätigt: S3-Bucket ist komplett leer. Dies ist der erste Lauf.');
        isFirstRun = true;
      }
    } catch (checkError) {
      console.error(`Fehler bei der Überprüfung des S3-Buckets: ${checkError.message}`);
    }
  }
  
  return { previousResults, isFirstRun };
}

/**
 * Hauptfunktion: Scannt bazaraki.com nach Immobilienanzeigen
 */
async function scrapeListings(filters = {}) {
  try {
    // Überprüfen, ob wir im Debug-Modus mit Test-Daten arbeiten sollen
    if (process.env.DEBUG_MODE === 'true') {
      console.log('DEBUG-MODUS: Verwende Test-Daten anstatt Live-Scraping');
      return generateTestListings(5);
    }
    
    // Filterkey generieren
    const filterKey = filters.price_max ? `price_max_${filters.price_max}` : `price_max_${DEFAULT_PRICE_MAX}`;
    console.log(`Verwende Filter-Schlüssel für S3-Ergebnisse: ${filterKey}`);
    
    // Vorherige Ergebnisse laden, bevor wir mit dem Scraping beginnen
    console.log('Lade vorherige Ergebnisse aus S3...');
    const { previousResults, isFirstRun } = await loadPreviousResults(filterKey);
    const previousIds = new Set(previousResults.listings.map(listing => listing.id));
    console.log(`${previousIds.size} vorherige Anzeigen-IDs geladen`);
    
    // Bestimme, welche Immobilientypen gescrapt werden sollen
    let propertyTypes = [];
    if (filters.propertyTypes && Array.isArray(filters.propertyTypes)) {
      // Wenn explizite Liste von Immobilientypen angegeben wurde
      propertyTypes = filters.propertyTypes;
    } else if (filters.propertyType) {
      // Wenn ein einzelner Typ angegeben wurde
      propertyTypes = [filters.propertyType];
    } else {
      // Standard: Sowohl Apartments als auch Häuser scrapen
      propertyTypes = ['apartments-flats', 'houses'];
    }
    
    console.log(`Scrape folgende Immobilientypen: ${propertyTypes.join(', ')}`);
    
    const newListings = [];
    const allCurrentListings = [];
    const allProcessedIds = new Set();
    
    // Jeden Immobilientyp nacheinander komplett verarbeiten
    for (const propertyType of propertyTypes) {
      console.log(`\n== Beginne komplette Verarbeitung für Immobilientyp: ${propertyType} ==\n`);
      
      // Filter kombinieren (Standard + benutzerdefiniert)
      const searchFilters = { 
        ...DEFAULT_FILTERS, 
        ...filters,
        propertyType // Überschreibe mit aktuellem Typ
      };
      
      console.log(`Starte Scraping für ${propertyType} mit Filtern: ${JSON.stringify(searchFilters)}`);
      
      // Spezifische URLs verwenden anstatt generierter URLs
      let searchUrl;
      if (propertyType === 'apartments-flats') {
        searchUrl = 'https://www.bazaraki.com/real-estate-to-rent/apartments-flats/number-of-bedrooms---2/number-of-bedrooms---3/number-of-bedrooms---4/?price_max=1250&lat=34.82605406475855&lng=32.40992763476561&radius=10000';
      } else if (propertyType === 'houses') {
        searchUrl = 'https://www.bazaraki.com/real-estate-to-rent/houses/number-of-bedrooms---2/number-of-bedrooms---3/pafos-district-paphos/?price_max=1250';
      } else {
        // Fallback auf generierte URL, falls ein anderer Typ verwendet wird
        searchUrl = buildSearchUrl(searchFilters);
      }
      console.log(`Such-URL für ${propertyType}: ${searchUrl}`);
      
      // Suchseite laden und alle Anzeigen-Links extrahieren
      console.log(`Lade Suchergebnisseiten für ${propertyType}...`);
      const urls = await extractListingUrls(searchUrl, 10); // Stelle sicher, dass bis zu 10 Seiten gescrapt werden
      
      // URLs und IDs aufbereiten
      const listingUrlsWithIds = [];
      for (const url of urls) {
        const id = extractAdId(url);
        if (id) {
          listingUrlsWithIds.push({ url, id });
          allProcessedIds.add(id); // Für späteren Vergleich aller IDs
        } else {
          console.warn(`Konnte keine Ad-ID für URL extrahieren: ${url}`);
        }
      }
      
      console.log(`${listingUrlsWithIds.length} Anzeigen-URLs für ${propertyType} gefunden`);
      
      // Vergleich mit vorherigen IDs für diesen Immobilientyp
      const currentTypeIds = new Set(listingUrlsWithIds.map(item => item.id));
      const newIdsForType = [...currentTypeIds].filter(id => !previousIds.has(id));
      
      console.log(`Schnellvergleich für ${propertyType}: ${newIdsForType.length} neue Anzeigen identifiziert`);
      
      // Detaillierte Informationen für neue Anzeigen dieses Typs abrufen
      if (newIdsForType.length > 0) {
        console.log(`Verarbeite ${newIdsForType.length} neue ${propertyType}-Anzeigen im Detail...`);
        
        // Nur die URLs der neuen Anzeigen herausfiltern
        const newListingUrlsForType = listingUrlsWithIds
          .filter(item => newIdsForType.includes(item.id))
          .map(item => ({ url: item.url, id: item.id }));
        
        // Detaillierte Daten für jede neue Anzeige extrahieren
        for (let i = 0; i < newListingUrlsForType.length; i++) {
          const { url, id } = newListingUrlsForType[i];
          
          console.log(`Verarbeite neue ${propertyType}-Anzeige ${i+1}/${newListingUrlsForType.length}: ${id}`);
          
          try {
            // Basisinformationen
            const listing = {
              id,
              url,
              propertyType, // Speichere den Immobilientyp im Objekt
              scrapedAt: new Date().toISOString()
            };
            
            // Detaillierte Informationen extrahieren
            const details = await extractListingDetails(url);
            const newListing = { ...listing, ...details };
            
            // Zu beiden Listen hinzufügen
            newListings.push(newListing);
            allCurrentListings.push(newListing);
            
            // Kurze Pause zwischen den Anfragen
            await delay(500);
          } catch (error) {
            console.error(`Fehler beim Verarbeiten der neuen Anzeige ${id}: ${error.message}`);
          }
        }
      } else {
        console.log(`Keine neuen ${propertyType}-Anzeigen gefunden, überspringe detailliertes Scraping für diesen Typ.`);
      }
      
      console.log(`\n== Verarbeitung für Immobilientyp ${propertyType} abgeschlossen ==\n`);
      
      // Kurze Pause zwischen den Immobilientypen
      await delay(1000);
    }
    
    // Globaler Vergleich für entfernte Anzeigen
    const removedIds = [...previousIds].filter(id => !allProcessedIds.has(id));
    console.log(`Gesamtvergleich: ${newListings.length} neue Anzeigen über alle Typen, ${removedIds.length} entfernte Anzeigen`);

    
    // Füge alle unveränderten Anzeigen aus den vorherigen Ergebnissen hinzu
    if (previousResults.listings && previousResults.listings.length > 0) {
      // Für jede vorherige Anzeige prüfen, ob sie noch aktuell ist (nicht entfernt)
      const unchangedListings = previousResults.listings.filter(listing => 
        currentIds.has(listing.id) && !newIds.includes(listing.id)
      );
      
      console.log(`Füge ${unchangedListings.length} unveränderte Anzeigen aus dem Cache hinzu`);
      allCurrentListings.push(...unchangedListings);
    }
    
    console.log(`Gesamtergebnis: ${allCurrentListings.length} aktuelle Anzeigen (${newListings.length} neue, ${removedIds.length} entfernte)`);
    
    // Rückgabe aller aktuellen Anzeigen (neue + unveränderte)
    return allCurrentListings;
  } catch (error) {
    console.error(`Fehler beim Scrapen der Anzeigen: ${error.message}`);
    throw error;
  }
}

/**
 * Generiert Test-Daten für lokales Testen
 */
function generateTestListings(count = 5) {
  console.log(`Generiere ${count} Test-Anzeigen für lokales Testen...`);
  
  const listings = [];
  
  // Beispiel-Standorte in Limassol
  const locations = ['Limassol Marina', 'Germasogeia', 'Amathus', 'Neapolis', 'Molos', 'Agios Tychonas', 'Potamos Germasogeia'];
  
  // Beispiel-Titel
  const titles = [
    'Schöne Möblierte Wohnung mit Meerblick',
    'Moderne 2-Schlafzimmer-Wohnung in zentraler Lage',
    'Luxuriöses Apartment mit Pool und Garten',
    'Gemütliche Wohnung in Strandnahe',
    'Neu renovierte Wohnung mit Balkon',
    'Penthouse mit Panoramablick auf die Stadt',
    'Geschäftswohnung im Stadtzentrum'
  ];
  
  // Beispiel-Preise
  const prices = [
    { text: '€1,200 / Monat', value: 1200 },
    { text: '€1,500 / Monat', value: 1500 },
    { text: '€1,800 / Monat', value: 1800 },
    { text: '€950 / Monat', value: 950 },
    { text: '€1,350 / Monat', value: 1350 },
    { text: '€2,000 / Monat', value: 2000 },
    { text: '€1,100 / Monat', value: 1100 }
  ];
  
  // Beispiel-Bilder
  const images = [
    ['https://example.com/apartment1.jpg', 'https://example.com/apartment1_2.jpg'],
    ['https://example.com/apartment2.jpg', 'https://example.com/apartment2_2.jpg', 'https://example.com/apartment2_3.jpg'],
    ['https://example.com/apartment3.jpg'],
    ['https://example.com/apartment4.jpg', 'https://example.com/apartment4_2.jpg'],
    ['https://example.com/apartment5.jpg', 'https://example.com/apartment5_2.jpg']
  ];
  
  // Test-Anzeigen generieren
  for (let i = 0; i < count; i++) {
    const id = `test-${1000 + i}`;
    
    listings.push({
      id,
      url: `https://www.bazaraki.com/adv/${id}/`,
      scrapedAt: new Date().toISOString(),
      title: titles[i % titles.length],
      price: prices[i % prices.length],
      location: locations[i % locations.length],
      description: `Dies ist eine Test-Beschreibung für die Anzeige ${id}. Die Wohnung verfügt über mehrere Zimmer und eine gute Ausstattung.`,
      details: {
        bedrooms: 2 + (i % 2),
        bathrooms: 1 + (i % 2),
        area: 85 + (i * 10)
      },
      images: images[i % images.length]
    });
  }
  
  console.log(`${listings.length} Test-Anzeigen generiert`);
  return listings;
}

/**
 * Baut eine Such-URL mit den angegebenen Filtern
 */
function buildSearchUrl(filters) {
  // Neue URL-Struktur von Bazaraki (Stand Mai 2025)
  // https://www.bazaraki.com/real-estate-to-rent/apartments-flats/pafos-district-paphos/?lat=&lng=&radius=20
  
  // Basis-URL für Immobilien-Miete
  let urlPath = '/real-estate-to-rent';
  
  // Immobilientyp hinzufügen (z.B. apartments-flats, houses, etc.)
  if (filters.propertyType) {
    urlPath += `/${filters.propertyType}`;
  }
  
  // Bezirk/Stadt hinzufügen
  if (filters.district) {
    urlPath += `/${filters.district}`;
  }
  
  // URL erstellen
  const url = new URL(`${BASE_URL}${urlPath}/`);
  
  // Standardparameter hinzufügen
  url.searchParams.append('lat', '');
  url.searchParams.append('lng', '');
  
  // Zusätzliche Filter als Query-Parameter hinzufügen
  for (const [key, value] of Object.entries(filters)) {
    // propertyType und district sind bereits im Pfad enthalten
    if (!['propertyType', 'district'].includes(key) && value) {
      url.searchParams.append(key, value);
    }
  }
  
  return url.toString();
}

/**
 * Extrahiert die ID aus einer Anzeigen-URL
 */
function extractIdFromUrl(url) {
  try {
    // Format: https://www.bazaraki.com/adv/123456/
    const match = url.match(/\/adv\/([0-9]+)/i);
    return match ? match[1] : `unknown-${Date.now()}`;
  } catch (error) {
    console.error(`Fehler beim Extrahieren der ID aus URL ${url}: ${error.message}`);
    return `unknown-${Date.now()}`;
  }
}

/**
 * Extrahiert Anzeigen-URLs von der Suchergebnisseite
 */
async function extractListingUrls(url, maxPages = 10) {
  try {
    const allUrls = new Set();
    let currentUrl = url;
    
    // Bis zu maxPages Seiten durchsuchen
    for (let page = 1; page <= maxPages; page++) {
      console.log(`Lade Suchergebnisseite ${page}...`);
      
      // Seite laden
      const response = await axios.get(currentUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
      });
      
      // HTML parsen
      const dom = new JSDOM(response.data);
      const document = dom.window.document;
      
      // Versuche, die Gesamtzahl der Anzeigen zu extrahieren
      try {
        const countElement = document.querySelector('.search-header__count');
        if (countElement) {
          console.log(`Gesamtzahl laut Website: ${countElement.textContent.trim()}`);
        }
      } catch (countError) {
        console.log('Konnte die Gesamtzahl der Anzeigen nicht extrahieren');
      }
      
      // Alle Anzeigen-Links extrahieren (mehrere Selektoren testen)
      const adLinks = [];
      
      // Verschiedene Selektoren für Links zu Anzeigen ausprobieren
      document.querySelectorAll('a[href*="/adv/"]').forEach(link => adLinks.push(link));
      document.querySelectorAll('.announcement__link').forEach(link => adLinks.push(link));
      document.querySelectorAll('.announcement-container a').forEach(link => adLinks.push(link));
      document.querySelectorAll('div.announcement a').forEach(link => adLinks.push(link));
      
      // Links zur Menge hinzufügen (für Eindeutigkeit)
      let newLinks = 0;
      adLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href) {
          // Vollständigen URL erstellen, falls es ein relativer Link ist
          const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
          
          // Nur eindeutige Links zur Menge hinzufügen
          if (!allUrls.has(fullUrl) && fullUrl.includes('/adv/')) {
            allUrls.add(fullUrl);
            newLinks++;
          }
        }
      });
      
      console.log(`Seite ${page}: ${newLinks} neue Anzeigen-Links gefunden`);
      
      // Nach nächster Seite suchen mit verschiedenen möglichen Selektoren
      const paginationSelectors = [
        '.pagination-next a',
        '.pagination-wrapper .next-page',
        '.pagination-wrapper a[rel="next"]',
        '.pagination a.next',
        'a.next-page',
        'a[rel="next"]',
        'li.next a',
        // Dynamisch alle Link-Elemente mit "next", "weiter" oder Seite+1 im Text finden
        'a[href*="page"]'
      ];
      
      let nextPageLink = null;
      let nextHref = null;
      
      // Alle möglichen Selektoren für den "Nächste Seite"-Link durchgehen
      for (const selector of paginationSelectors) {
        const links = document.querySelectorAll(selector);
        
        // Debug-Info für jeden Selektor
        if (links.length > 0) {
          console.log(`Gefundene Links für Selektor "${selector}": ${links.length}`);
          
          // Bei mehreren "nächste Seite"-Links, alle prüfen
          for (const link of links) {
            const href = link.getAttribute('href');
            const linkText = link.textContent.trim();
            
            // Debug-Info
            console.log(`Möglicher nächster Seiten-Link: ${linkText} -> ${href}`);
            
            // Prüfen, ob der Link zur nächsten Seite führt (enthält page=N+1 oder ähnliches)
            if (href && (
              href.includes(`page=${page + 1}`) || 
              href.includes(`/page/${page + 1}`) ||
              linkText === `${page + 1}` ||
              linkText.toLowerCase().includes('next') ||
              linkText.toLowerCase().includes('weiter')
            )) {
              nextPageLink = link;
              nextHref = href;
              console.log(`Nächste Seite gefunden: ${linkText} -> ${href}`);
              break;
            }
          }
        }
        
        if (nextPageLink) break;
      }
      
      // Wenn ein nächster Seiten-Link gefunden wurde, zur nächsten Seite navigieren
      if (nextPageLink && nextHref && page < maxPages) {
        currentUrl = nextHref.startsWith('http') ? nextHref : `${BASE_URL}${nextHref}`;
        console.log(`Navigiere zur nächsten Seite: ${currentUrl}`);
      } else {
        console.log('Keine weitere Seite gefunden oder maximale Seitenzahl erreicht.');
        
        // Versuch, die letzte Seite automatisch zu bestimmen und dort weiterzumachen
        try {
          const paginationLinks = document.querySelectorAll('.pagination a');
          let highestPage = page;
          
          paginationLinks.forEach(link => {
            const pageNum = parseInt(link.textContent.trim());
            if (!isNaN(pageNum) && pageNum > highestPage && pageNum <= maxPages) {
              highestPage = pageNum;
            }
          });
          
          if (highestPage > page) {
            console.log(`Direkte Navigation zu Seite ${highestPage} versuchen...`);
            // Erstellt eine neue URL mit geänderter Seitenzahl
            const parts = currentUrl.split('?');
            const baseUrl = parts[0];
            const params = new URLSearchParams(parts[1] || '');
            
            // URL anpassen je nachdem, welches Format verwendet wird
            if (currentUrl.includes('page=')) {
              // Format: ?page=N
              params.set('page', highestPage.toString());
              currentUrl = `${baseUrl}?${params.toString()}`;
            } else if (currentUrl.includes('/page/')) {
              // Format: /page/N/
              currentUrl = currentUrl.replace(/\/page\/\d+/, `/page/${highestPage}`);
            } else {
              // Füge als neuen Parameter hinzu
              params.set('page', highestPage.toString());
              currentUrl = `${baseUrl}?${params.toString()}`;
            }
            
            console.log(`Neue URL: ${currentUrl}`);
            continue; // Überspringe break, versuche die neue URL
          }
        } catch (paginationError) {
          console.log(`Fehler bei der Paginierung: ${paginationError.message}`);
        }
        
        break; // Keine weiteren Seiten oder maximale Seitenzahl erreicht
      }
      
      // Kurze Pause zwischen den Seitenaufrufen
      console.log(`Pause vor Laden der nächsten Seite (${page + 1})...`);
      await delay(1500);
    }
    
    console.log(`Insgesamt ${allUrls.size} eindeutige Anzeigen-URLs gefunden`);
    return [...allUrls];
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Anzeigen-URLs: ${error.message}`);
    return [];
  }
}

/**
 * Extrahiert detaillierte Informationen für eine einzelne Anzeige
 */
async function extractListingDetails(url) {
  try {
    // Anzeigenseite laden
    const response = await axios.get(url, {
      headers: DEFAULT_HEADERS
    });
    
    // HTML parsen
    const dom = new JSDOM(response.data);
    const document = dom.window.document;
    
    // Titel extrahieren
    const titleElement = document.querySelector('h1.announcement-title, .adv-title, .title');
    const title = titleElement ? titleElement.textContent.trim() : 'Keine Beschreibung';
    
    // Verschiedene Informationen extrahieren
    const details = {
      title,
      price: extractPrice(document),
      location: extractLocation(document),
      description: extractDescription(document),
      details: extractPropertyDetails(document),
      images: extractImages(document)
    };
    
    return details;
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Anzeigendetails für ${url}: ${error.message}`);
    return {
      title: 'Fehler beim Laden der Anzeige',
      price: { text: 'Unbekannt', value: 0 },
      details: {},
      description: '',
      images: []
    };
  }
}

// Standard HTTP-Header für realistische Browser-Simulation
const DEFAULT_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
  'Accept-Language': 'en-US,en;q=0.9',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
};

/**
 * Hilfsfunktion zum Extrahieren der Ad-ID aus einer URL oder Anzeige
 */
function extractAdId(urlOrListing) {
  if (typeof urlOrListing === 'string') {
    // Wenn es eine URL ist, extrahiere die ID aus der URL
    const match = urlOrListing.match(/\/adv\/([0-9]+)/);
    return match ? match[1] : null;
  } else if (urlOrListing && urlOrListing.url) {
    // Wenn es ein Listing-Objekt ist, extrahiere die ID aus der URL
    return extractAdId(urlOrListing.url);
  } else if (urlOrListing && urlOrListing.id) {
    // Wenn es bereits eine ID hat, verwende diese
    return urlOrListing.id;
  }
  return null;
}

/**
 * Sammelt alle Anzeigen-URLs basierend auf den Filterparametern
 */
async function collectListingUrls(config) {
  // URL-Pfad basierend auf Typ
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
  
  const allListings = [];
  
  // Durch Seiten iterieren
  for (let page = 1; page <= (config.maxPages || 3); page++) {
    params.set('page', page);
    const url = `${BASE_URL}${urlPath}?${params.toString()}`;
    console.log(`Sammle Anzeigen von Seite ${page}: ${url}`);
    
    try {
      // Seite abrufen
      const response = await axios.get(url, { 
        headers: DEFAULT_HEADERS,
        timeout: 10000 // 10 Sekunden Timeout für Lambda
      });
      
      // Debug-Ausgabe
      if (DEBUG_MODE) {
        // In Lambda speichern wir in S3 statt lokalem Dateisystem
        const debugKey = `debug/page_${page}_${Date.now()}.html`;
        await s3.putObject({
          Bucket: S3_BUCKET_NAME,
          Key: debugKey,
          Body: response.data,
          ContentType: 'text/html'
        }).promise();
        console.log(`Debug-HTML für Seite ${page} gespeichert als s3://${S3_BUCKET_NAME}/${debugKey}`);
      }
      
      // HTML parsen
      const dom = new JSDOM(response.data);
      const document = dom.window.document;
      
      // Nach Anzeigenlinks suchen
      const adLinks = document.querySelectorAll('a[href*="/adv/"]');
      console.log(`Gefunden: ${adLinks.length} potenzielle Anzeigenlinks auf Seite ${page}`);
      
      // Links extrahieren und filtern
      const pageListings = Array.from(adLinks)
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
      
      allListings.push(...pageListings);
      
      // Wenn Seite keine Einträge hat, abbrechen
      if (pageListings.length === 0 && page === 1) {
        console.log("Keine Anzeigen auf der ersten Seite gefunden. Abbruch.");
        break;
      }
      
      // Kurze Pause, um Blockierung zu vermeiden
      await delay(1000);
    } catch (error) {
      console.error(`Fehler beim Abrufen der Seite ${page}: ${error.message}`);
      // Weitermachen mit der nächsten Seite
    }
  }
  
  // Duplikate entfernen
  const uniqueListings = {};
  allListings.forEach(listing => {
    if (listing.id) {
      uniqueListings[listing.id] = listing;
    }
  });
  
  return Object.values(uniqueListings);
}

/**
 * Ruft detaillierte Informationen zu jeder Anzeige ab
 */
async function getDetailedListings(listingUrls) {
  const detailedListings = [];
  
  for (let i = 0; i < listingUrls.length; i++) {
    const listing = listingUrls[i];
    console.log(`[${i+1}/${listingUrls.length}] Verarbeite Anzeige: ${listing.id} - ${listing.url}`);
    
    try {
      // Anzeigenseite abrufen
      const response = await axios.get(listing.url, { 
        headers: DEFAULT_HEADERS,
        timeout: 10000 // Lambda-optimiertes Timeout
      });
      
      // HTML parsen
      const dom = new JSDOM(response.data);
      const document = dom.window.document;
      
      // Detaillierte Informationen extrahieren
      const detailedListing = {
        ...listing,
        price: extractPrice(document),
        location: extractLocation(document),
        details: extractPropertyDetails(document),
        description: extractDescription(document),
        images: extractImages(document),
        date: new Date().toISOString()
      };
      
      detailedListings.push(detailedListing);
      
      // Debug-Ausgabe
      if (DEBUG_MODE) {
        const debugKey = `debug/listing_${listing.id}_${Date.now()}.html`;
        await s3.putObject({
          Bucket: S3_BUCKET_NAME,
          Key: debugKey,
          Body: response.data,
          ContentType: 'text/html'
        }).promise();
        console.log(`Debug-HTML für Anzeige ${listing.id} gespeichert als s3://${S3_BUCKET_NAME}/${debugKey}`);
      }
      
      // Kurze Pause, um Blockierung zu vermeiden
      await delay(500);
    } catch (error) {
      console.error(`Fehler beim Abrufen der Detailseite für ${listing.id}: ${error.message}`);
      // Grundlegende Informationen hinzufügen, wenn Details fehlschlagen
      detailedListings.push({
        ...listing,
        error: error.message,
        date: new Date().toISOString()
      });
    }
  }
  
  return detailedListings;
}

/**
 * Extrahiert den Preis aus der Anzeigenseite
 */
function extractPrice(document) {
  try {
    // Verschiedene mögliche Selektoren für den Preis
    const priceSelectors = [
      '.announcement-price__cost',
      '.announcement__price',
      '.price-large',
      '[itemprop="price"]',
      '.price'
    ];
    
    let priceText = '';
    for (const selector of priceSelectors) {
      const priceElement = document.querySelector(selector);
      if (priceElement) {
        priceText = priceElement.textContent.trim();
        break;
      }
    }
    
    if (!priceText) return { amount: null, currency: '€', text: 'Preis auf Anfrage' };
    
    // Preis und Währung extrahieren
    const priceMatch = priceText.match(/(\d[\d\s,.]+)\s*([€$£₽]|EUR)/i);
    if (priceMatch) {
      const amount = parseInt(priceMatch[1].replace(/[\s,.]/g, ''), 10);
      const currency = priceMatch[2] || '€';
      return { amount, currency, text: priceText };
    }
    
    return { amount: null, currency: '€', text: priceText };
  } catch (error) {
    console.error(`Fehler beim Extrahieren des Preises: ${error.message}`);
    return { amount: null, currency: '€', text: 'Unbekannter Preis' };
  }
}

/**
 * Extrahiert den Standort aus der Anzeigenseite
 */
function extractLocation(document) {
  try {
    // Verschiedene mögliche Selektoren für den Standort
    const locationSelectors = [
      '.announcement-address',
      '.announcement__location',
      '[itemprop="address"]',
      '.location'
    ];
    
    for (const selector of locationSelectors) {
      const locationElement = document.querySelector(selector);
      if (locationElement) {
        return locationElement.textContent.trim();
      }
    }
    
    return 'Standort nicht angegeben';
  } catch (error) {
    console.error(`Fehler beim Extrahieren des Standorts: ${error.message}`);
    return 'Standort nicht verfügbar';
  }
}

/**
 * Extrahiert Details wie Schlafzimmer, Badezimmer und Fläche
 */
function extractPropertyDetails(document) {
  try {
    const details = {};
    
    // Schlafzimmer, Badezimmer, Fläche
    const detailsSelectors = {
      bedrooms: ['.bedrooms', '[data-type="bedrooms"]', '.announcement-parameters__bedrooms'],
      bathrooms: ['.bathrooms', '[data-type="bathrooms"]', '.announcement-parameters__bathrooms'],
      area: ['.area', '[data-type="area"]', '.announcement-parameters__area'],
      propertyType: ['.property-type', '[data-type="type"]']
    };
    
    // Für jeden Detailtyp
    for (const [detailType, selectors] of Object.entries(detailsSelectors)) {
      for (const selector of selectors) {
        const element = document.querySelector(selector);
        if (element) {
          let text = element.textContent.trim();
          
          // Zahlen extrahieren
          if (detailType === 'bedrooms') {
            const match = text.match(/(\d+)/);
            if (match) details.bedrooms = parseInt(match[1], 10);
          } else if (detailType === 'bathrooms') {
            const match = text.match(/(\d+)/);
            if (match) details.bathrooms = parseInt(match[1], 10);
          } else if (detailType === 'area') {
            const match = text.match(/(\d+)/);
            if (match) details.area = parseInt(match[1], 10);
          } else {
            details[detailType] = text;
          }
          
          break;
        }
      }
    }
    
    // Allgemeine Detail-Elemente
    const detailElements = document.querySelectorAll('.announcement-parameters__item, .detail-item');
    detailElements.forEach(element => {
      const text = element.textContent.trim();
      
      // Typische Eigenschaftsmerkmale erkennen
      if (text.includes('bedroom') && !details.bedrooms) {
        const match = text.match(/(\d+)\s+bedroom/i);
        if (match) details.bedrooms = parseInt(match[1], 10);
      } else if (text.includes('bathroom') && !details.bathrooms) {
        const match = text.match(/(\d+)\s+bathroom/i);
        if (match) details.bathrooms = parseInt(match[1], 10);
      } else if ((text.includes('m²') || text.includes('sq.m')) && !details.area) {
        const match = text.match(/(\d+)\s*(m²|sq\.m)/i);
        if (match) details.area = parseInt(match[1], 10);
      }
    });
    
    return details;
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Immobiliendetails: ${error.message}`);
    return {};
  }
}

/**
 * Extrahiert die Beschreibung
 */
function extractDescription(document) {
  try {
    // Verschiedene mögliche Selektoren für die Beschreibung
    const descriptionSelectors = [
      '.announcement-description',
      '[itemprop="description"]',
      '.description'
    ];
    
    for (const selector of descriptionSelectors) {
      const descElement = document.querySelector(selector);
      if (descElement) {
        let text = descElement.textContent.trim();
        // Beschreibung kürzen, wenn sie zu lang ist
        if (text.length > 500) {
          text = text.substring(0, 500) + '...';
        }
        return text;
      }
    }
    
    return 'Keine Beschreibung verfügbar';
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Beschreibung: ${error.message}`);
    return 'Beschreibung nicht verfügbar';
  }
}

/**
 * Extrahiert Bilder aus der Anzeigenseite
 */
function extractImages(document) {
  try {
    const images = [];
    
    // Verschiedene mögliche Selektoren für Bildergalerien
    const gallerySelectors = [
      '.announcement-gallery img',
      '.swiper-slide img',
      '.announcement-slider img',
      '[data-src]', 
      '[data-lazy]',
      '.carousel img'
    ];
    
    // Für jeden Selektor nach Bildern suchen
    for (const selector of gallerySelectors) {
      const imgElements = document.querySelectorAll(selector);
      if (imgElements.length > 0) {
        imgElements.forEach(img => {
          // Verschiedene Bildattribute prüfen
          let imgUrl = img.getAttribute('src') || 
                      img.getAttribute('data-src') || 
                      img.getAttribute('data-lazy') ||
                      img.getAttribute('data-background');
          
          if (imgUrl) {
            // Relative URLs zu absoluten machen
            if (imgUrl.startsWith('/')) {
              imgUrl = `${BASE_URL}${imgUrl}`;
            }
            
            // Nur eindeutige Bilder hinzufügen
            if (!images.includes(imgUrl)) {
              images.push(imgUrl);
            }
          }
        });
        
        // Wenn Bilder gefunden wurden, abbrechen
        if (images.length > 0) break;
      }
    }
    
    // Begrenzen auf die ersten 5 Bilder für Telegram
    return images.slice(0, 5);
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Bilder: ${error.message}`);
    return [];
  }
}

/**
 * Speichert die aktuellen Ergebnisse in S3 und vergleicht sie mit den vorherigen
 */
async function saveAndCompareResults(listings, filterKey = '') {
  try {
    const today = new Date().toISOString().split('T')[0];
    // Verwende den filterKey im S3-Pfad, falls vorhanden
    const key = filterKey 
      ? `${RESULTS_PREFIX}${filterKey}/${today}.json`
      : `${RESULTS_PREFIX}${today}.json`;
    
    // Stelle sicher, dass jede Anzeige eine eindeutige Ad-ID hat
    const processedListings = listings.map(listing => {
      // Extrahiere die Ad-ID aus der URL, falls noch nicht vorhanden
      if (!listing.id) {
        const adId = extractAdId(listing.url);
        if (adId) {
          listing.id = adId;
        }
      }
      return listing;
    });
    
    // Aktuelle Ergebnisse
    const currentResults = {
      timestamp: new Date().toISOString(),
      listings: processedListings
    };
    
    // Vorherige Ergebnisse laden
    let previousResults = { listings: [] };
    let isFirstRun = false;
    let previousFileFound = false;
    
    try {
      // Suche nach der neuesten Datei im S3 Bucket mit dem gleichen Filter
      const filterPrefix = filterKey ? `${RESULTS_PREFIX}${filterKey}/` : RESULTS_PREFIX;
      
      const listParams = {
        Bucket: S3_BUCKET_NAME,
        Prefix: filterPrefix
      };
      
      console.log(`Suche nach vorherigen Ergebnissen mit Prefix: ${filterPrefix}`);
      const listedObjects = await s3.listObjectsV2(listParams).promise();
      
      if (listedObjects.Contents.length === 0) {
        // Keine vorherigen Dateien gefunden
        console.log('S3-Bucket ist leer oder enthält keine Dateien mit diesem Filter. Dies ist der erste Lauf.');
        isFirstRun = true;
      } else {
        // Sortiere Dateien nach LastModified (neueste zuerst)
        const sortedObjects = [...listedObjects.Contents].sort((a, b) => 
          new Date(b.LastModified) - new Date(a.LastModified)
        );
        
        // Aktuelle Datei überspringen, falls sie bereits existiert
        let latestFile = sortedObjects[0];
        if (latestFile.Key === key && sortedObjects.length > 1) {
          latestFile = sortedObjects[1]; // Nimm die zweitneueste, wenn die neueste die aktuelle ist
        }
        
        if (latestFile && latestFile.Key !== key) {
          // Lade die Daten aus der letzten Datei
          const previousData = await s3.getObject({
            Bucket: S3_BUCKET_NAME,
            Key: latestFile.Key
          }).promise();
          
          if (previousData && previousData.Body) {
            previousResults = JSON.parse(previousData.Body.toString());
            previousFileFound = true;
            console.log(`Vorherige Ergebnisse geladen aus ${latestFile.Key}: ${previousResults.listings.length} Anzeigen`);
          }
        } else {
          console.log('Keine geeignete vorherige Datei gefunden.');
        }
      }
    } catch (error) {
      console.error(`Fehler beim Laden vorheriger Ergebnisse: ${error.message}`);
      // Wenn keine vorherigen Ergebnisse gefunden wurden, verwenden wir eine leere Liste
      previousResults = { listings: [] };
    }
    
    // Wenn keine vorherige Datei gefunden wurde, noch einmal prüfen, ob es wirklich der erste Lauf ist
    if (!previousFileFound && !isFirstRun) {
      try {
        const checkParams = {
          Bucket: S3_BUCKET_NAME,
          Prefix: RESULTS_PREFIX
        };
        
        const allObjects = await s3.listObjectsV2(checkParams).promise();
        if (allObjects.Contents.length === 0) {
          console.log('Bestätigt: S3-Bucket ist komplett leer. Dies ist der erste Lauf.');
          isFirstRun = true;
        }
      } catch (checkError) {
        console.error(`Fehler bei der Überprüfung des S3-Buckets: ${checkError.message}`);
      }
    }
    
    // Aktuelle Ergebnisse speichern
    await s3.putObject({
      Bucket: S3_BUCKET_NAME,
      Key: key,
      Body: JSON.stringify(currentResults),
      ContentType: 'application/json'
    }).promise();
    console.log(`Aktuelle Ergebnisse gespeichert: s3://${S3_BUCKET_NAME}/${key}`);
    
    // Vergleiche aktuelle mit vorherigen Ergebnissen
    const currentIds = new Set(processedListings.map(listing => listing.id));
    const previousIds = new Set(previousResults.listings.map(listing => listing.id));
    
    // Bei leerem S3-Bucket (erster Lauf) keine Benachrichtigungen für neue Anzeigen
    let newIds = [];
    if (isFirstRun) {
      console.log('Erster Lauf: Alle Anzeigen werden als "bereits bekannt" markiert');
      newIds = [];
    } else if (previousResults.listings.length === 0) {
      // Wenn keine vorherigen Listings gefunden wurden, aber es nicht der erste Lauf ist,
      // behandeln wir es trotzdem als ersten Lauf für diesen Filter
      console.log('Keine vorherigen Listings für diesen Filter gefunden. Behandle als ersten Lauf für diesen Filter.');
      newIds = [];
    } else {
      // Normale Vergleichslogik: Neue IDs sind die, die nicht in den vorherigen Ergebnissen waren
      newIds = [...currentIds].filter(id => !previousIds.has(id));
      console.log(`Vergleiche ${currentIds.size} aktuelle mit ${previousIds.size} vorherigen IDs, ${newIds.length} neue gefunden`);
    }
    
    const removedIds = [...previousIds].filter(id => !currentIds.has(id));
    
    // Detaillierte Informationen sammeln
    const newListings = processedListings.filter(listing => newIds.includes(listing.id));
    const removedListings = previousResults.listings.filter(listing => removedIds.includes(listing.id));
    
    console.log(`Vergleichsergebnis: ${newListings.length} neue, ${removedListings.length} entfernte Anzeigen`);
    
    return {
      currentListings: processedListings,
      newListings,
      removedListings,
      isFirstRun
    };
  } catch (error) {
    console.error(`Fehler beim Speichern/Vergleichen der Ergebnisse: ${error.message}`);
    // Minimal-Ergebnis zurückgeben, falls etwas schief geht
    return {
      currentListings: processedListings || listings,
      newListings: [],
      removedListings: [],
      isFirstRun: false
    };
  }
}

/**
 * Generiert eine formatierte Telegram-Nachricht mit Listing-Informationen
 */
function generateTelegramMessage(changes, runId = '') {
  try {
    const dateStr = new Date().toISOString().replace('T', ' ').substring(0, 19);
    let message = `<b>Bazaraki Immobilien-Update (${dateStr})</b>\n\n`;
    message += `Run ID: <code>${runId}</code>\n\n`;
    
    // Zusammenfassung
    const total = changes.currentListings?.length || 0;
    const newCount = changes.newListings?.length || 0;
    const removedCount = changes.removedListings?.length || 0;
    
    message += `*Aktuelle Anzeigen:* ${total}\n`;
    message += `*Neue Anzeigen:* ${newCount}\n`;
    message += `*Entfernte Anzeigen:* ${removedCount}\n\n`;
    
    // Fehlermeldung, falls vorhanden
    if (changes.error) {
      message += `⚠️ *FEHLER:* ${changes.error}\n\n`;
    }
    
    // Warnung bei 0 Anzeigen
    if (total === 0) {
      message += `⚠️ *WARNUNG:* Keine Anzeigen gefunden. Mögliches Problem mit dem Scraper.\n\n`;
    }
    
    // Detaillierte Liste der neuen Anzeigen
    if (newCount > 0) {
      message += `*Neue Anzeigen:*\n`;
      changes.newListings.slice(0, 10).forEach((listing, i) => {
        const title = (listing.title || 'Keine Beschreibung').substring(0, 50);
        const url = listing.url || '#';
        const price = listing.price?.text || 'Preis auf Anfrage';
        const location = listing.location || 'Ort unbekannt';
        
        const details = [];
        if (listing.details?.bedrooms) details.push(`${listing.details.bedrooms} BR`);
        if (listing.details?.bathrooms) details.push(`${listing.details.bathrooms} BA`);
        if (listing.details?.area) details.push(`${listing.details.area} m²`);
        const detailsStr = details.length > 0 ? ` - ${details.join(', ')}` : '';
        
        message += `${i + 1}. [${title}](${url}) - ${price}${detailsStr}\n`;
        
        // Standort hinzufügen
        if (location && location !== 'Standort nicht angegeben') {
          message += `   📍 ${location}\n`;
        }
        
        // Bild-Link hinzufügen, wenn verfügbar
        if (listing.images && listing.images.length > 0) {
          // Direktes Bild-Tag für Telegram (HTML Format) verwenden, damit die Bilder direkt angezeigt werden
          message += `   📷 <a href="${listing.images[0]}">Foto</a>
`;
        }
        
        message += `\n`;
      });
      
      if (newCount > 10) {
        message += `_...und ${newCount - 10} weitere_\n\n`;
      }
    }
    
    // Entfernte Anzeigen (kurzgefasst)
    if (removedCount > 0) {
      message += `*Entfernte Anzeigen:*\n`;
      changes.removedListings.slice(0, 5).forEach((listing, i) => {
        const title = (listing.title || 'Keine Beschreibung').substring(0, 50);
        const price = listing.price?.text || '';
        message += `${i + 1}. ${title}${price ? ` - ${price}` : ''}\n`;
      });
      
      if (removedCount > 5) {
        message += `_...und ${removedCount - 5} weitere_\n`;
      }
    }
    
    return message;
  } catch (error) {
    console.error(`Fehler beim Generieren der Telegram-Nachricht: ${error.message}`);
    return `*Bazaraki Scraper Fehler*\n\nFehler beim Generieren der Nachricht: ${error.message}`;
  }
}

/**
 * Sendet eine Benachrichtigung über Telegram
 */
async function sendTelegramNotification(changes, force = false, runId = '') {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log('Telegram-Konfiguration fehlt. Keine Benachrichtigung gesendet.');
    return false;
  }
  
  const hasNewListings = changes.newListings?.length > 0;
  const hasRemovedListings = changes.removedListings?.length > 0;
  const hasError = !!changes.error;
  const isFirstRun = changes.isFirstRun === true;
  const hasChanges = hasNewListings || hasRemovedListings;
  
  if (isFirstRun && !force) {
    console.log('Erster Lauf: Alle Anzeigen wurden gespeichert, aber keine Benachrichtigung gesendet.');
    return true;
  }
  
  // Ab jetzt immer eine Nachricht senden, auch wenn keine Änderungen vorliegen
  console.log(`Sende Telegram-Benachrichtigung: ${hasNewListings ? changes.newListings.length + ' neue, ' : ''}${hasRemovedListings ? changes.removedListings.length + ' entfernte Anzeigen' : ''}${!hasChanges ? 'Keine Änderungen' : ''}${hasError ? ', Fehler aufgetreten' : ''}`);
  
  try {
    // Zuerst eine Zusammenfassungsnachricht senden
    const dateStr = new Date().toISOString().replace('T', ' ').substring(0, 19);
    let summaryMessage = `<b>Bazaraki Immobilien-Update (${dateStr})</b>\n\n`;
    summaryMessage += `Run ID: <code>${runId}</code>\n\n`;
    
    // Zusammenfassung
    const total = changes.currentListings?.length || 0;
    const newCount = changes.newListings?.length || 0;
    const removedCount = changes.removedListings?.length || 0;
    
    summaryMessage += `<b>Aktuelle Anzeigen:</b> ${total}\n`;
    summaryMessage += `<b>Neue Anzeigen:</b> ${newCount}\n`;
    summaryMessage += `<b>Entfernte Anzeigen:</b> ${removedCount}\n\n`;
    
    // Statusmeldung hinzufügen
    if (!hasChanges) {
      summaryMessage += `<b>Status:</b> ✅ Keine Veränderungen seit dem letzten Scan.\n\n`;
    } else {
      summaryMessage += `<b>Status:</b> ✨ Es wurden Veränderungen gefunden.\n\n`;
    }
    
    // Fehlermeldung, falls vorhanden
    if (hasError) {
      summaryMessage += `⚠️ <b>FEHLER:</b> ${changes.error}\n\n`;
    }
    
    // Warnung bei 0 Anzeigen
    if (total === 0) {
      summaryMessage += `⚠️ <b>WARNUNG:</b> Keine Anzeigen gefunden. Mögliches Problem mit dem Scraper.\n\n`;
    }
    
    // Zusammenfassungsnachricht senden
    await sendTelegramMessage(summaryMessage);
    
    // Jetzt einzelne Nachrichten für neue Anzeigen senden
    if (hasNewListings) {
      console.log(`Sende einzelne Nachrichten für ${changes.newListings.length} neue Anzeigen...`);
      
      for (let i = 0; i < changes.newListings.length; i++) {
        const listing = changes.newListings[i];
        await sendSingleListingMessage(listing, i+1, changes.newListings.length);
        
        // Pause zwischen Nachrichten um Rate-Limits zu vermeiden
        if (i < changes.newListings.length - 1) {
          await delay(500);
        }
      }
    }
    
    // Entfernte Anzeigen in einer Zusammenfassungsnachricht
    if (hasRemovedListings) {
      let removedMessage = `<b>Entfernte Anzeigen (${removedCount}):</b>\n`;
      
      changes.removedListings.forEach((listing, i) => {
        const title = (listing.title || 'Keine Beschreibung').substring(0, 50);
        const price = listing.price?.text || '';
        removedMessage += `${i + 1}. ${title}${price ? ` - ${price}` : ''}\n`;
      });
      
      await sendTelegramMessage(removedMessage);
    }
    
    console.log('Telegram-Benachrichtigungen erfolgreich gesendet');
    return true;
  } catch (error) {
    console.error(`Fehler beim Senden der Telegram-Benachrichtigung: ${error.message}`);
    return false;
  }
}

/**
 * Sendet eine einzelne Nachricht über Telegram
 */
async function sendTelegramMessage(text, parseMode = 'HTML', disablePreview = false) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    return false;
  }
  
  // Zu lange Nachrichten aufteilen (Telegram-Limit: 4096 Zeichen)
  const messageChunks = splitLongMessage(text, 4000);
  
  // Nachrichten senden
  for (let i = 0; i < messageChunks.length; i++) {
    const chunk = messageChunks[i];
    const messagePrefix = messageChunks.length > 1 ? `<b>Teil ${i+1}/${messageChunks.length}</b>\n\n` : '';
    const fullChunk = i === 0 ? chunk : messagePrefix + chunk;
    
    // Telegram API aufrufen
    const telegramUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const payload = {
      chat_id: TELEGRAM_CHAT_ID,
      text: fullChunk,
      parse_mode: parseMode,
      disable_web_page_preview: disablePreview
    };
    
    const response = await axios.post(telegramUrl, payload, {
      timeout: 10000 // 10 Sekunden Timeout
    });
    
    if (response.status !== 200 || !response.data.ok) {
      console.error(`Telegram API-Fehler: ${JSON.stringify(response.data)}`);
      return false;
    }
    
    // Pause zwischen Nachrichten
    if (i < messageChunks.length - 1) {
      await delay(1000);
    }
  }
  
  return true;
}

/**
 * Sendet eine einzelne Anzeige als Telegram-Nachricht
 */
async function sendSingleListingMessage(listing, index, total) {
  try {
    console.log(`Sende Einzelnachricht ${index}/${total} für Anzeige: ${listing.id}`);
    
    // Titel und URL der Anzeige
    const title = listing.title || 'Keine Beschreibung';
    const url = listing.url || '#';
    
    // Nachricht erstellen
    let message = `<b>Anzeige ${index}/${total}</b>: ${title}\n\n`;
    
    // Preis und Details hinzufügen
    if (listing.price && listing.price.text) {
      message += `<b>Preis:</b> ${listing.price.text}\n`;
    }
    
    // Standort hinzufügen
    if (listing.location && listing.location !== 'Standort nicht angegeben') {
      message += `<b>Standort:</b> ${listing.location}\n`;
    }
    
    // Details hinzufügen (Schlafzimmer, Badezimmer, Fläche)
    if (listing.details) {
      const details = [];
      if (listing.details.bedrooms) details.push(`${listing.details.bedrooms} Schlafzimmer`);
      if (listing.details.bathrooms) details.push(`${listing.details.bathrooms} Badezimmer`);
      if (listing.details.area) details.push(`${listing.details.area} m²`);
      
      if (details.length > 0) {
        message += `<b>Details:</b> ${details.join(', ')}\n`;
      }
    }
    
    // Beschreibung hinzufügen
    if (listing.description) {
      message += `\n<b>Beschreibung:</b>\n${listing.description.substring(0, 500)}`;
      if (listing.description.length > 500) {
        message += `... <a href="${url}">mehr lesen</a>`;
      }
      message += '\n';
    }
    
    // Link zur Anzeige hinzufügen
    message += `\n<a href="${url}">Anzeige auf Bazaraki ansehen</a>\n`;
    
    // Nachricht senden (ohne Vorschau, da wir eigene Bilder senden)
    await sendTelegramMessage(message, 'HTML', true);
    
    // Wenn Bilder vorhanden sind, sende das erste Bild direkt
    if (listing.images && listing.images.length > 0) {
      const imageUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendPhoto`;
      
      try {
        const imagePayload = {
          chat_id: TELEGRAM_CHAT_ID,
          photo: listing.images[0],
          caption: `${title} - ${listing.price?.text || ''}`,
          parse_mode: 'HTML'
        };
        
        await axios.post(imageUrl, imagePayload, {
          timeout: 15000 // 15 Sekunden Timeout für Bilder
        });
        
        console.log(`Bild für Anzeige ${listing.id} erfolgreich gesendet`);
      } catch (imageError) {
        console.error(`Fehler beim Senden des Bildes für Anzeige ${listing.id}: ${imageError.message}`);
      }
    }
    
    return true;
  } catch (error) {
    console.error(`Fehler beim Senden der Einzelnachricht für Anzeige ${listing.id}: ${error.message}`);
    return false;
  }
}

/**
 * Teilt eine lange Nachricht in mehrere Teile auf
 */
function splitLongMessage(message, maxLength) {
  if (message.length <= maxLength) {
    return [message];
  }
  
  const chunks = [];
  let currentChunk = '';
  const lines = message.split('\n');
  
  for (const line of lines) {
    if (currentChunk.length + line.length + 1 > maxLength) {
      chunks.push(currentChunk);
      currentChunk = line;
    } else {
      currentChunk += (currentChunk ? '\n' : '') + line;
    }
  }
  
  if (currentChunk) {
    chunks.push(currentChunk);
  }
  
  return chunks;
}

/**
 * Helfer-Funktion für Verzögerungen
 */
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Lambda-Handler-Funktion
 */
async function handler(event, context) {
  console.log('Bazaraki Scraper gestartet...');
  console.log('Event:', JSON.stringify(event, null, 2));
  
  // Eindeutige Run-ID generieren
  const runId = `run-${new Date().toISOString().replace(/[^0-9]/g, '').substring(0, 14)}`;
  console.log(`Run ID: ${runId}`);
  
  try {
    // Benutzerdefinierte Filter aus dem Event-Objekt extrahieren
    const customFilters = {};
    
    // Preisfilter aus dem Event-Objekt oder Umgebungsvariable
    if (event?.price_max) {
      customFilters.price_max = event.price_max;
      console.log(`Benutzerdefinierter Preisfilter: ${event.price_max}€`);
    }
    
    // Weitere benutzerdefinierte Filter
    if (event?.propertyType) customFilters.propertyType = event.propertyType;
    if (event?.district) customFilters.district = event.district;
    if (event?.radius) customFilters.radius = event.radius;
    if (event?.bedrooms) customFilters.bedrooms = event.bedrooms;
    
    // Schlüssel für S3-Ergebnisse basierend auf Filtern generieren
    const filterKey = `price_max_${customFilters.price_max || DEFAULT_PRICE_MAX}`;
    console.log(`Verwende Filter-Schlüssel für S3-Ergebnisse: ${filterKey}`);
    
    // Hauptaufgabe ausführen mit benutzerdefinierten Filtern
    const listings = await scrapeListings(customFilters);
    console.log(`${listings.length} Anzeigen erfolgreich gescrapt`);
    
    // Ergebnisse speichern und mit vorherigen vergleichen
    const results = await saveAndCompareResults(listings, filterKey);
    
    // Benachrichtigung senden (nur wenn Änderungen vorhanden oder force=true)
    const force = event?.force === true;
    await sendTelegramNotification(results, force, runId);
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        runId,
        timestamp: new Date().toISOString(),
        totalListings: listings.length,
        newListings: results.newListings.length,
        removedListings: results.removedListings.length,
        filters: { ...DEFAULT_FILTERS, ...customFilters },
        success: true
      })
    };
  } catch (error) {
    console.error(`Fehler beim Ausführen des Bazaraki Scrapers: ${error.message}`);
    
    // Fehlermeldung per Telegram senden
    await sendTelegramNotification({
      currentListings: [],
      newListings: [],
      removedListings: [],
      error: error.message
    }, true, runId);
    
    return {
      statusCode: 500,
      body: JSON.stringify({
        runId,
        timestamp: new Date().toISOString(),
        error: error.message,
        success: false
      })
    };
  }
}

/**
 * Sendet eine Testnachricht über Telegram
 */
async function sendTestTelegramMessage() {
  try {
    console.log('Sende Telegram-Testnachricht...');
    
    // Beispiel-Anzeige erstellen
    const testListing = {
      id: '12345678',
      title: 'Schöne 2-Schlafzimmer Wohnung in Paphos mit Meerblick',
      url: 'https://www.bazaraki.com/adv/12345678_2-bedroom-apartment-to-rent/',
      price: {
        text: '€950',
        value: 950
      },
      location: 'Paphos, Kato Paphos',
      details: {
        bedrooms: '2',
        bathrooms: '1',
        area: '85 m²'
      },
      description: 'Willkommen in dieser wunderschönen, renovierten 2-Schlafzimmer-Wohnung in Kato Paphos. Die Wohnung bietet einen atemberaubenden Meerblick, ist voll möbliert und verfügt über eine moderne Küche, klimatisierte Räume und einen großen Balkon. Der Komplex bietet einen gemeinschaftlichen Pool und liegt nur 5 Gehminuten vom Strand und lokalen Annehmlichkeiten entfernt. Ideal für Langzeitmiete.',
      images: [
        'https://cdn1.bazaraki.com/media/cache1/eb/81/eb816799e320f553e47201461204cd31.webp'
      ],
      scrapedAt: new Date().toISOString()
    };
    
    // Testnachricht mit der Beispiel-Anzeige senden
    await sendSingleListingMessage(testListing, 1, 1);
    
    console.log('Telegram-Testnachricht erfolgreich gesendet');
    return true;
  } catch (error) {
    console.error(`Fehler beim Senden der Telegram-Testnachricht: ${error.message}`);
    return false;
  }
}

// Export für AWS Lambda
exports.handler = handler;

// Lokales Testen, wenn Skript direkt ausgeführt wird
if (require.main === module) {
  (async () => {
    try {
      console.log('Lokaler Testmodus gestartet');
      
      // Setze einige Standard-Umgebungsvariablen für lokales Testen
      // Diese sollten in einer echten Umgebung als Umgebungsvariablen gesetzt werden
      if (!process.env.S3_BUCKET_NAME) {
        console.log('HINWEIS: Umgebungsvariablen nicht gefunden. Verwende Standard-Testwerte.');
        process.env.S3_BUCKET_NAME = 'test-bucket';
        process.env.RESULTS_PREFIX = 'bazaraki/results/';
        
        // DEBUG_MODE auf 'true' erzeugt Test-Daten
        process.env.DEBUG_MODE = 'false';
      }
      
      console.log('Starte lokalen Test des Bazaraki-Scrapers...');
      
      // Kommandozeilenargumente auswerten
      const args = process.argv.slice(2);
      
      if (args.includes('--test-telegram')) {
        // Nur Telegram-Test ausführen
        await sendTestTelegramMessage();
      } else {
        // Normalen Scraper-Test ausführen
        await handler({
          // Zum Erzwingen von Telegram-Benachrichtigungen auch bei erstem Lauf:
          force: args.includes('--force')
        }, {});
      }
      
      console.log('Test erfolgreich abgeschlossen');
    } catch (error) {
      console.error(`Test fehlgeschlagen: ${error.message}`);
    }
    // Hier Ihre eigenen Telegram-Credentials einfügen
    // Ohne diese Werte kann keine Nachricht gesendet werden
    if (!process.env.TELEGRAM_BOT_TOKEN) {
      process.env.TELEGRAM_BOT_TOKEN = '6922016071:AAEsNogGzmkLwxXAXfbWP3h09-XY8CbJ6qE';
      process.env.TELEGRAM_CHAT_ID = '1197930445';
    }
  })();
}
