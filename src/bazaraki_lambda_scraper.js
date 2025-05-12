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
  // Mock S3 für lokale Tests mit Persistenz
  const fs = require('fs');
  const path = require('path');
  
  // Lokaler Speicherort für S3-Daten
  const LOCAL_STORAGE_DIR = path.join(process.cwd(), 'local_s3_storage');
  const STORAGE_FILE = path.join(LOCAL_STORAGE_DIR, 's3_storage.json');
  
  // Stelle sicher, dass das Verzeichnis existiert
  if (!fs.existsSync(LOCAL_STORAGE_DIR)) {
    fs.mkdirSync(LOCAL_STORAGE_DIR, { recursive: true });
    console.log(`[S3-MOCK] Verzeichnis erstellt: ${LOCAL_STORAGE_DIR}`);
  }
  
  // Lade vorhandene Daten oder initialisiere ein leeres Objekt
  let localS3Storage = {};
  try {
    if (fs.existsSync(STORAGE_FILE)) {
      const data = fs.readFileSync(STORAGE_FILE, 'utf8');
      localS3Storage = JSON.parse(data);
      console.log(`[S3-MOCK] Daten geladen aus: ${STORAGE_FILE}`);
      console.log(`[S3-MOCK] Anzahl gespeicherter Objekte: ${Object.keys(localS3Storage).length}`);
    }
  } catch (error) {
    console.error(`[S3-MOCK] Fehler beim Laden der Daten: ${error.message}`);
    localS3Storage = {};
  }
  
  // Funktion zum Speichern des aktuellen Zustands
  const saveStorage = () => {
    try {
      fs.writeFileSync(STORAGE_FILE, JSON.stringify(localS3Storage, null, 2));
      console.log(`[S3-MOCK] Daten gespeichert in: ${STORAGE_FILE}`);
    } catch (error) {
      console.error(`[S3-MOCK] Fehler beim Speichern der Daten: ${error.message}`);
    }
  };
  
  s3 = {
    getObject: (params) => ({
      promise: () => {
        return new Promise((resolve, reject) => {
          const key = `${params.Bucket}/${params.Key}`;
          if (localS3Storage[key]) {
            console.log(`[S3-MOCK] Datei geladen: ${key}`);
            resolve({
              Body: Buffer.from(localS3Storage[key])
            });
          } else {
            const error = new Error('Die Datei existiert nicht.');
            error.code = 'NoSuchKey';
            console.log(`[S3-MOCK] Datei nicht gefunden: ${key}`);
            reject(error);
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
          saveStorage(); // Persistentes Speichern
          resolve({ ETag: 'mock-etag' });
        });
      }
    }),
    listObjectsV2: (params) => ({
      promise: () => {
        return new Promise((resolve) => {
          const prefix = params.Prefix || '';
          const contents = [];
          
          // Alle Schlüssel durchgehen, die mit dem Präfix beginnen
          Object.keys(localS3Storage).forEach(fullKey => {
            // Format: "bucket/key" aufteilen
            const parts = fullKey.split('/');
            const bucket = parts[0];
            const key = parts.slice(1).join('/');
            
            if (bucket === params.Bucket && key.startsWith(prefix)) {
              contents.push({
                Key: key,
                Size: localS3Storage[fullKey].length,
                LastModified: new Date()
              });
            }
          });
          
          console.log(`[S3-MOCK] Gefundene Dateien für Präfix '${prefix}': ${contents.length}`);
          resolve({ Contents: contents });
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
 * Hauptfunktion: Scannt bazaraki.com nach Immobilienanzeigen mit optimierter ID-Prüfung
 */
/**
 * Verbesserte Scraping-Funktion mit zweistufigem Ansatz:
 * 1. Schneller ID-Scan über alle Seiten
 * 2. Detailliertes Scraping nur für neue Anzeigen
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
    
    // ===== OPTIMIERUNG: Frühes Laden vorheriger Ergebnisse =====
    console.log('Lade vorherige Ergebnisse aus S3...');
    const startTime = Date.now();
    const { previousResults, isFirstRun } = await loadPreviousResults(filterKey);
    const previousIds = new Set(previousResults.listings.map(listing => listing.id));
    const previousIdsByType = {};
    const previousListingsById = {};
    
    // Index erstellen, um schnell auf vorherige Anzeigen zuzugreifen
    previousResults.listings.forEach(listing => {
      previousListingsById[listing.id] = listing;
      
      // Nach Immobilientyp gruppieren (falls verfügbar)
      const type = listing.propertyType || 'unknown';
      if (!previousIdsByType[type]) previousIdsByType[type] = new Set();
      previousIdsByType[type].add(listing.id);
    });
    
    console.log(`${previousIds.size} vorherige Anzeigen-IDs geladen (${Date.now() - startTime}ms)`);    
    console.log(`Anzeigen nach Typ: ${Object.keys(previousIdsByType).map(type => `${type}: ${previousIdsByType[type]?.size || 0}`).join(', ')}`);
    
    // Bestimme, welche Immobilientypen gescrapt werden sollen
    let propertyTypes = [];
    if (filters.propertyTypes && Array.isArray(filters.propertyTypes)) {
      propertyTypes = filters.propertyTypes;
    } else if (filters.propertyType) {
      propertyTypes = [filters.propertyType];
    } else {
      propertyTypes = ['apartments-flats', 'houses'];
    }
    
    console.log(`Scrape folgende Immobilientypen: ${propertyTypes.join(', ')}`);
    
    // Zu erstellende Listen für die Ergebnisse
    const newListings = [];
    const allCurrentListings = [];
    const allProcessedIds = new Set();
    
    // Übersichten für Statistiken
    const statsByType = {};
    propertyTypes.forEach(type => {
      statsByType[type] = { total: 0, new: 0, unchanged: 0, removed: 0, processingTime: 0 };
    });
    
    // Jeden Immobilientyp nacheinander komplett verarbeiten
    for (const propertyType of propertyTypes) {
      const typeStartTime = Date.now();
      console.log(`\n== Beginne optimierte Verarbeitung für Immobilientyp: ${propertyType} ==\n`);
      
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
      
      // ===== OPTIMIERUNG: Schneller Ad-ID-Scan mit Überspringen bekannter IDs =====
      console.log(`Beginne optimierten ID-Scan für ${propertyType}...`);
      const scanStartTime = Date.now();
      
      // Vorherige IDs an die URL-Extraktionsfunktion übergeben für frühe Optimierung
      // Fügt skipKnown=true hinzu, um bekannte IDs direkt zu überspringen
      const urlResults = await extractListingUrls(searchUrl, 10, previousIds, true);
      
      // Aktuelle IDs und URLs verarbeiten
      const currentTypeIds = new Set(); // Aktuelle IDs
      const newIds = []; // Neue IDs (nicht in vorherigen)
      const newListingUrls = []; // URLs für neue Anzeigen
      const unchangedIds = new Set(); // Unveränderte IDs (bekannte IDs)
      
      // 1. Entfernte Anzeigen werden automatisch erkannt (nicht in aktueller Menge)
      
      // 2. Neue Anzeigen verarbeiten (wurden bereits beim URL-Scan vorgefiltert)
      for (const url of urlResults.newUrls) {
        // Extrahiere ID aus URL oder verwende bereits extrahierte IDs aus Map
        const id = urlResults.idsByUrlMap[url] || extractAdId(url);
        
        if (!id) {
          console.warn(`Konnte keine Ad-ID für URL extrahieren: ${url}`);
          continue;
        }
        
        // ID für späteren globalen Vergleich speichern
        allProcessedIds.add(id);
        currentTypeIds.add(id);
        
        // Neue IDs sind schon gefiltert dank skipKnown=true
        newIds.push(id);
        newListingUrls.push({ url, id });
      }
      
      // 3. IDs der übersprungenen Anzeigen als "unverändert" markieren
      for (const id of urlResults.skippedIds) {
        unchangedIds.add(id);
        currentTypeIds.add(id);
        allProcessedIds.add(id);
      }
      
      // Erfolgsstatistik ausgeben
      console.log(`Optimierter Scan hat ${urlResults.savedRequestsCount} Anfragen eingespart`);
      console.log(`Geschätzte Zeitersparnis: ~${urlResults.estimatedTimeSaved.toFixed(1)}s`);
      
      
      // Ermittle entfernte IDs (nur für diesen Immobilientyp)
      const removedIds = [];
      if (previousIdsByType[propertyType]) {
        previousIdsByType[propertyType].forEach(id => {
          if (!currentTypeIds.has(id)) {
            removedIds.push(id);
          }
        });
      }
      
      // Speichere Statistiken für Reports
      statsByType[propertyType].total = currentTypeIds.size;
      statsByType[propertyType].new = newIds.length;
      statsByType[propertyType].unchanged = unchangedIds.size;
      statsByType[propertyType].removed = removedIds.length;
      
      // Schneller ID-Scan abgeschlossen
      console.log(`Schneller ID-Scan für ${propertyType}: ${Date.now() - scanStartTime}ms`);
      console.log(`Ergebnis: ${currentTypeIds.size} Anzeigen gefunden (${newIds.length} neu, ${unchangedIds.size} unverändert, ${removedIds.length} entfernt)`);
      
      // Übersicht ausgeben, falls viele IDs gefunden wurden
      if (newIds.length > 0) {
        console.log(`✨ ${newIds.length} neue Anzeigen für ${propertyType} gefunden zur detaillierten Verarbeitung`);
      }
      
      if (removedIds.length > 0) {
        console.log(`🚫 ${removedIds.length} entfernte Anzeigen für ${propertyType} identifiziert`);
        // Optional: Hier könnten wir Details zu den entfernten Anzeigen anzeigen
      }
      
      // ===== OPTIMIERUNG: Unveränderte Anzeigen direkt übernehmen =====
      // Für unveränderte Anzeigen die vorherigen Daten wiederverwenden
      for (const id of unchangedIds) {
        if (previousListingsById[id]) {
          allCurrentListings.push(previousListingsById[id]);
        }
      }
      
      // Detaillierte Informationen für neue Anzeigen abrufen
      if (newIds.length > 0) {
        console.log(`Hole detaillierte Informationen für ${newIds.length} neue ${propertyType}-Anzeigen...`);
        const detailsStartTime = Date.now();
        
        // Detaillierte Daten für jede neue Anzeige extrahieren
        for (let i = 0; i < newListingUrls.length; i++) {
          const { url, id } = newListingUrls[i];
          
          console.log(`Verarbeite neue ${propertyType}-Anzeige ${i+1}/${newListingUrls.length}: ${id}`);
          
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
        
        console.log(`Detaillierte Verarbeitung für ${propertyType} abgeschlossen: ${Date.now() - detailsStartTime}ms`);
      } else {
        console.log(`Keine neuen ${propertyType}-Anzeigen gefunden, überspringe detailliertes Scraping für diesen Typ.`);
      }
      
      // Gesamte Verarbeitungszeit für diesen Typ
      statsByType[propertyType].processingTime = Date.now() - typeStartTime;
      console.log(`\n== Verarbeitung für Immobilientyp ${propertyType} abgeschlossen (${statsByType[propertyType].processingTime}ms) ==\n`);
      
      // Kurze Pause zwischen den Immobilientypen
      await delay(1000);
    }
    
    // Globaler Vergleich für entfernte Anzeigen - bereits verarbeitet beim ID-Scan
    const removedIds = [...previousIds].filter(id => !allProcessedIds.has(id));
    
    // Zusammenfassung der Ergebnisse generieren
    const totalStats = {
      total: allCurrentListings.length,
      new: newListings.length,
      removed: removedIds.length,
      unchanged: allCurrentListings.length - newListings.length,
      processingTime: Date.now() - startTime
    };
    
    // Detaillierte Statistik für jeden Typ anzeigen
    console.log(`\n==== ZUSAMMENFASSUNG DER ERGEBNISSE ====`);
    console.log(`Gesamtverarbeitungszeit: ${totalStats.processingTime}ms`);
    console.log(`\nStatistik pro Immobilientyp:`);
    
    for (const type of propertyTypes) {
      const stats = statsByType[type];
      console.log(`- ${type}: ${stats.total} Anzeigen (${stats.new} neu, ${stats.unchanged} unverändert, ${stats.removed} entfernt) in ${stats.processingTime}ms`);
    }
    
    // Gesamtstatistik anzeigen
    console.log(`\nGESAMTERGEBNIS:`);
    console.log(`${totalStats.total} aktuelle Anzeigen (${totalStats.new} neu, ${totalStats.unchanged} unverändert, ${totalStats.removed} entfernt)`);
    
    // Bei Erstausführung oder vielen neuen Anzeigen
    if (isFirstRun) {
      console.log(`Erste Ausführung: Alle ${totalStats.total} Anzeigen wurden als neu betrachtet.`);
    } else if (totalStats.new > 0) {
      console.log(`\n✨ ${totalStats.new} NEUE ANZEIGEN GEFUNDEN:`);
      newListings.slice(0, 10).forEach((listing, i) => {
        console.log(`  ${i+1}. ${listing.title?.substring(0, 50) || 'Keine Beschreibung'} (ID: ${listing.id})`);
      });
      if (newListings.length > 10) {
        console.log(`  ... und ${newListings.length - 10} weitere neue Anzeigen`);
      }
    }
    
    if (totalStats.removed > 0) {
      console.log(`\n🚫 ${totalStats.removed} ENTFERNTE ANZEIGEN:`);
      const removedListings = [];
      // Finde Details zu den entfernten Anzeigen
      for (const id of removedIds) {
        if (previousListingsById[id]) {
          removedListings.push(previousListingsById[id]);
        }
      }
      
      // Zeige Details zu den ersten 10 entfernten Anzeigen
      removedListings.slice(0, 10).forEach((listing, i) => {
        console.log(`  ${i+1}. ${listing.title?.substring(0, 50) || 'Keine Beschreibung'} (ID: ${listing.id})`);
      });
      if (removedListings.length > 10) {
        console.log(`  ... und ${removedListings.length - 10} weitere entfernte Anzeigen`);
      }
    }
    
    // Optimierungsstatistik anzeigen
    const savedRequests = totalStats.unchanged;
    const estimatedTimeSaved = savedRequests * 1.5; // ~1,5 Sekunden pro Anfrage gespart
    console.log(`\n✅ OPTIMIERUNGSBERICHT:`);
    console.log(`${savedRequests} Anfragen eingespart durch Wiederverwendung vorhandener Daten`);
    console.log(`Geschätzte Zeitersparnis: ~${estimatedTimeSaved.toFixed(1)} Sekunden (${(estimatedTimeSaved / 60).toFixed(1)} Minuten)`);
    console.log(`====================================\n`);
    
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
 * Extrahiert eine Ad-ID aus einer URL mit verbesserter Robustheit
 * 
 * @param {string} url - Die URL oder das Listing-Objekt, aus dem die ID extrahiert werden soll
 * @returns {string|null} - Die extrahierte ID als String oder null, wenn keine ID gefunden wurde
 */
function extractAdId(url) {
  if (!url) return null;
  
  // Falls ein Objekt übergeben wurde, versuche die URL zu extrahieren
  if (typeof url === 'object') {
    if (url.id) return String(url.id).trim();
    url = url.url || '';
  }
  
  if (typeof url !== 'string') return null;
  
  try {
    // Format 1: https://www.bazaraki.com/adv/123456_description-here/
    // Format 2: https://www.bazaraki.com/adv/123456/
    // Format 3: https://bazaraki.com/adv/123456
    // Format 4: /adv/123456_description
    
    // Verschiedene Regex-Muster für unterschiedliche URL-Formate
    const patterns = [
      /\/adv\/([0-9]+)(?:_|\/|$)/, // Standard-Format mit _ oder / nach der ID
      /\/adv\/([0-9]+)/, // Einfaches Format ohne Trenner
      /bazaraki\.com\/adv\/([0-9]+)/ // Vollständige Domain mit ID
    ];
    
    for (const pattern of patterns) {
      const match = url.match(pattern);
      if (match && match[1]) {
        // Stelle sicher, dass die ID ein String ist und keine führenden Nullen hat
        return String(parseInt(match[1], 10));
      }
    }
    
    // Fallback: Versuche, irgendeine Zahl zu finden
    const fallbackMatch = url.match(/\/(\d+)/);
    if (fallbackMatch) {
      return String(parseInt(fallbackMatch[1], 10));
    }
    
    return null;
  } catch (error) {
    console.error(`Fehler beim Extrahieren der ID aus URL ${url}: ${error.message}`);
    return null;
  }
}

/**
 * Extrahiert detaillierte Informationen für eine einzelne Anzeige
 * 
 * @param {string} url - URL der Anzeige, deren Details abgerufen werden sollen
 * @returns {Object} - Detaillierte Informationen zur Anzeige
 */
async function extractListingDetails(url) {
  try {
    console.log(`Lade Details für Anzeige: ${url}`);
    
    // Anzeigenseite laden mit höherem Timeout
    const response = await axios.get(url, {
      headers: DEFAULT_HEADERS,
      timeout: 15000 // 15 Sekunden Timeout
    });
    
    // HTML parsen
    const dom = new JSDOM(response.data);
    const document = dom.window.document;
    
    // Anzeigen-ID aus URL extrahieren
    const adId = extractAdId(url) || 'unknown';
    
    // Titel extrahieren - verbesserte Selektoren
    const titleElement = document.querySelector('h1.announcement-title, h1.title-announcement, .adv-title, .title');
    const title = titleElement ? titleElement.textContent.trim() : 'Keine Beschreibung';
    
    // Meta-Daten aus OpenGraph-Tags extrahieren
    const ogDescription = document.querySelector('meta[property="og:description"]');
    const metaDescription = ogDescription ? ogDescription.getAttribute('content') : '';
    
    // Berechnet "sauberen" Titel ohne Preis und andere Zahlen
    const cleanTitle = title.replace(/\d+[\.,]?\d*\s*(?:€|EUR|\$)/g, '').trim();

    // Verschiedene Informationen extrahieren mit verbesserten Methoden
    const price = extractPrice(document);
    const location = extractLocation(document);
    const description = extractDescription(document) || metaDescription;
    const propertyDetails = extractPropertyDetails(document);
    const images = extractImages(document);
    
    // Ergebnis zusammenstellen
    return {
      id: adId,
      url,
      title,
      cleanTitle,
      price,
      location,
      description,
      images,
      ...propertyDetails,
      extractedAt: new Date().toISOString()
    };
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Anzeigendetails für ${url}: ${error.message}`);
    return {
      id: extractAdId(url) || `unknown-${Date.now()}`,
      url,
      error: error.message,
      extractedAt: new Date().toISOString()
    };
  }
}
/**
 * Extrahiert den Preis aus der Anzeigenseite
 * 
 * @param {Document} document - Das DOM-Dokument der Anzeigenseite
 * @returns {object} - Preisinformationen
 */
function extractPrice(document) {
  try {
    // Versuche verschiedene Selektoren für den Preis
    const priceSelectors = [
      '.announcement-price', '.announcement__price', '.price-val', 
      '[itemprop="price"]', '.adv-price', '.price-container strong'
    ];
    
    let priceText = '';
    let priceElement = null;
    
    for (const selector of priceSelectors) {
      priceElement = document.querySelector(selector);
      if (priceElement) {
        priceText = priceElement.textContent.trim();
        break;
      }
    }
    
    if (!priceText) return { value: 0, currency: 'EUR', text: 'Preis nicht angegeben' };
    
    // Normalisiere den Preis
    priceText = priceText.replace(/\s+/g, ' ').trim();
    
    // Extrahiere Zahl und Währung
    const numericValue = priceText.replace(/[^0-9,.]/g, '').replace(/,/g, '.').replace(/[.](?=.*[.])/g, '');
    const value = parseFloat(numericValue) || 0;
    
    // Währung erkennen
    let currency = 'EUR';
    if (priceText.includes('$')) currency = 'USD';
    
    return {
      value,
      currency,
      text: priceText,
      normalized: `${value} ${currency}`
    };
  } catch (error) {
    console.error(`Fehler beim Extrahieren des Preises: ${error.message}`);
    return { value: 0, currency: 'EUR', text: 'Fehler bei Preisermittlung' };
  }
}
async function extractListingDetails(url) {
  try {
    console.log(`Lade Details für Anzeige: ${url}`);
    
    // Anzeigenseite laden mit höherem Timeout
    const response = await axios.get(url, {
      headers: DEFAULT_HEADERS,
      timeout: 15000 // 15 Sekunden Timeout
    });
    
    // HTML parsen
    const dom = new JSDOM(response.data);
    const document = dom.window.document;
    
    // Anzeigen-ID aus URL extrahieren
    const adId = extractAdId(url) || 'unknown';
    
    // Titel extrahieren - verbesserte Selektoren
    const titleElement = document.querySelector('h1.announcement-title, h1.title-announcement, .adv-title, .title');
    const title = titleElement ? titleElement.textContent.trim() : 'Keine Beschreibung';
    
    // Meta-Daten aus OpenGraph-Tags extrahieren
    const ogDescription = document.querySelector('meta[property="og:description"]');
    const metaDescription = ogDescription ? ogDescription.getAttribute('content') : '';
    
    // Berechnet "sauberen" Titel ohne Preis und andere Zahlen
    const cleanTitle = title.replace(/\d+[\.,]?\d*\s*(?:€|EUR|\$)/g, '').trim();

    // Verschiedene Informationen extrahieren mit verbesserten Methoden
    const price = extractPrice(document);
    const location = extractLocation(document);
    const description = extractDescription(document) || metaDescription;
    const propertyDetails = extractPropertyDetails(document);
    const images = extractImages(document);
    
    // Zusätzliche Charakteristiken aus der neuen HTML-Struktur extrahieren
    const characteristics = {};
    const charElements = document.querySelectorAll('.announcement-characteristics .chars-column li');
    
    charElements.forEach(charElement => {
      const keyElement = charElement.querySelector('.key-chars');
      const valueElement = charElement.querySelector('.value-chars');
      
      if (keyElement && valueElement) {
        const key = keyElement.textContent.trim().replace(/:$/, '');
        const value = valueElement.textContent.trim();
        
        // Schlüssel in kebab-case konvertieren für konsistentes Format
        const keyName = key.toLowerCase()
          .replace(/\s+/g, '-')
          .replace(/[^a-z0-9-]/g, '');
        
        characteristics[keyName] = value;
      }
    });
    
    // Baudatum/Jahr extrahieren, falls vorhanden
    let constructionYear = '';
    if (characteristics['construction-year']) {
      constructionYear = characteristics['construction-year'];
    }
    
    // Einrichtungsart extrahieren (möbliert, teilmöbliert, usw.)
    let furnishing = '';
    if (characteristics['furnishing']) {
      furnishing = characteristics['furnishing'];
    }
    
    // Haustiere erlaubt
    let petsAllowed = false;
    if (characteristics['pets'] && characteristics['pets'].toLowerCase().includes('allowed')) {
      petsAllowed = true;
    }
    
    // Grundstücksfläche (falls vorhanden)
    let plotArea = '';
    if (characteristics['plot-area']) {
      plotArea = characteristics['plot-area'];
    }
    
    // Status der Immobilie (neu, gebraucht, etc.)
    let propertyStatus = '';
    if (characteristics['status']) {
      propertyStatus = characteristics['status'];
    }
    
    // Verfügbarkeit der Immobilie
    let availability = '';
    if (characteristics['availability']) {
      availability = characteristics['availability'];
    }
    
    // Kombinierte Details mit allen verfügbaren Informationen
    const details = {
      id: adId,
      title: cleanTitle,
      fullTitle: title,
      price,
      location,
      description,
      details: propertyDetails,
      images,
      characteristics,
      constructionYear,
      furnishing,
      petsAllowed,
      plotArea,
      propertyStatus,
      availability,
      url,
      scrapedAt: new Date().toISOString()
    };
    
    return details;
  } catch (error) {
    console.error(`Fehler beim Extrahieren der Anzeigendetails für ${url}: ${error.message}`);
    return {
      title: 'Fehler beim Laden der Anzeige',
      price: { text: 'Unbekannt', value: 0 },
      details: {},
      description: '',
      images: [],
      id: extractAdId(url) || 'error',
      url,
      error: error.message,
      scrapedAt: new Date().toISOString()
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
 * Extrahiert alle Anzeigen-URLs von einer Suchseite und folgt den Paginierungslinks
 * Mit Optimierung zum frühen Überspringen bereits bekannter IDs
 * 
 * @param {string} url - Die Such-URL
 * @param {number} maxPages - Maximale Anzahl der zu durchsuchenden Seiten
 * @param {Set<string>} [knownIds=null] - Optional: Set mit bereits bekannten IDs zum Überspringen
 * @param {boolean} [skipKnown=false] - Ob bekannte IDs übersprungen werden sollen
 * @returns {Object} - Ergebnisobjekt mit URLs und Metadaten
 */
async function extractListingUrls(url, maxPages = 10, knownIds = null, skipKnown = false) {
  try {
    // Performance-Messung starten
    const startTime = Date.now();
    
    // Ergebnisstruktur initialisieren
    const result = {
      allUrls: [],                // Alle gefundenen URLs
      newUrls: [],                // Nur URLs, die nicht bekannt sind
      skippedIds: new Set(),      // IDs, die übersprungen wurden
      urlsByIdMap: {},            // Mapping ID -> URL
      idsByUrlMap: {},            // Mapping URL -> ID
      pagesCrawled: 0,            // Anzahl durchsuchter Seiten
      totalFound: 0,              // Gesamtzahl gefundener Anzeigen
      skippedCount: 0,            // Anzahl übersprungener Anzeigen
      savedRequestsCount: 0,      // Anzahl eingesparter Anfragen
      estimatedTimeSaved: 0       // Geschätzte eingesparte Zeit in Sekunden
    };
    
    console.log(`Starte URL-Extraktion von ${url} ${knownIds ? `mit ${knownIds.size} bekannten IDs` : ''}`);
    if (skipKnown && knownIds) {
      console.log(`ID-Optimierung aktiviert: Bekannte Anzeigen werden übersprungen`);
    }
    
    const urlSet = new Set(); // Für Deduplizierung
    let currentUrl = url;
    
    // Durch alle Ergebnisseiten iterieren
    for (let page = 1; page <= maxPages; page++) {
      console.log(`Lade Ergebnisseite ${page}/${maxPages}: ${currentUrl}`);
      result.pagesCrawled++;
      
      // Seite laden mit verbesserter Fehlerbehandlung
      let response;
      try {
        response = await axios.get(currentUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
          },
          timeout: 30000 // 30 Sekunden Timeout
        });
      } catch (error) {
        console.error(`Fehler beim Laden der Seite ${page}: ${error.message}`);
        // Bei der ersten Seite brechen wir ab, sonst machen wir weiter
        if (page === 1) {
          throw new Error(`Konnte keine Anzeigen laden: ${error.message}`);
        }
        break;
      }
      
      // HTML parsen
      const dom = new JSDOM(response.data);
      const document = dom.window.document;
      
      // Gesamtzahl der Ergebnisse extrahieren (wenn vorhanden)
      try {
        const countElement = document.querySelector('.search-header__count');
        if (countElement) {
          console.log(`Gesamtanzahl laut Website: ${countElement.textContent.trim()}`);
        }
      } catch (countError) {
        // Ignorieren, wenn nicht vorhanden
      }
      
      // Alle Anzeigen-Links mit verschiedenen Selektoren extrahieren
      const adLinks = [];
      document.querySelectorAll('a[href*="/adv/"], .announcement__link, .announcement-container a, div.announcement a').forEach(link => {
        if (link.href && link.href.includes('/adv/')) {
          adLinks.push(link);
        }
      });
      
      console.log(`${adLinks.length} potenzielle Anzeigenlinks auf Seite ${page} gefunden`);
      
      // Jede Anzeige verarbeiten
      let newOnPage = 0;
      let skippedOnPage = 0;
      
      for (const link of adLinks) {
        const href = link.getAttribute('href');
        if (!href || !href.includes('/adv/')) continue;
        
        // Vollständige URL erstellen
        const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
        
        // URL deduplizieren
        if (urlSet.has(fullUrl)) continue;
        urlSet.add(fullUrl);
        
        // ID frühzeitig extrahieren für optimiertes Überspringen
        const id = extractAdId(fullUrl);
        
        // ID-basierte Optimierung: Bekannte IDs überspringen
        if (id && knownIds && skipKnown && knownIds.has(id)) {
          result.skippedIds.add(id);
          result.skippedCount++;
          skippedOnPage++;
          result.savedRequestsCount++;
          // Trotzdem zur Liste aller URLs hinzufügen
          result.allUrls.push(fullUrl);
        } else {
          // Neue oder unbekannte Anzeige verarbeiten
          newOnPage++;
          result.newUrls.push(fullUrl);
          result.allUrls.push(fullUrl);
          
          // ID-Mapping pflegen
          if (id) {
            result.urlsByIdMap[id] = fullUrl;
            result.idsByUrlMap[fullUrl] = id;
          }
        }
      }
      
      // Statistik für diese Seite
      result.totalFound += newOnPage + skippedOnPage;
      console.log(`Seite ${page}: ${newOnPage} neue Anzeigen, ${skippedOnPage} bekannte übersprungen`);
      
      // Nächste Seite suchen mit vereinfachter Logik
      let foundNextPage = false;
      try {
        // Nach "nächste Seite"-Links suchen
        const nextPageLinks = document.querySelectorAll('.pagination-wrapper a.next-page, .pagination a[rel="next"], .pagination__next, a.next-page');
        
        if (nextPageLinks && nextPageLinks.length > 0) {
          const nextLink = nextPageLinks[0];
          const nextHref = nextLink.getAttribute('href');
          
          if (nextHref) {
            currentUrl = nextHref.startsWith('http') ? nextHref : `${BASE_URL}${nextHref}`;
            foundNextPage = true;
            console.log(`Nächste Seite gefunden: ${currentUrl}`);
          }
        }
        
        // Falls keine spezifischen "next"-Links gefunden wurden, suche nach Zahlen-Links
        if (!foundNextPage) {
          const pageLinks = document.querySelectorAll('.pagination a');
          const nextPageNumber = page + 1;
          
          for (const link of pageLinks) {
            const linkText = link.textContent.trim();
            const href = link.getAttribute('href');
            
            if (linkText === `${nextPageNumber}` && href) {
              currentUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
              foundNextPage = true;
              console.log(`Nächste Seite (${nextPageNumber}) gefunden: ${currentUrl}`);
              break;
            }
          }
        }
      } catch (paginationError) {
        console.error(`Fehler beim Suchen der nächsten Seite: ${paginationError.message}`);
      }
      
      // Wenn keine nächste Seite gefunden wurde oder maximale Seitenzahl erreicht ist, beenden
      if (!foundNextPage || page >= maxPages) {
        console.log(`Keine weitere Seite gefunden oder maximale Seitenzahl erreicht. Beende nach Seite ${page}.`);
        break;
      }
      
      // Kurze Pause zwischen den Seiten einlegen
      await delay(1000);
    }
    
    // Abschlussstatistik
    const totalTime = Math.round((Date.now() - startTime) / 1000);
    result.estimatedTimeSaved = result.savedRequestsCount * 1.5; // Ca. 1,5 Sekunden pro Anfrage gespart
    
    console.log(`URL-Extraktion abgeschlossen in ${totalTime}s:`);
    console.log(`- ${result.totalFound} Anzeigen gefunden`);
    console.log(`- ${result.newUrls.length} neue Anzeigen zum Verarbeiten`);
    console.log(`- ${result.skippedCount} bereits bekannte Anzeigen übersprungen`);
    console.log(`- ~${result.estimatedTimeSaved.toFixed(1)}s Laufzeit durch Optimierung eingespart`);
    
    return result;
  } catch (error) {
    console.error(`Fehler bei URL-Extraktion: ${error.message}`);
    return {
      allUrls: [],
      newUrls: [],
      skippedIds: new Set(),
      urlsByIdMap: {},
      idsByUrlMap: {},
      error: error.message
    };
  }
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
/**
 * Extrahiert eine Ad-ID aus einer URL mit verbesserter Robustheit
 */
function extractAdId(url) {
  if (!url) return null;
  
  // Verschiedene Regex-Muster für mögliche URL-Formate
  const patterns = [
    /\/adv\/(\d+)_/,  // Standard-Format: /adv/12345_title
    /\/(\d+)\/?$/,    // Alternative: /12345/
    /id=(\d+)/        // Query-Parameter: ?id=12345
  ];
  
  for (const pattern of patterns) {
    const match = url.match(pattern);
    if (match && match[1]) {
      return String(match[1]).trim(); // Immer als String mit Trimming zurückgeben
    }
  }
  
  return null;
}

/**
 * Erzeugt einen reduzierten Listing-Datensatz für den Zustandsspeicher
 * Speichert nur die wichtigsten Felder, um Speicherkosten zu minimieren
 */
function createCompactListing(listing) {
  return {
    id: String(listing.id || '').trim(),
    title: listing.title,
    url: listing.url,
    price: listing.price,
    location: listing.location,
    details: listing.details,
    propertyType: listing.propertyType,
    scrapedAt: listing.scrapedAt || new Date().toISOString()
  };
}

/**
 * Optimierte Single-File-Funktion für Speicherung und Vergleich
 * Verwendet nur eine einzige state.json-Datei für maximale Kosteneffizienz
 */
async function saveAndCompareResults(listings, filterKey = '') {
  try {
    console.log('Starte optimierten Single-File-Vergleich...');
    const startTime = Date.now();
    
    // Single-File-Ansatz: Verwende nur eine einzige status.json-Datei
    const stateKey = `${RESULTS_PREFIX}${filterKey ? filterKey+'/' : ''}state.json`;
    
    // Stelle sicher, dass jede Anzeige eine eindeutige Ad-ID hat und normalisiere sie
    const processedListings = listings.map(listing => {
      // Extrahiere die Ad-ID aus der URL, falls noch nicht vorhanden
      if (!listing.id) {
        const adId = extractAdId(listing.url);
        if (adId) {
          listing.id = adId;
        }
      } else {
        // Normalisieren: Stelle sicher, dass die ID ein String ist
        listing.id = String(listing.id).trim();
      }
      return listing;
    });
    
    // Die aktuellen IDs als Set für schnellen Vergleich
    const currentIds = new Set(processedListings.map(listing => String(listing.id).trim()));
    
    // Debug: Zeige einige aktuelle IDs
    const sampleCurrentIds = [...currentIds].slice(0, 5);
    console.log(`Verarbeite ${processedListings.length} Anzeigen mit ${currentIds.size} eindeutigen IDs`);
    console.log(`Beispiel-IDs aus aktuellen Ergebnissen: ${sampleCurrentIds.join(', ')}`);
    
    // Vorherigen Zustand laden - verwende kompaktes Format für Kosteneffizienz
    let previousState = { timestamp: '', listings: [] };
    let isFirstRun = false;
    
    try {
      console.log(`Lade vorherigen Zustand aus: ${stateKey}`);
      const response = await s3.getObject({
        Bucket: S3_BUCKET_NAME,
        Key: stateKey
      }).promise();
      
      if (response && response.Body) {
        try {
          const parsedState = JSON.parse(response.Body.toString());
          if (parsedState && Array.isArray(parsedState.listings)) {
            previousState = parsedState;
            console.log(`Vorheriger Zustand geladen: ${parsedState.timestamp}`);
            console.log(`Vorherige Anzeigen: ${parsedState.listings.length}`);
            
            // Debug: Zeige einige vorherige IDs
            const previousIds = new Set(parsedState.listings.map(l => String(l.id).trim()));
            const samplePrevIds = [...previousIds].slice(0, 5);
            console.log(`Beispiel-IDs aus vorherigem Zustand: ${samplePrevIds.join(', ')}`);
          } else {
            console.warn(`Ungültiger Zustand in ${stateKey}. Format nicht korrekt.`);
          }
        } catch (parseError) {
          console.error(`Fehler beim Parsen des vorherigen Zustands: ${parseError.message}`);
        }
      }
    } catch (error) {
      if (error.code === 'NoSuchKey') {
        console.log(`Keine vorherige Zustandsdatei gefunden. Dies ist der erste Lauf.`);
        isFirstRun = true;
      } else {
        console.error(`Fehler beim Laden des vorherigen Zustands: ${error.message}`);
      }
    }
    
    // IDs aus vorherigem Zustand für Vergleich vorbereiten - mit zusätzlicher Normalisierung
    const previousIds = new Set();
    previousState.listings.forEach(listing => {
      if (listing.id) {
        // Stelle sicher, dass die ID ein normalisierter String ohne führende Nullen ist
        const normalizedId = String(parseInt(String(listing.id).trim(), 10));
        previousIds.add(normalizedId);
      }
    });
    
    // Normalisiere auch die aktuellen IDs für einen konsistenten Vergleich
    const normalizedCurrentIds = new Set();
    const normalizedIdMap = {}; // Mapping zwischen normalisierten IDs und Original-Listings
    
    processedListings.forEach(listing => {
      if (listing.id) {
        const normalizedId = String(parseInt(String(listing.id).trim(), 10));
        normalizedCurrentIds.add(normalizedId);
        normalizedIdMap[normalizedId] = listing;
      }
    });
    
    console.log(`Vergleich: ${normalizedCurrentIds.size} aktuelle vs ${previousIds.size} vorherige IDs.`);
    
    // Vergleiche aktuelle mit vorherigen IDs
    let newIds = [];
    const forceNotification = process.env.FORCE_NOTIFICATION === 'true';
    
    if (isFirstRun) {
      if (forceNotification) {
        console.log('Erster Lauf mit erzwungener Benachrichtigung: Alle Anzeigen werden als neu gemeldet.');
        // Alle aktuellen Anzeigen als neu betrachten
        newIds = [...normalizedCurrentIds];
      } else {
        console.log('Erster Lauf: Keine neuen Anzeigen werden gemeldet.');
        newIds = [];
      }
    } else if (previousIds.size === 0) {
      if (forceNotification) {
        console.log('Keine vorherigen Anzeigen gefunden, aber Benachrichtigung erzwungen.');
        newIds = [...normalizedCurrentIds];
      } else {
        console.log('Keine vorherigen Anzeigen gefunden. Behandle als ersten Lauf.');
        newIds = [];
      }
    } else {
      // Normale Vergleichslogik: Neue IDs sind die, die nicht im vorherigen Zustand waren
      newIds = [...normalizedCurrentIds].filter(id => !previousIds.has(id));
      console.log(`Ergebnis: ${newIds.length} neue Anzeigen gefunden.`);
      
      // Warnung bei verdächtig vielen neuen Anzeigen
      if (newIds.length > 50 && newIds.length > (normalizedCurrentIds.size * 0.25)) {
        console.warn(`⚠️ WARNUNG: Ungewöhnlich viele neue Anzeigen (${newIds.length}/${normalizedCurrentIds.size})!`);
        console.warn('Es könnte ein Problem mit dem ID-Vergleich oder dem Laden des vorherigen Zustands vorliegen.');
      }
    }
    
    // Entfernte Anzeigen identifizieren - mit normalisierten IDs
    const removedIds = [...previousIds].filter(id => !normalizedCurrentIds.has(id));
    console.log(`${removedIds.length} entfernte Anzeigen identifiziert.`);
    
    // Detaillierte Listen für neue und entfernte Anzeigen erstellen
    const newListings = [];
    newIds.forEach(id => {
      if (normalizedIdMap[id]) {
        newListings.push(normalizedIdMap[id]);
      }
    });
    
    const removedListings = previousState.listings.filter(listing => {
      if (!listing.id) return false;
      const normalizedId = String(parseInt(String(listing.id).trim(), 10));
      return removedIds.includes(normalizedId);
    });
    
    // Aktuellen Zustand in kompaktem Format speichern
    const compactListings = processedListings.map(createCompactListing);
    const currentState = {
      timestamp: new Date().toISOString(),
      listings: compactListings
    };
    
    console.log(`Speichere neuen Zustand in: ${stateKey}`);
    await s3.putObject({
      Bucket: S3_BUCKET_NAME,
      Key: stateKey,
      Body: JSON.stringify(currentState),
      ContentType: 'application/json'
    }).promise();
    
    const elapsedTime = Date.now() - startTime;
    console.log(`Speicher- und Vergleichsvorgang abgeschlossen in ${elapsedTime}ms.`);
    console.log(`Zusammenfassung: ${newListings.length} neue, ${removedListings.length} entfernte Anzeigen.`);
    
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
  // Überprüfe, ob Telegram-Benachrichtigungen übersprungen werden sollen
  if (process.env.SKIP_TELEGRAM === 'true') {
    console.log('Telegram-Benachrichtigungen werden übersprungen (SKIP_TELEGRAM=true)');
    console.log('Benachrichtigungsinhalt wäre:');
    console.log(JSON.stringify(changes, null, 2));
    return true;
  }
  
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
  console.log(`Sende optimierte Telegram-Benachrichtigung: ${hasNewListings ? changes.newListings.length + ' neue, ' : ''}${hasRemovedListings ? changes.removedListings.length + ' entfernte Anzeigen' : ''}${!hasChanges ? 'Keine Änderungen' : ''}${hasError ? ', Fehler aufgetreten' : ''}`);
  
  try {
    // Aktuelle Zeit für Datum-/Zeitstempel
    const now = new Date();
    const dateStr = now.toLocaleString('de-DE', { 
      day: '2-digit', 
      month: '2-digit', 
      year: 'numeric', 
      hour: '2-digit', 
      minute: '2-digit'
    });
    
    // Start der Zusammenfassungsnachricht mit Emoji für bessere Übersichtlichkeit
    let summaryMessage = `<b>🏠 Bazaraki Immobilien-Update</b>\n`;
    summaryMessage += `<i>${dateStr} Uhr</i>\n\n`;
    
    // Optionale Run-ID für Debugging
    if (runId) {
      summaryMessage += `Run: <code>${runId}</code>\n\n`;
    }
    
    // Zusammenfassung mit verbesserten Statistiken
    const total = changes.currentListings?.length || 0;
    const newCount = changes.newListings?.length || 0;
    const removedCount = changes.removedListings?.length || 0;
    const unchangedCount = total - newCount;
    const processingTimeMs = changes.processingTime || 0;
    
    // Haupt-Statistiken mit Emojis für bessere Erkennbarkeit
    summaryMessage += `<b>📊 Aktuelle Statistik:</b>\n`;
    summaryMessage += `• <b>${total}</b> aktuelle Anzeigen\n`;
    
    if (newCount > 0) {
      summaryMessage += `• <b>✨ ${newCount}</b> neue Anzeigen gefunden\n`;
    }
    
    if (removedCount > 0) {
      summaryMessage += `• <b>🚫 ${removedCount}</b> Anzeigen entfernt\n`;
    }
    
    if (unchangedCount > 0) {
      summaryMessage += `• <b>📋 ${unchangedCount}</b> unveränderte Anzeigen\n`;
    }
    
    // Optimierungs-Statistiken
    if (unchangedCount > 0 && !isFirstRun) {
      const timeSavedPerRequest = 1.5; // ~1,5 Sekunden pro Anfrage gespart (konservative Schätzung)
      const estimatedSavedSeconds = (unchangedCount * timeSavedPerRequest).toFixed(1);
      
      summaryMessage += `\n<b>⚡ Optimierung:</b>\n`;
      summaryMessage += `• <b>${unchangedCount}</b> Anfragen durch ID-Prüfung eingespart\n`;
      summaryMessage += `• <b>~${estimatedSavedSeconds}s</b> Laufzeit optimiert\n`;
      
      if (processingTimeMs > 0) {
        const processingTimeSeconds = (processingTimeMs / 1000).toFixed(1);
        summaryMessage += `• Gesamtlaufzeit: <b>${processingTimeSeconds}s</b>\n`;
      }
    }
    
    // Statusmeldung als eigener Block
    summaryMessage += `\n<b>Status:</b> `;
    if (!hasChanges) {
      summaryMessage += `✅ <i>Keine Veränderungen seit dem letzten Scan.</i>\n`;
    } else {
      summaryMessage += `✨ <i>Es wurden Veränderungen gefunden!</i>\n`;
    }
    
    // Fehlermeldung, falls vorhanden
    if (hasError) {
      summaryMessage += `\n⚠️ <b>FEHLER:</b>\n${changes.error}\n`;
    }
    
    // Warnung bei 0 Anzeigen
    if (total === 0) {
      summaryMessage += `\n⚠️ <b>WARNUNG:</b> Keine Anzeigen gefunden. Mögliches Problem mit dem Scraper.\n`;
    }
    
    // Zusammenfassungsnachricht senden
    await sendTelegramMessage(summaryMessage);
    
    // Bei der ersten Ausführung oder zu vielen neuen Anzeigen keine Detailnachrichten
    const forceNotification = process.env.FORCE_NOTIFICATION === 'true' || force;
    
    if ((isFirstRun && !forceNotification) || (newCount > 20 && !forceNotification)) {
      console.log('Zu viele neue Anzeigen oder erste Ausführung, überspringe Detailnachrichten.');
      const skipMsg = `<i>Zu viele neue Anzeigen (${newCount}), überspringe Detailnachrichten.</i>`;
      await sendTelegramMessage(skipMsg);
      return true;
    }
    
    // Bei erzwungener Benachrichtigung beim ersten Lauf
    if (isFirstRun && forceNotification) {
      console.log('Erste Ausführung mit erzwungener Benachrichtigung: Sende Details für alle Anzeigen.');
      await sendTelegramMessage('<b>⚠️ Erste Ausführung:</b> Alle Anzeigen werden als neu betrachtet.');
    }
    
    // Ankündigung für neue Anzeigen
    if (hasNewListings) {
      if (newCount > 1) {
        await sendTelegramMessage(`<b>Es folgen ${newCount} neue Immobilienanzeigen:</b>`);
        await delay(1000); // Kurze Pause vor den Detailnachrichten
      }
      
      console.log(`Sende ${newCount} Detailnachrichten für neue Anzeigen...`);
      
      // Für jede neue Anzeige eine separate Nachricht senden
      for (let i = 0; i < changes.newListings.length; i++) {
        await sendSingleListingMessage(changes.newListings[i], i+1, changes.newListings.length);
        
        // Adaptive Pause zwischen Nachrichten um Rate-Limits zu vermeiden
        // Je mehr Nachrichten bereits gesendet wurden, desto länger die Pause
        if (i < changes.newListings.length - 1) {
          const pauseTime = i < 3 ? 1000 : (i < 8 ? 2000 : 3000);
          await delay(pauseTime);
        }
      }
    }
    
    // Entfernte Anzeigen in einer Zusammenfassungsnachricht
    if (hasRemovedListings && removedCount <= 20) {
      let removedMessage = `<b>🚫 Entfernte Anzeigen (${removedCount}):</b>\n`;
      
      changes.removedListings.forEach((listing, i) => {
        const title = (listing.title || 'Keine Beschreibung').substring(0, 50);
        const price = listing.price?.text || '';
        const location = listing.location ? ` in ${listing.location}` : '';
        removedMessage += `${i + 1}. ${title}${price ? ` - ${price}` : ''}${location}\n`;
      });
      
      await sendTelegramMessage(removedMessage);
    } else if (hasRemovedListings && removedCount > 20) {
      // Nur Anzahl melden bei zu vielen entfernten Anzeigen
      await sendTelegramMessage(`<b>🚫 ${removedCount} Anzeigen wurden entfernt</b> (zu viele für eine detaillierte Auflistung).`);
    }
    
    console.log('Optimierte Telegram-Benachrichtigungen erfolgreich gesendet');
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
/**
 * Filtert Beschreibungen, um nur deutsche und russische Teile zu behalten
 */
function filterDescriptionLanguages(description) {
  if (!description) return '';
  
  // Teile nach Sprachsektionen aufteilen (meist durch Überschriften getrennt)
  const sections = [];
  
  // Deutsche und russische Beschreibungen finden und extrahieren
  const germanMatch = description.match(/(?:Deutsch|German|DE|auf Deutsch)[:\s]*(.*?)(?=\n\s*\n|\n\s*(?:Russisch|Russian|RU|English|Englisch|EN|Greek|Griechisch|GR):|$)/si);
  const russianMatch = description.match(/(?:Russisch|Russian|RU)[:\s]*(.*?)(?=\n\s*\n|\n\s*(?:Deutsch|German|DE|English|Englisch|EN|Greek|Griechisch|GR):|$)/si);
  
  // Deutsche Beschreibung hinzufügen
  if (germanMatch && germanMatch[1]) {
    sections.push(`<b>🇩🇪 Deutsch:</b>\n${germanMatch[1].trim()}`);
  } else {
    // Wenn keine explizite deutsche Beschreibung gefunden wurde, nehmen wir an, dass die gesamte Beschreibung auf Deutsch ist
    // (außer sie hat eindeutige Sprach-Marker für andere Sprachen)
    if (!description.match(/English|Englisch|EN:|Greek|Griechisch|GR:|Russian|Russisch|RU:|DE:|German|Deutsch:/))
      sections.push(`<b>🇩🇪 Deutsch:</b>\n${description.trim()}`);
  }
  
  // Russische Beschreibung hinzufügen
  if (russianMatch && russianMatch[1]) {
    sections.push(`<b>🇷🇺 Russisch:</b>\n${russianMatch[1].trim()}`);
  }
  
  return sections.join('\n\n');
}

async function sendSingleListingMessage(listing, index, total) {
  try {
    console.log(`Sende Einzelnachricht ${index}/${total} für Anzeige: ${listing.id}`);
    
    // Telegram Rate-Limit beachten: Adaptive Verzögerung basierend auf Index
    // Morgens und mittags langsamer senden als nachts
    const hour = new Date().getHours();
    const isNighttime = hour >= 22 || hour <= 6;
    const baseDelay = isNighttime ? 1000 : 2000;
    const delayTime = index < 3 ? baseDelay : (index < 8 ? baseDelay * 1.5 : baseDelay * 2); 
    await delay(delayTime);
    
    // Daten vorbereiten und fehlende Werte abfangen
    const title = listing.title || (listing.fullTitle ? listing.fullTitle.replace(/€[0-9.,]+/g, '').trim() : 'Keine Beschreibung');
    const url = listing.url || '#';
    const adId = listing.id || 'Unbekannt';
    
    // Emoji für Nachrichtenformatierung
    const emoji = {
      house: '🏠',
      apartment: '🏢',
      price: '💰',
      location: '📍',
      area: '📏',
      bedrooms: '🛏️',
      bathrooms: '🚿',
      pets: '🐕',
      calendar: '📅',
      furniture: '🛋️',
      link: '🔗'
    };
    
    // Typ der Immobilie bestimmen
    const isApartment = title.toLowerCase().includes('apartment') || 
                        title.toLowerCase().includes('wohnung') || 
                        (listing.propertyType && listing.propertyType.includes('apart'));
    const propertyEmoji = isApartment ? emoji.apartment : emoji.house;
    
    // Nachricht erstellen mit schönerer Formatierung
    let message = `<b>${propertyEmoji} Immobilie ${index}/${total}</b>\n`;
    message += `<b>${title}</b>\n\n`;
    
    // Preis mit Emoji hinzufügen
    if (listing.price && listing.price.text) {
      message += `${emoji.price} <b>${listing.price.text}</b>\n`;
    }
    
    // Standort mit Emoji hinzufügen
    if (listing.location && listing.location !== 'Standort nicht angegeben') {
      message += `${emoji.location} ${listing.location}\n`;
    }
    
    // Zimmerdaten sammeln (entweder aus details oder characteristics)
    let detailsList = [];
    
    // Schlafzimmer
    const bedrooms = listing.details?.bedrooms || 
                     (listing.characteristics && listing.characteristics['bedrooms']) ||
                     (listing.characteristics && listing.characteristics['number-of-bedrooms']);
    if (bedrooms) {
      detailsList.push(`${emoji.bedrooms} ${bedrooms} Schlafzimmer`);
    }
    
    // Badezimmer
    const bathrooms = listing.details?.bathrooms || 
                      (listing.characteristics && listing.characteristics['bathrooms']) ||
                      (listing.characteristics && listing.characteristics['number-of-bathrooms']);
    if (bathrooms) {
      detailsList.push(`${emoji.bathrooms} ${bathrooms} Badezimmer`);
    }
    
    // Fläche
    const area = listing.details?.area || 
                 (listing.characteristics && listing.characteristics['property-area']) ||
                 (listing.characteristics && listing.characteristics['area']);
    if (area) {
      detailsList.push(`${emoji.area} ${area}`);
    }
    
    // Grundstücksgröße
    const plotArea = listing.plotArea || 
                     (listing.characteristics && listing.characteristics['plot-area']);
    if (plotArea) {
      detailsList.push(`Grundstück: ${plotArea}`);
    }
    
    // Details als Liste anzeigen
    if (detailsList.length > 0) {
      message += `\n${detailsList.join('\n')}\n`;
    }
    
    // Zusätzliche Details sammeln
    let additionalDetails = [];
    
    // Einrichtung
    const furnishing = listing.furnishing || 
                      (listing.characteristics && listing.characteristics['furnishing']);
    if (furnishing) {
      additionalDetails.push(`${emoji.furniture} ${furnishing}`);
    }
    
    // Haustiere
    const petsAllowed = listing.petsAllowed !== undefined ? listing.petsAllowed : 
                       (listing.characteristics && listing.characteristics['pets'] && 
                        listing.characteristics['pets'].toLowerCase().includes('allow'));
    if (petsAllowed !== undefined) {
      additionalDetails.push(`${emoji.pets} Haustiere: ${petsAllowed ? 'erlaubt' : 'nicht erlaubt'}`);
    }
    
    // Baujahr
    const constructionYear = listing.constructionYear || 
                            (listing.characteristics && listing.characteristics['construction-year']);
    if (constructionYear) {
      additionalDetails.push(`${emoji.calendar} Baujahr: ${constructionYear}`);
    }
    
    // Weitere relevante charakteristische Merkmale
    if (listing.characteristics) {
      const relevantKeys = ['type', 'parking', 'energy-efficiency', 'included'];
      relevantKeys.forEach(key => {
        if (listing.characteristics[key]) {
          const niceName = key.replace(/-/g, ' ');
          additionalDetails.push(`${niceName}: ${listing.characteristics[key]}`);
        }
      });
    }
    
    // Zusätzliche Details als Liste anzeigen
    if (additionalDetails.length > 0) {
      message += `\n<b>Weitere Details:</b>\n• ${additionalDetails.join('\n• ')}\n`;
    }
    
    // Gefilterte Beschreibung (nur Deutsch und Russisch) 
    if (listing.description) {
      const filteredDescription = filterDescriptionLanguages(listing.description);
      if (filteredDescription) {
        message += `\n${filteredDescription}`;
        // Wenn die Beschreibung sehr lang ist, kürzen
        if (filteredDescription.length > 500) {
          message += `\n... <a href="${url}">mehr lesen</a>`;
        }
        message += '\n';
      }
    }
    
    // Link zur Anzeige und ID hinzufügen
    message += `\n${emoji.link} <a href="${url}">Anzeige auf Bazaraki ansehen</a> (ID: ${adId})\n`;
    
    // Nachricht senden (ohne Vorschau, da wir eigene Bilder senden)
    await sendTelegramMessage(message, 'HTML', true);
    await delay(1000); // Pause nach dem Senden der Hauptnachricht
    
    // Alle verfügbaren Bilder senden
    if (listing.images && listing.images.length > 0) {
      const imageUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendPhoto`;
      const mediaGroupUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`;
      
      try {
        // Wenn es mehrere Bilder gibt, sende sie als Mediengruppe (max. 10 Bilder pro Gruppe)
        if (listing.images.length > 1) {
          // Bilder in Gruppen von maximal 10 aufteilen (Telegram-Limit)
          for (let i = 0; i < listing.images.length; i += 10) {
            const imagesBatch = listing.images.slice(i, i + 10);
            const groupNumber = Math.floor(i / 10) + 1;
            const totalGroups = Math.ceil(listing.images.length / 10);
            
            // Mediengruppe erstellen (Album mit mehreren Bildern)
            const media = imagesBatch.map((img, imgIndex) => ({
              type: 'photo',
              media: img,
              caption: imgIndex === 0 ? 
                `${propertyEmoji} ${title} (Gruppe ${groupNumber}/${totalGroups})` : 
                '',
              parse_mode: 'HTML'
            }));
            
            // Mediengruppe senden
            await axios.post(mediaGroupUrl, {
              chat_id: TELEGRAM_CHAT_ID,
              media
            }, {
              timeout: 30000 // 30 Sekunden Timeout für Bildgruppen
            });
            
            console.log(`Bildgruppe ${groupNumber}/${totalGroups} für Anzeige ${listing.id} gesendet`);
            await delay(3000); // Längere Pause zwischen Bildgruppen
          }
        } else {
          // Nur ein einzelnes Bild senden
          const imagePayload = {
            chat_id: TELEGRAM_CHAT_ID,
            photo: listing.images[0],
            caption: `${propertyEmoji} ${title}\n${listing.price?.text || ''}`,
            parse_mode: 'HTML'
          };
          
          await axios.post(imageUrl, imagePayload, {
            timeout: 20000 // 20 Sekunden Timeout für Bilder
          });
          
          console.log(`Bild für Anzeige ${listing.id} erfolgreich gesendet`);
        }
      } catch (imageError) {
        console.error(`Fehler beim Senden der Bilder für Anzeige ${listing.id}: ${imageError.message}`);
        
        // Differenzierte Fehlerbehandlung
        if (imageError.message.includes('429') || imageError.message.includes('too many requests')) {
          console.log('Rate-Limit erreicht, warte 10 Sekunden...');
          await delay(10000); // Sehr lange Pause bei Rate-Limit
        } else {
          await delay(5000); // Standard-Pause bei anderen Fehlern
        }
      }
    }
    
    return true;
  } catch (error) {
    console.error(`Fehler beim Senden der Einzelnachricht für Anzeige ${listing.id}: ${error.message}`);
    
    // Differenzierte Fehlerbehandlung
    if (error.message.includes('429') || error.message.includes('too many requests')) {
      console.log('Rate-Limit erreicht, warte 10 Sekunden...');
      await delay(10000); // Sehr lange Pause bei Rate-Limit
    } else {
      await delay(5000); // Standard-Pause bei anderen Fehlern
    }
    
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
 * Sendet eine Mediengruppe (Bilder-Album) über Telegram
 */
async function sendTelegramMediaGroup(images, caption, url, index, total, retryCount = 0) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log('Telegram-Konfiguration fehlt. Keine Mediengruppe gesendet.');
    return false;
  }
  
  try {
    const mediaGroupUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`;
    
    // Erstellt eine Mediengruppe mit den Bildern
    // Der erste Eintrag enthält die Caption, die anderen sind leer
    const media = images.map((img, idx) => ({
      type: 'photo',
      media: img,
      caption: idx === 0 ? caption.substring(0, 1024) : '', // Telegram Limit: 1024 Zeichen
      parse_mode: 'HTML'
    }));
    
    // Einige Informationen über den Sendevorgang protokollieren
    console.log(`Sende Mediengruppe mit ${images.length} Bildern für Anzeige ${index}/${total}...`);
    
    // Mediengruppe senden
    const response = await axios.post(mediaGroupUrl, {
      chat_id: TELEGRAM_CHAT_ID,
      media
    }, {
      timeout: 30000 // 30 Sekunden Timeout
    });
    
    console.log(`Mediengruppe erfolgreich gesendet. Status: ${response.status}`);
    return true;
  } catch (error) {
    // Fehlerbehandlung mit Wiederholungsversuch
    const maxRetries = 3;
    
    if (error.response) {
      console.error(`Telegram API Fehler: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    } else {
      console.error(`Fehler beim Senden der Mediengruppe: ${error.message}`);
    }
    
    // Bei Rate-Limit oder Netzwerkfehler Wiederholungsversuch
    if (retryCount < maxRetries && (error.message.includes('429') || 
        error.message.includes('ETIMEOUT') || 
        error.message.includes('network'))) {
      // Exponentielles Backoff
      const waitTime = 3000 * Math.pow(2, retryCount);
      console.log(`Wiederholungsversuch ${retryCount + 1}/${maxRetries} in ${waitTime}ms...`);
      await delay(waitTime);
      return sendTelegramMediaGroup(images, caption, url, index, total, retryCount + 1);
    }
    
    return false;
  }
}

/**
 * Sendet eine Testnachricht über Telegram mit mehreren Beispielbildern
 */
async function sendTestTelegramMessage() {
  try {
    console.log('Starte Test der optimierten Anzeigennachrichten...');
    
    // Beispiel-Anzeige erstellen mit mehr Daten und Bildern
    const testListing = {
      id: '12345678',
      title: 'Schöne 3-Schlafzimmer Bungalow in Paphos mit Meerblick',
      url: 'https://www.bazaraki.com/adv/12345678_3-bedroom-detached-house-to-rent/',
      price: {
        text: '€2.000',
        value: 2000
      },
      location: 'Paphos, Thrinia',
      details: {
        bedrooms: '3',
        bathrooms: '4',
        area: '180 m²'
      },
      characteristics: {
        'type': 'Detached house',
        'parking': 'Uncovered',
        'plot-area': '980 m²',
        'furnishing': 'Semi-Furnished',
        'included': 'Garden, Alarm, Fireplace, Storage room',
        'online-viewing': 'Yes',
        'air-conditioning': 'Full, all rooms',
        'construction-year': '2025',
        'energy-efficiency': 'A'
      },
      description: 'Brand New Luxury 3 bedroom en suite Bungalow in Drinia (Thrinia) village. Live in a Private Haven of Outdoor Beauty & Indoor Luxury, its a 25 minute drive from Paphos.\n\nEllenisch:\nΕλληνικα\n\nEnglish:\nThis is a beautiful house with 3 bedrooms and a nice view.\n\nDeutsch:\nDies ist ein wunderschönes, brandneues Luxus-Bungalow mit 3 Schlafzimmern in der Ortschaft Drinia. Genießen Sie die Schönheit der Natur und den Luxus im Inneren, nur 25 Minuten von Paphos entfernt.\n\nРусский:\nЭто великолепный новый роскошный бунгало с 3 спальнями в деревне Дриния, всего в 25 минутах езды от Пафоса.',
      images: [
        'https://cdn1.bazaraki.com/media/cache1/5d/fc/5dfc29971e9abe42143856f3667a4579.webp',
        'https://cdn1.bazaraki.com/media/cache1/32/d1/32d1995b0736ff987ab000434aaaea5c.webp',
        'https://cdn1.bazaraki.com/media/cache1/87/be/87be844e72a34fc8a53e8b64fc22417e.webp',
        'https://cdn1.bazaraki.com/media/cache1/fd/e3/fde356b398f806c8efa65080045682f0.webp',
        'https://cdn1.bazaraki.com/media/cache1/9e/38/9e3800556b9cb6206393916ba0474f03.webp',
        'https://cdn1.bazaraki.com/media/cache1/bd/76/bd764485f1e8a6f3334aae5db6bdbb9d.webp'
      ],
      scrapedAt: new Date().toISOString(),
      propertyType: 'houses'
    };
    
    // Prüfen, ob Telegram-Konfiguration vorhanden ist
    if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
      try {
        // Hinweis-Nachricht senden
        console.log('Sende Telegram-Testnachricht...');
        await sendTelegramMessage(`<b>💬 Test der optimierten Anzeigennachrichten</b>\n\nEs folgt eine Beispielanzeige mit mehreren Bildern und erweitertem Layout für den HTML-Parser.`);
        await delay(1000);
        
        // Testnachricht mit der Beispiel-Anzeige senden
        await sendSingleListingMessage(testListing, 1, 1);
        
        console.log('Erweiterte Telegram-Testnachricht erfolgreich gesendet');
      } catch (telegramError) {
        console.warn(`Telegram-Nachricht konnte nicht gesendet werden: ${telegramError.message}`);
      }
    } else {
      console.log('Keine Telegram-Konfiguration gefunden. Simuliere Nachrichtenformat...');
      
      // Nachrichtenformat simulieren
      console.log('\n=== SIMULIERTE TELEGRAM-NACHRICHT ===');
      console.log('Beispielanzeige:');
      console.log(`Titel: ${testListing.title}`);
      console.log(`Preis: ${testListing.price.text}`);
      console.log(`Ort: ${testListing.location}`);
      console.log(`Details: ${testListing.details.bedrooms} Schlafzimmer, ${testListing.details.bathrooms} Badezimmer, ${testListing.details.area}`);
      console.log(`Bilder: ${testListing.images.length} Stück`);
      console.log('====================================\n');
    }
    
    return true;
  } catch (error) {
    console.error(`Fehler beim Testen der optimierten Anzeigennachrichten: ${error.message}`);
    return false;
  }
}

/**
 * Testet den optimierten Vergleichsprozess für Ad IDs
 */
async function testOptimizedScraping() {
  try {
    console.log('Starte Test des optimierten Scraping-Prozesses...');
    
    // Einige Test-ID-Sets erstellen
    const previousIds = new Set(['123', '456', '789', '101112']);
    const currentIds = new Set(['123', '789', '131415', '161718']);
    
    // Statistik berechnen
    const unchangedIds = new Set();
    const newIds = [];
    const removedIds = [];
    
    // Unveränderte IDs finden (in beiden Sets)
    currentIds.forEach(id => {
      if (previousIds.has(id)) {
        unchangedIds.add(id);
      } else {
        newIds.push(id);
      }
    });
    
    // Entfernte IDs finden (nur in previousIds)
    previousIds.forEach(id => {
      if (!currentIds.has(id)) {
        removedIds.push(id);
      }
    });
    
    // Statistik ausgeben
    console.log(`\n== ID-Vergleich-Test ==`);
    console.log(`Vorherige IDs: ${[...previousIds].join(', ')}`);
    console.log(`Aktuelle IDs: ${[...currentIds].join(', ')}`);
    console.log(`Unveränderte IDs: ${[...unchangedIds].join(', ')}`);
    console.log(`Neue IDs: ${newIds.join(', ')}`);
    console.log(`Entfernte IDs: ${removedIds.join(', ')}`);
    
    // Zeitersparnisschätzung
    const savedRequests = unchangedIds.size;
    const estimatedTimeSaved = savedRequests * 1.5; // ~1,5 Sekunden pro Anfrage gespart
    
    console.log(`\n== Optimierungsbericht ==`);
    console.log(`${savedRequests} Anfragen eingespart durch Wiederverwendung vorhandener Daten`);
    console.log(`Geschätzte Zeitersparnis: ~${estimatedTimeSaved.toFixed(1)} Sekunden`);
    
    // Nachricht via Telegram senden (wenn konfiguriert)
    if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
      const message = `<b>📊 Test des optimierten ID-Vergleichs</b>\n\n` +
                     `Vorherige IDs: ${previousIds.size}\n` +
                     `Aktuelle IDs: ${currentIds.size}\n` +
                     `Unveränderte IDs: ${unchangedIds.size}\n` +
                     `Neue IDs: ${newIds.length}\n` +
                     `Entfernte IDs: ${removedIds.length}\n\n` +
                     `<b>⚡ Optimierungsbericht:</b>\n` +
                     `${savedRequests} Anfragen eingespart\n` +
                     `~${estimatedTimeSaved.toFixed(1)} Sekunden gespart`;
      
      try {
        await sendTelegramMessage(message);
        console.log('Telegram-Nachricht erfolgreich gesendet');
      } catch (telegramError) {
        console.warn(`Telegram-Nachricht konnte nicht gesendet werden: ${telegramError.message}`);
        console.log('Test wurde trotzdem erfolgreich abgeschlossen');
      }
    } else {
      console.log('Keine Telegram-Konfiguration gefunden. Nachricht wird nicht gesendet.');
    }
    
    return true;
  } catch (error) {
    console.error(`Fehler beim Testen des optimierten Scraping-Prozesses: ${error.message}`);
    return false;
  }
}

// Export für AWS Lambda
// Handler für AWS Lambda exportieren
exports.handler = handler;

// Funktionen für Tests exportieren
exports.sendTestTelegramMessage = sendTestTelegramMessage;
exports.testOptimizedScraping = testOptimizedScraping;
exports.sendTelegramMessage = sendTelegramMessage;
exports.sendTelegramMediaGroup = sendTelegramMediaGroup;

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
