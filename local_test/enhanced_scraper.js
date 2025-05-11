/**
 * Erweiterter Bazaraki Scraper
 * Extrahiert detaillierte Informationen und Bilder für Telegram-Nachrichten
 */

const axios = require('axios');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');

// AWS-Konfiguration (optional)
const s3 = new AWS.S3();
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME || 'bazaraki-scraper-results';
const RESULTS_PREFIX = process.env.RESULTS_PREFIX || 'results/';

// Telegram-Konfiguration
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
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

// Standard HTTP-Header für realistische Browser-Simulation
const DEFAULT_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
  'Accept-Language': 'en-US,en;q=0.9',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
};

/**
 * Hauptfunktion, die den gesamten Scraping-Prozess steuert
 */
async function scrapeBazaraki() {
  console.log('Starte erweiterten Bazaraki-Scraper...');
  
  try {
    // Konfiguration
    const config = {
      location: 'paphos',
      dealType: 'rent',
      propertyType: 'apartments',
      minPrice: 500,
      maxPrice: 1500,
      maxPages: 2  // Begrenzen auf 2 Seiten für Test
    };
    
    // Anzeigen-URLs sammeln
    const listingUrls = await collectListingUrls(config);
    console.log(`${listingUrls.length} Anzeigen-URLs gesammelt`);
    
    // Detaillierte Informationen zu jeder Anzeige abrufen
    const detailedListings = await getDetailedListings(listingUrls.slice(0, 5)); // Begrenze auf 5 für Tests
    console.log(`${detailedListings.length} detaillierte Anzeigen extrahiert`);
    
    // Optional: Ergebnisse speichern
    await saveResults(detailedListings);
    
    // Telegram-Nachricht generieren und senden
    const telegramMessage = generateTelegramMessage(detailedListings);
    console.log("\nGenerierte Telegram-Nachricht (Ausschnitt):");
    console.log(telegramMessage.substring(0, 500) + "...");
    
    // Wenn Telegram-Konfiguration vorhanden ist, Nachricht senden
    if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
      await sendTelegramMessage(telegramMessage);
    } else {
      console.log("Keine Telegram-Konfiguration gefunden. Nachricht nicht gesendet.");
    }
    
    console.log("\nScraping abgeschlossen!");
    return detailedListings;
  } catch (error) {
    console.error(`Fehler beim Scraping: ${error.message}`);
    console.error(error.stack);
    return [];
  }
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
      const response = await axios.get(url, { headers: DEFAULT_HEADERS });
      
      // Debug-Ausgabe
      if (DEBUG_MODE) {
        const debugFilePath = path.join(__dirname, `debug_page_${page}.html`);
        fs.writeFileSync(debugFilePath, response.data);
        console.log(`Debug-HTML für Seite ${page} gespeichert in: ${debugFilePath}`);
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
      const response = await axios.get(listing.url, { headers: DEFAULT_HEADERS });
      
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
        const debugFilePath = path.join(__dirname, `debug_listing_${listing.id}.html`);
        fs.writeFileSync(debugFilePath, response.data);
        console.log(`Debug-HTML für Anzeige ${listing.id} gespeichert in: ${debugFilePath}`);
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
    const priceMatch = priceText.match(/([\d\s,.]+)\s*([€$£₽]|EUR)/i);
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
 * Speichert die Ergebnisse in JSON und optional S3
 */
async function saveResults(listings) {
  try {
    // Lokal speichern
    const localFilePath = path.join(__dirname, 'bazaraki_results.json');
    fs.writeFileSync(localFilePath, JSON.stringify(listings, null, 2));
    console.log(`Ergebnisse lokal gespeichert in: ${localFilePath}`);
    
    // Optional: In S3 speichern
    if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
      const today = new Date().toISOString().split('T')[0];
      const s3Key = `${RESULTS_PREFIX}${today}.json`;
      
      await s3.putObject({
        Bucket: S3_BUCKET_NAME,
        Key: s3Key,
        Body: JSON.stringify({
          timestamp: new Date().toISOString(),
          listings: listings
        }),
        ContentType: 'application/json'
      }).promise();
      
      console.log(`Ergebnisse in S3 gespeichert: s3://${S3_BUCKET_NAME}/${s3Key}`);
    }
  } catch (error) {
    console.error(`Fehler beim Speichern der Ergebnisse: ${error.message}`);
  }
}

/**
 * Generiert eine formatierte Telegram-Nachricht mit Listing-Informationen und Bildern
 */
function generateTelegramMessage(listings) {
  const dateStr = new Date().toISOString().replace('T', ' ').substring(0, 19);
  let message = `*Bazaraki Immobilien-Update (${dateStr})*\n\n`;
  
  // Zusammenfassung
  message += `*Gefundene Anzeigen:* ${listings.length}\n\n`;
  
  // Detaillierte Liste der Anzeigen
  listings.forEach((listing, i) => {
    const price = listing.price?.text || 'Preis auf Anfrage';
    const location = listing.location || 'Ort unbekannt';
    const bedrooms = listing.details?.bedrooms ? `${listing.details.bedrooms} BR` : '';
    const bathrooms = listing.details?.bathrooms ? `${listing.details.bathrooms} BA` : '';
    const area = listing.details?.area ? `${listing.details.area} m²` : '';
    const details = [bedrooms, bathrooms, area].filter(Boolean).join(', ');
    
    message += `*${i + 1}. ${listing.title || 'Keine Beschreibung'}*\n`;
    message += `📍 ${location}\n`;
    message += `💰 ${price}\n`;
    if (details) message += `🏠 ${details}\n`;
    message += `🔗 [Anzeige ansehen](${listing.url})\n`;
    
    // Erste 100 Zeichen der Beschreibung
    if (listing.description && listing.description !== 'Keine Beschreibung verfügbar') {
      const shortDesc = listing.description.substring(0, 100) + 
                       (listing.description.length > 100 ? '...' : '');
      message += `📝 _${shortDesc}_\n`;
    }
    
    // Bildreferenz für die Nachricht
    if (listing.images && listing.images.length > 0) {
      message += `🖼 [Foto](${listing.images[0]})\n`;
    }
    
    message += '\n';
  });
  
  // Hinweis zu Bildern (da Telegram in normalen Nachrichten keine Bilder rendert)
  message += "_Hinweis: Bilder sind als Links angegeben, da Telegram in Text-Nachrichten keine Bilder darstellen kann._";
  
  return message;
}

/**
 * Sendet eine Nachricht über Telegram
 */
async function sendTelegramMessage(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log("Telegram-Konfiguration fehlt. Keine Nachricht gesendet.");
    return false;
  }
  
  try {
    // Nachricht aufteilen, wenn sie zu lang ist (Telegram-Limit: 4096 Zeichen)
    const maxLength = 4000;
    
    if (message.length <= maxLength) {
      // Kurze Nachricht direkt senden
      await sendSingleMessage(message);
    } else {
      // Lange Nachricht in Teile aufteilen
      const parts = splitMessage(message, maxLength);
      console.log(`Nachricht zu lang (${message.length} Zeichen), wird in ${parts.length} Teile aufgeteilt`);
      
      for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        const header = i === 0 ? '' : `*Teil ${i+1}/${parts.length}*\n\n`;
        await sendSingleMessage(header + part);
        
        // Kurze Pause zwischen den Nachrichten
        if (i < parts.length - 1) {
          await delay(1000);
        }
      }
    }
    
    console.log("Telegram-Nachricht(en) erfolgreich gesendet");
    return true;
  } catch (error) {
    console.error(`Fehler beim Senden der Telegram-Nachricht: ${error.message}`);
    return false;
  }
}

/**
 * Sendet eine einzelne Telegram-Nachricht
 */
async function sendSingleMessage(text) {
  const telegramUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const payload = {
    chat_id: TELEGRAM_CHAT_ID,
    text: text,
    parse_mode: 'Markdown',
    disable_web_page_preview: false
  };
  
  const response = await axios.post(telegramUrl, payload);
  
  if (response.status !== 200 || !response.data.ok) {
    throw new Error(`Telegram API-Fehler: ${JSON.stringify(response.data)}`);
  }
}

/**
 * Teilt eine lange Nachricht in mehrere kleinere Teile auf
 */
function splitMessage(message, maxLength) {
  const parts = [];
  let currentPart = '';
  
  // Nach Zeilenumbrüchen aufteilen
  const lines = message.split('\n');
  
  for (const line of lines) {
    // Wenn die aktuelle Zeile zu lang ist, um hinzugefügt zu werden
    if (currentPart.length + line.length + 1 > maxLength) {
      // Wenn die aktuelle Zeile für sich selbst zu lang ist, aufteilen
      if (line.length > maxLength) {
        // Aktuelle Teil abschließen und hinzufügen
        if (currentPart) {
          parts.push(currentPart);
          currentPart = '';
        }
        
        // Lange Zeile in maxLength-große Blöcke aufteilen
        let remainingLine = line;
        while (remainingLine.length > 0) {
          const chunk = remainingLine.substring(0, maxLength);
          parts.push(chunk);
          remainingLine = remainingLine.substring(maxLength);
        }
      } else {
        // Aktuellen Teil abschließen und neuen beginnen
        parts.push(currentPart);
        currentPart = line;
      }
    } else {
      // Zeile hinzufügen mit Zeilenumbruch, wenn nicht leer
      if (currentPart) {
        currentPart += '\n' + line;
      } else {
        currentPart = line;
      }
    }
  }
  
  // Letzten Teil hinzufügen, wenn vorhanden
  if (currentPart) {
    parts.push(currentPart);
  }
  
  return parts;
}

/**
 * Helfer-Funktion für Verzögerungen
 */
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Direkte Ausführung, wenn als Skript aufgerufen
if (require.main === module) {
  scrapeBazaraki()
    .then(listings => {
      console.log(`Scraping abgeschlossen. ${listings.length} Anzeigen gefunden.`);
    })
    .catch(error => {
      console.error('Unbehandelter Fehler:', error);
    });
}
