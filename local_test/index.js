/**
 * Bazaraki Puppeteer Scraper für EC2
 * 
 * Dieser Scraper verwendet Puppeteer, um JavaScript-gerenderte Inhalte von Bazaraki zu extrahieren
 * und speichert die Ergebnisse in S3.
 */

const puppeteer = require('puppeteer');
const AWS = require('aws-sdk');
const axios = require('axios');
const fs = require('fs');
const { CronJob } = require('cron');

// AWS-Konfiguration
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

/**
 * Hauptfunktion zum Scrapen von Bazaraki
 */
async function scrapeBazaraki() {
  const runId = new Date().toISOString().replace(/[:.]/g, '-');
  console.log(`Bazaraki Scraper gestartet. Run ID: ${runId}`);
  
  let browser = null;
  
  try {
    // Browser mit angepassten Optionen starten - für lokale Tests
    // Verwende den Auto-Download von Puppeteer statt fest installiertem Chrome
    browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--window-size=1280,800'
      ]
      // Kein executablePath angeben - Puppeteer lädt Browser automatisch
    });
    
    // Konfiguration für den Scraper
    const config = {
      location: 'paphos',
      dealType: 'rent',
      propertyType: 'apartments',
      minPrice: 500,
      maxPrice: 1500,
      maxPages: 3
    };
    
    // Bazaraki-Anzeigen scrapen
    const listings = await scrapePropertyListings(browser, config);
    console.log(`Insgesamt ${listings.length} Immobilienanzeigen gefunden`);
    
    // Ergebnisse speichern und vergleichen
    const changes = await saveAndCompareResults(listings);
    
    // Benachrichtigung senden
    await sendTelegramNotification(changes, false, runId);
    
    console.log('Scraper erfolgreich beendet');
    return { statusCode: 200 };
  } catch (error) {
    console.error(`Fehler während des Scrapings: ${error.message}`);
    
    // Benachrichtigung bei Fehlern
    await sendTelegramNotification({ 
      currentListings: [], 
      newListings: [],
      removedListings: [],
      error: error.message
    }, true, runId);
    
    return { statusCode: 500 };
  } finally {
    if (browser) await browser.close();
  }
}

/**
 * Immobilienanzeigen von Bazaraki scrapen
 */
async function scrapePropertyListings(browser, config) {
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
  
  // Alle Anzeigen sammeln
  const allListings = [];
  
  // Durch die Seiten iterieren
  for (let page = 1; page <= config.maxPages; page++) {
    params.set('page', page);
    const url = `${BASE_URL}${urlPath}?${params.toString()}`;
    console.log(`Scrape Seite ${page} von ${config.maxPages}: ${url}`);
    
    const browserPage = await browser.newPage();
    await browserPage.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36');
    
    try {
      // Seite laden und warten
      await browserPage.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
      await browserPage.waitForTimeout(3000);
      
      // Debug: HTML speichern
      if (DEBUG_MODE) {
        const html = await browserPage.content();
        fs.writeFileSync(`/tmp/bazaraki_page_${page}.html`, html);
      }
      
      // Anzeigen extrahieren mit verschiedenen Selektoren
      const selectors = [
        '.announcement-container', '.list-simple__item',
        '[data-listing-id]', 'a[href*="/adv/"]'
      ];
      
      let listings = [];
      
      // Versuche alle Selektoren
      for (const selector of selectors) {
        try {
          const foundListings = await browserPage.$$eval(selector, elements => {
            return elements.map(el => {
              // ID aus Attributen oder URL extrahieren
              let id = '';
              if (el.id && el.id.match(/\d+/)) {
                id = el.id.match(/\d+/)[0];
              } else if (el.getAttribute('data-id')) {
                id = el.getAttribute('data-id');
              } else if (el.getAttribute('href') && el.getAttribute('href').match(/\/adv\/(\d+)/)) {
                id = el.getAttribute('href').match(/\/adv\/(\d+)/)[1];
              }
              
              // URL
              let url = '';
              if (el.tagName === 'A' && el.getAttribute('href')?.includes('/adv/')) {
                url = el.getAttribute('href');
              } else {
                const linkEl = el.querySelector('a[href*="/adv/"]');
                if (linkEl) url = linkEl.getAttribute('href');
              }
              
              if (url && url.startsWith('/')) {
                url = `https://www.bazaraki.com${url}`;
              }
              
              // Titel
              let title = el.getAttribute('title') || '';
              if (!title) {
                const titleEl = el.querySelector('.announcement__title, h3, .title');
                if (titleEl) title = titleEl.textContent.trim();
              }
              
              // Preis
              let price = null;
              let currency = '€';
              const priceEl = el.querySelector('.price, [class*="price"]');
              if (priceEl) {
                const priceText = priceEl.textContent.trim();
                const priceMatch = priceText.match(/([\d,\.\s]+)\s*([€$£₽]|EUR)/);
                if (priceMatch) {
                  price = parseInt(priceMatch[1].replace(/[\s,.]/g, ''), 10);
                  currency = priceMatch[2];
                }
              }
              
              return {
                id: id || `unknown-${Math.random().toString(36).substring(2, 8)}`,
                title: title || 'Keine Beschreibung',
                url,
                price,
                currency,
                date: new Date().toISOString()
              };
            }).filter(listing => listing.url && !listing.url.includes('/user/'));
          });
          
          if (foundListings && foundListings.length > 0) {
            console.log(`${foundListings.length} Anzeigen mit Selektor ${selector} gefunden`);
            listings = foundListings;
            break;
          }
        } catch (e) {
          console.log(`Keine Anzeigen mit Selektor ${selector} gefunden`);
        }
      }
      
      if (listings.length > 0) {
        allListings.push(...listings);
      } else if (page === 1) {
        break; // Bei Seite 1 ohne Ergebnisse abbrechen
      }
      
      await browserPage.waitForTimeout(2000);
      await browserPage.close();
      
    } catch (error) {
      console.error(`Fehler bei Seite ${page}: ${error.message}`);
      await browserPage.close();
    }
  }
  
  // Duplikate entfernen (basierend auf ID)
  const uniqueListings = {};
  allListings.forEach(listing => {
    uniqueListings[listing.id] = listing;
  });
  
  return Object.values(uniqueListings);
}

/**
 * Speichert Ergebnisse in S3 und vergleicht sie mit vorherigen
 */
async function saveAndCompareResults(listings) {
  const today = new Date().toISOString().split('T')[0];
  const key = `${RESULTS_PREFIX}${today}.json`;
  
  // Aktuelle Ergebnisse
  const currentResults = {
    timestamp: new Date().toISOString(),
    listings: listings
  };
  
  // Vorherige Ergebnisse laden
  let previousListings = [];
  try {
    const response = await s3.getObject({
      Bucket: S3_BUCKET_NAME,
      Key: key
    }).promise();
    previousListings = JSON.parse(response.Body.toString('utf-8')).listings || [];
  } catch (error) {
    console.log(`Keine vorherigen Ergebnisse gefunden: ${error.message}`);
  }
  
  // Speichern
  try {
    await s3.putObject({
      Bucket: S3_BUCKET_NAME,
      Key: key,
      Body: JSON.stringify(currentResults),
      ContentType: 'application/json'
    }).promise();
  } catch (error) {
    console.error(`Fehler beim Speichern: ${error.message}`);
  }
  
  // Vergleichen
  const currentIds = new Set(listings.map(listing => listing.id));
  const previousIds = new Set(previousListings.map(listing => listing.id));
  
  const newIds = [...currentIds].filter(id => !previousIds.has(id));
  const removedIds = [...previousIds].filter(id => !currentIds.has(id));
  
  const newListings = listings.filter(listing => newIds.includes(listing.id));
  const removedListings = previousListings.filter(listing => removedIds.includes(listing.id));
  
  return {
    currentListings: listings,
    newListings,
    removedListings
  };
}

/**
 * Telegram-Benachrichtigung senden
 */
async function sendTelegramNotification(changes, force = false, runId = '') {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    return false;
  }
  
  if (!changes.newListings?.length && !changes.removedListings?.length && !changes.error && !force) {
    return true;
  }
  
  try {
    const dateStr = new Date().toISOString().replace('T', ' ').substring(0, 19);
    let message = `*Bazaraki Immobilien-Update (${dateStr})*\n\n`;
    message += `Run ID: \`${runId || 'N/A'}\`\n\n`;
    
    // Zusammenfassung
    const total = changes.currentListings?.length || 0;
    const newCount = changes.newListings?.length || 0;
    const removedCount = changes.removedListings?.length || 0;
    
    message += `*Aktuelle Anzeigen:* ${total}\n`;
    message += `*Neue Anzeigen:* ${newCount}\n`;
    message += `*Entfernte Anzeigen:* ${removedCount}\n\n`;
    
    // Fehlermeldung
    if (changes.error) {
      message += `⚠️ *FEHLER:* ${changes.error}\n\n`;
    }
    
    // Neue Anzeigen
    if (newCount > 0) {
      message += "*Neue Anzeigen:*\n";
      changes.newListings.slice(0, 10).forEach((listing, i) => {
        const title = (listing.title || 'Keine Beschreibung').substring(0, 50);
        const url = listing.url || '#';
        const price = listing.price ? `${listing.price} ${listing.currency || '€'}` : "Preis auf Anfrage";
        message += `${i + 1}. [${title}](${url}) - ${price}\n`;
      });
      
      if (newCount > 10) {
        message += `_...und ${newCount - 10} weitere_\n`;
      }
      message += "\n";
    }
    
    // Kürzen wenn zu lang
    if (message.length > 4000) {
      message = message.substring(0, 3900) + "\n\n_Nachricht gekürzt_";
    }
    
    // Senden
    const telegramUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const response = await axios.post(telegramUrl, {
      chat_id: TELEGRAM_CHAT_ID,
      text: message,
      parse_mode: 'Markdown',
      disable_web_page_preview: true
    });
    
    return response.status === 200;
  } catch (error) {
    console.error(`Telegram-Fehler: ${error.message}`);
    return false;
  }
}

// Hauptausführung
if (require.main === module) {
  scrapeBazaraki();
}

// Cron-Job
const job = new CronJob(
  '0 8,20 * * *',  // 8:00 und 20:00 UTC
  scrapeBazaraki,
  null,
  false,
  'UTC'
);

// Auto-Start in Produktion
if (process.env.NODE_ENV === 'production') {
  job.start();
  console.log('Cron-Job gestartet (8:00 und 20:00 UTC)');
}

module.exports = { scrapeBazaraki };
